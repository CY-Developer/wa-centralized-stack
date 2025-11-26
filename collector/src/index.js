require('../../common/src/env'); // 仍然读取 .env
const express = require('express');
const bodyParser = require('body-parser');
const morgan = require('morgan');
const IORedis = require('ioredis');
const fs = require('fs');
const path = require('path');
const axios = require('axios');

const {CFG} = require('./config');
const cw = require('./chatwoot'); // <-- 修正后有 getInboxIdByIdentifier 等

// ====== 基础配置 ======
const MEDIA_DIR = path.resolve(CFG.chatwoot.MEDIA_DIR || 'data/media');

const CHATWOOT_ACCOUNT_ID = CFG.chatwoot.accountId || '';
const CHATWOOT_INBOX_ID = CFG.chatwoot.inboxId || null;
const CHATWOOT_INBOX_IDENTIFIER = CFG.chatwoot.inboxIdentifier || null;

const WA_BRIDGE_URL = CFG.chatwoot.WA_BRIDGE_URL || '';
const WA_BRIDGE_TOKEN = process.env.WA_BRIDGE_TOKEN || '';
const WA_DEFAULT_SESSION = process.env.SESSIONS || '';

const PORT = Number(process.env.COLLECTOR_PORT || 7001);
const INGEST_TOKEN = process.env.COLLECTOR_INGEST_TOKEN || process.env.API_TOKEN || '';

const REDIS_URL = process.env.REDIS_URL || '';
const redis = REDIS_URL ? new IORedis(REDIS_URL, {maxRetriesPerRequest: null}) : null;

function ensureDir(p) {
    if (!fs.existsSync(p)) fs.mkdirSync(p, {recursive: true});
}

ensureDir(MEDIA_DIR);
const COLLECTOR_LOG = path.resolve(process.env.COLLECTOR_LOG || path.join(__dirname, 'collector.log'));
const collectorStream = fs.createWriteStream(COLLECTOR_LOG, {flags: 'a'});


function logToCollector(tag, data) {
    try {
        const line = `[${new Date().toISOString()}] ${tag} ${data ? JSON.stringify(data) : ''}\n`;
        collectorStream.write(line);
    } catch (e) {
        console.warn('logToCollector failed:', e.message);
    }
    // 同步打一份到控制台，方便本地开发
    console.log(tag, data || '');
}

global.logToCollector = logToCollector; // 需要的话其他文件也能用

// 进程启动时就落一条，告诉你日志实际写到哪
logToCollector('[BOOT] collector up', {log: COLLECTOR_LOG, pid: process.pid});
console.log('[BOOT] collector up, log file =', COLLECTOR_LOG);
// == 小工具 ==
const CW_INTERNAL_BASE = process.env.CW_INTERNAL_BASE || 'http://chatwoot-web:3000';

function rewriteCwDataUrl(u) {
    if (!u) return u;
    try {
        const src = new URL(u);
        // 只重写来自 Chatwoot 的 localhost:3000
        if (src.hostname === 'localhost' && (src.port === '' || src.port === '3000')) {
            const base = new URL(CW_INTERNAL_BASE);
            src.protocol = base.protocol;
            src.hostname = base.hostname;
            src.port = base.port;
            return src.toString();
        }
        return u;
    } catch {
        return u;
    }
}
// 从 Redis 中解析发送目标（优先联系人的键，再回退会话键，最后退到手机号）
// === REPLACE: resolveWaTarget (统一从 Redis 解析目标) ===
async function resolveWaTarget({redis, conversation_id, sender, fallback, WA_DEFAULT_SESSION}) {
    const getJSON = async (k) => {
        try {
            const v = await redis.get(k);
            return v ? JSON.parse(v) : null;
        } catch (_) {
            return null;
        }
    };

    // 1) 优先用会话映射
    let m = null;
    if (conversation_id) m = await getJSON(`cw:mapping:conv:${conversation_id}`);
    // 2) 再退到联系人映射
    if (!m && sender?.id) m = await getJSON(`cw:mapping:contact:${sender.id}`);

    // 3) 再退到 Chatwoot 的 identifier: wa:<sessionId>:<digits>
    const idParts = String(sender?.identifier || '').split(':');
    const idSess = idParts[1] || null;
    const idDigits = idParts[2] || null;

    // 4) 选 sessionId
    const sessionId = m?.sessionId
        || sender?.additional_attributes?.session_id
        || idSess
        || WA_DEFAULT_SESSION;

    // 5) 选 to（优先 chatId / phone_e164 / phone；最后用 identifier 里的 digits / fallback）
    let toRaw = m?.chatId
        || m?.phone_e164
        || m?.phone
        || m?.phone_raw
        || idDigits
        || fallback
        || '';

    // 6) 归一：群聊保留 @g.us；其余取纯数字交给 manager 去 getNumberId
    let to = /@g\.us$/i.test(toRaw) ? toRaw : String(toRaw).replace(/[^\d]/g, '');

    // 7) 记录一下我们最终采用了什么（方便你在 collector.log 里看）
    logToCollector('[CW_TARGET]', {
        from: m ? (m.chatId ? 'redis.chatId' : (m.phone_e164 || m.phone ? 'redis.phone' : 'redis.other')) :
            (idDigits ? 'identifier' : (fallback ? 'fallback' : 'none')),
        contact_id: sender?.id || null,
        conversation_id,
        sessionId,
        to
    });

    return {sessionId, to};
}


function toE164(phone) {
    if (!phone) return '';
    let s = String(phone).trim().replace(/[^\d+]/g, '');
    if (!s) return '';
    if (!s.startsWith('+')) s = '+' + s;
    return s;
}

const http = require('http');
const https = require('https');
const keepAliveHttp = new http.Agent({keepAlive: true, maxSockets: 100});
const keepAliveHttps = new https.Agent({keepAlive: true, maxSockets: 100});

function normalizeWaTo(input) {
    if (!input) return null;
    let t = String(input).trim();

    // 群聊：直接放行
    if (/^[^@]+@g\.us$/i.test(t)) return t;

    // 个人号：已是标准 chatId
    if (/^\d+@c\.us$/i.test(t)) return t;

    // 形如 "digits@lid" —— 一律退回到 c.us
    if (/^\d+@lid$/i.test(t)) {
        return t.replace(/@lid$/i, '@c.us');
    }

    // 其它任何含有 @lid 的花样（例如某些实现会给出 lid_*）——尽力取出数字回退到 c.us
    if (/@lid/i.test(t)) {
        const digits = t.replace(/\D/g, '');
        return digits ? `${digits}@c.us` : null;
    }

    // 只给了手机号：去非数字补 c.us
    const digits = t.replace(/\D/g, '');
    return digits ? `${digits}@c.us` : null;
}

async function postWithRetry(url, data, headers = {}, tries = 2) {
    const cfg = {
        method: 'post',
        url,
        data,
        headers: {'Content-Type': 'application/json', ...headers},
        timeout: 8000,
        httpAgent: keepAliveHttp,
        httpsAgent: keepAliveHttps,
        maxBodyLength: 30 * 1024 * 1024,
    };

    let backoff = 300; // 初始退避
    for (let i = 1; i <= tries; i++) {
        try {
            const r = await axios(cfg);
            return r.data;
        } catch (err) {
            logToCollector('[BRIDGE_POST_ERROR]', {
                url,
                try: i,
                status: err.response?.status,
                code: err.code,
                errno: err.errno,
                address: err.address,
                port: err.port
            });

            // 最后一轮直接抛出
            if (i === tries) throw err;

            const transient = ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE'].includes(err.code || '') || !err.response?.status;
            await new Promise(r => setTimeout(r, transient ? 400 : backoff));
            backoff = Math.min(4000, backoff * 2);
        }
    }
}


function arrayify(x) {
    if (!x) return [];
    return Array.isArray(x) ? x : [x];
}

// 统一把 webhook 的 attachment/media 转成 [{file_url|data_url, file_type, filename}]
function normalizeAttachments({attachment, media, messageId}) {
    const out = [];

    // A：attachment 可以是对象或数组；支持 data_url / file_url / b64 / mime / filename / caption
    for (const a of arrayify(attachment)) {
        if (!a) continue;
        const o = {};
        if (a.data_url) o.data_url = a.data_url;
        else if (a.file_url || a.url) o.file_url = a.file_url || a.url;
        else if (a.b64 && a.mime) {
            const m = String(a.mime || '').split(';')[0]; // 去掉 ;codecs=opus 等参数
            o.data_url = `data:${m};base64,${a.b64}`;
        }
        if (a.mime) o.file_type = a.mime;
        if (a.filename) o.filename = a.filename;
        // 允许透传 caption（供上层决定是否并入 content）
        if (a.caption) o._caption = a.caption;

        out.push(o);
    }

    // B：media 可以是对象或数组；支持 url/b64/mime/name/caption
    for (const m of arrayify(media)) {
        if (!m) continue;
        const o = {};
        if (m.url) o.file_url = m.url;
        else if (m.b64 && m.mime) {
            const mm = String(m.mime || '').split(';')[0];
            o.data_url = `data:${mm};base64,${m.b64}`;
        }
        if (m.mime) o.file_type = m.mime;
        if (m.name) o.filename = m.name;
        if (m.caption) o._caption = m.caption;
        out.push(o);
    }

    // 去掉空项（既无 data_url 也无 file_url）
    return out.filter(x => x.data_url || x.file_url);
}

let CACHED_INBOX_ID = CHATWOOT_INBOX_ID || null;

async function resolveInboxId() {
    if (CACHED_INBOX_ID) return CACHED_INBOX_ID;

    if (CHATWOOT_INBOX_ID && /^\d+$/.test(String(CHATWOOT_INBOX_ID))) {
        // 校验一下这个 id 是否真的在该 account 下
        const list = await cw.listInboxes(CHATWOOT_ACCOUNT_ID);
        const arr = (list && list.payload) || list || [];
        const hit = arr.find(x => Number(x.id) === Number(CHATWOOT_INBOX_ID));
        if (!hit) {
            const candidates = arr.map(x => `${x.id}:${x.name || '-'}`).join(', ');
            throw new Error(`CHATWOOT_INBOX_ID=${CHATWOOT_INBOX_ID} 不属于 account=${CHATWOOT_ACCOUNT_ID}。可用 inbox: ${candidates}`);
        }
        CACHED_INBOX_ID = Number(CHATWOOT_INBOX_ID);
        return CACHED_INBOX_ID;
    }

    if (CHATWOOT_INBOX_IDENTIFIER) {
        CACHED_INBOX_ID = await cw.getInboxIdByIdentifier(CHATWOOT_ACCOUNT_ID, CHATWOOT_INBOX_IDENTIFIER);
        return CACHED_INBOX_ID;
    }

    throw new Error('未配置 CHATWOOT_INBOX_ID（数字）或 CHATWOOT_INBOX_IDENTIFIER（名称）。');
}

// ===== App 初始化 =====
const app = express();
app.use('/media', express.static(MEDIA_DIR));
app.use(bodyParser.json({limit: '25mb'}));
app.use(morgan('dev'));
app.use(morgan('short', {stream: collectorStream}));
app.all('/chatwoot/webhook', (req, _res, next) => {
    logToCollector('[CW_WEBHOOK] TAP', {
        ua: req.headers['user-agent'],
        ct: req.headers['content-type'],
        q: req.query
    });
    next();
});
// 统一鉴权：头 x-api-token 或 query ?token=
app.use((req, res, next) => {

    // 鉴权（支持 header: x-api-token 或 query: ?token=）
    const t = (
        req.headers['x-api-token'] ||
        req.headers['x-chatwoot-webhook-token'] ||
        req.query.token ||
        ''
    ).toString();
    if (INGEST_TOKEN && t !== INGEST_TOKEN) {
        return res.status(401).json({ok: false, error: 'bad token'});
    }

    next();
});
// 健康检查
app.get('/health', (_req, res) => res.json({ok: true}));

/**
 * 入站采集 → Chatwoot
 * 支持两种附件格式：
 *  A) { type:'image', attachment: { data_url|file_url|b64, mime?, filename? } }
 *  B) { media: { type: 'image'|'video'|'audio'|'document', url?|b64?, mime?, name?, caption? } }
 */
app.get('/debug/accounts', async (_req, res) => {
    try {
        const data = await cw.listAccounts();
        const payload = data?.payload || data || [];
        res.json({
            ok: true,
            env: {CHATWOOT_ACCOUNT_ID, CHATWOOT_INBOX_ID, CHATWOOT_BASE_URL: CFG.chatwoot.baseURL},
            accounts: payload.map(a => ({id: a.id, name: a.name, role: a.role})),
        });
    } catch (e) {
        res.status(500).json({ok: false, error: e.message, cw: e._cw || e.response?.data});
    }
});

app.get('/debug/inboxes', async (_req, res) => {
    try {
        const data = await cw.listInboxes(CHATWOOT_ACCOUNT_ID);
        const payload = data?.payload || data || [];
        res.json({
            ok: true,
            account_id: CHATWOOT_ACCOUNT_ID,
            inboxes: payload.map(x => ({id: x.id, name: x.name, channel_type: x.channel_type})),
        });
    } catch (e) {
        res.status(500).json({ok: false, error: e.message, cw: e._cw || e.response?.data});
    }
});

app.post('/ingest', async (req, res) => {
    try {
        const {
            sessionId,
            phone,
            name,
            text,
            type,
            messageId,
            timestamp,
            attachment,
            media
        } = req.body || {};
        const isGroup = /@g\.us$/i.test(String(messageId || '')) || /@g\.us$/i.test(String(req.body?.from || '')) || String(req.body?.server || '').toLowerCase() === 'g.us';
        const isStatus = String(phone || '').toLowerCase() === 'status' || /@broadcast/i.test(String(messageId || '')) || /@broadcast/i.test(String(req.body?.from || ''));
        if (isGroup || isStatus) {
            return res.json({ok: true, skipped: 'ignored group/status message'});
        }
        console.log('Received webhook:', {
            sessionId, phone, name, text, type, messageId,
            hasAttachment: !!attachment, hasMedia: !!media
        });

        const inbox_id = await resolveInboxId();
        const phoneE164 = toE164(phone);

        // 1) 联系人（稳定复用：identifier=wa:<原始phone>，并尽量提取 E.164）
        const contact = await cw.ensureContact({
            account_id: CHATWOOT_ACCOUNT_ID,
            rawPhone: phone,
            rawName: name,
            sessionId
        });

// 2) 会话（按联系人 + inbox 复用；不再传 source_id）
        const conv = await cw.ensureConversation({
            account_id: CHATWOOT_ACCOUNT_ID,
            inbox_id,
            contact_id: contact.id
        });
        const conversation_id = conv.id || conv;

// 3) 组装文本与附件（支持多附件、语音/视频等）
        let content = (text || '').toString();

// 统一归一
        const attachments = normalizeAttachments({attachment, media, messageId});

// 若外部把文字写在附件 caption 里，也并到 content（仅当原 content 为空）
        if (!content && attachments.length > 0) {
            const cap = attachments.find(a => a._caption)?.['_caption'];
            if (cap) content = cap;
        }
// 清理内部字段
        attachments.forEach(a => {
            delete a._caption;
        });

// 4) 发 incoming 消息到 Chatwoot
        const created = await cw.createIncomingMessage({
            account_id: CHATWOOT_ACCOUNT_ID,
            conversation_id,
            // 关键：传 content（或 text，chatwoot.js 里已兼容）
            content,
            attachments,
            text,         // 兼容保留
        });

        // 5) 映射保存（可选） + 同步到 Chatwoot 联系人备注
        if (redis) {
            const now = new Date().toISOString();

            // —— 计算 chatId（个人：<E164去掉+>@c.us；群/状态已在前面被过滤，这里都是个人）
            const calcChatId = (() => {
                const e164Digits = String(phoneE164 || '').replace(/^\+/, '');
                return e164Digits ? `${e164Digits}@c.us` : (normalizeWaTo(String(req.body?.chatId || '')) || null);
            })();

            const payload = {
                sessionId,
                phone: String(phone || ''),            // webhook 原始 phone（不带 +）
                phone_e164: String(phoneE164 || ''),   // 统一为 +E164
                name: String(name || ''),
                chatId: calcChatId,                    // 统一后的 chatId：<digits>@c.us
                updated_at: now
            };

            // 1) 以“联系人ID”为键（核心）
            await redis.set(
                `cw:mapping:contact:${contact.id}`,
                JSON.stringify(payload)
            );

            // 2) 兼容：以“会话ID”为键（保留）
            await redis.set(
                `cw:mapping:conv:${conversation_id}`,
                JSON.stringify(payload)
            );

            // 3) 同步到 Chatwoot 右侧【联系人备注】
            const noteContent =
                `sessionId: ${payload.sessionId}\n` +
                `raw phone: ${payload.phone}\n` +
                `phone_e164: ${payload.phone_e164}\n` +
                `chatId: ${payload.chatId}\n` +
                `updated_at: ${payload.updated_at}`;

            const noteKey = `cw:note:contact:${contact.id}`;
            let noteId = await redis.get(noteKey);

            try {
                if (noteId) {
                    // 更新已有备注
                    await cw.updateContactNote({
                        account_id: CHATWOOT_ACCOUNT_ID,
                        contact_id: contact.id,
                        note_id: noteId,
                        content: noteContent
                    });
                    logToCollector && logToCollector('[CW_NOTE] updated', { contact_id: contact.id, note_id: noteId });
                } else {
                    // 创建新备注
                    const note = await cw.createContactNote({
                        account_id: CHATWOOT_ACCOUNT_ID,
                        contact_id: contact.id,
                        content: noteContent
                    });
                    // 兼容不同返回结构，尽力拿到 id
                    noteId = String(note?.id || note?.note?.id || note?.payload?.id || '');
                    if (noteId) await redis.set(noteKey, noteId);
                    logToCollector && logToCollector('[CW_NOTE] created', { contact_id: contact.id, note_id: noteId || null });
                }
            } catch (e) {
                // 备注失败不影响主流程
                logToCollector && logToCollector('[CW_NOTE] error', {
                    contact_id: contact.id,
                    err: e?.response?.data || e?.message || String(e)
                });
            }
        }


        res.json({ok: true, conversation_id, message_id: created.id || created.message?.id || null});
    } catch (e) {
        console.error('[INGEST_ERROR]', e?.response?.data || e?.message || e);
        res.status(500).json({ok: false, error: e?.message || String(e), cw: e?.response?.data});
    }
});

// === 直连 Chatwoot 的测试接口（验证图片可显示）===
// app.post('/test/chatwoot', async (req, res) => {
//   try {
//     const body = req.body || {};
//     // 兼容三种传法：
//     // 1) { text, data_url }
//     // 2) { text, file_url }
//     // 3) { text, attachments: [ {data_url|file_url|b64,mime,filename,caption}, ... ] }
//     const text = (body.text || '').toString();
//
//     const inbox_id = await resolveInboxId();
//     const contact = await cw.ensureContact({
//       account_id: CHATWOOT_ACCOUNT_ID,
//       rawPhone: '+9990000000',
//       rawName: 'img-test',
//       sessionId
//     });
//     const conv = await cw.ensureConversation({
//       account_id: CHATWOOT_ACCOUNT_ID,
//       inbox_id,
//       contact_id: contact.id
//     });
//
//     let attachments = [];
//     if (Array.isArray(body.attachments)) {
//       attachments = normalizeAttachments({ attachment: body.attachments });
//     } else {
//       attachments = normalizeAttachments({
//         attachment: [{ data_url: body.data_url, file_url: body.file_url, mime: body.mime, filename: body.filename, caption: body.caption }]
//       });
//     }
//
//     const r = await cw.createIncomingMessage({
//       account_id: CHATWOOT_ACCOUNT_ID,
//       conversation_id: conv.id || conv,
//       content: text,
//       attachments
//     });
//
//     res.json({ ok: true, conversation_id: conv.id || conv, message_id: r.id || r.message?.id || null });
//   } catch (e) {
//     console.error('[TEST_CHATWOOT_ERROR]', e?._cw || e?.response?.data || e?.message || e);
//     res.status(500).json({ ok: false, error: e?.message || String(e), cw: e?._cw || e?.response?.data });
//   }
// });
//


app.post('/chatwoot/webhook', async (req, res) => {
    // 统一入站打点：不再丢失任何一次调用
    logToCollector('[CW_WEBHOOK] ARRIVE', {
        ip: req.ip,
        method: req.method,
        ua: req.headers['user-agent'] || '',
        ct: req.headers['content-type'] || '',
        hasToken: Boolean(req.query?.token),
    });

    // 小工具：带日志的早退
    const SKIP = (reason, extra = {}) => {
        logToCollector('[CW_WEBHOOK] SKIP', {reason, ...extra});
        return res.json({ok: true, skipped: reason});
    };

    try {
        const body = req.body || {};

        // Chatwoot 会有两种结构：
        // A) { event: 'message_created', data: { message: {...}, conversation: {...} } }
        // B) { event: 'message_created', id, message_type, content, private, attachments, conversation: {...}, ... }  // 扁平
        const event =
            body.event ||
            body.type ||
            body?.data?.event ||
            body?.payload_type; // 多做一层兼容

        // 先把 payload 摘出来，后面好统一用
        const conversation = body.data?.conversation || body.conversation || null;

        // 组装 message 兼容体
        const message =
            body.data?.message ||
            (event === 'message_created' && (body.id || body.message_type) ? {
                id: body.id,
                message_type: (body.message_type === 'outgoing' ? 2 :
                    body.message_type === 'incoming' ? 1 :
                        body.message_type),
                content: body.content,
                private: body.private,
                attachments: body.attachments || [],
                conversation_id: (conversation && conversation.id) || body.conversation_id
            } : null);

        // —— 把最关键的“事件+是否有message/conv”日志提前 —— //
        logToCollector('[CW_WEBHOOK] HEAD', {
            event,
            hasMessage: Boolean(message),
            hasConversation: Boolean(conversation),
        });

        // 非 message_created 的一律早退（但我们已记录 HEAD 了）
        if (!['message_created', 'message_updated'].includes(event)) {
            return SKIP('unhandled event', {event});
        }

        if (!message || !conversation) return SKIP('no message/conversation', {
            event, keys: Object.keys(body || {}),
        });

        // message_type: 1=incoming, 2=outgoing（也可能是字符串）
        const mt = message.message_type;
        const isOutgoing = (mt === 2) || (String(mt).toLowerCase() === 'outgoing');

        if (!isOutgoing) return SKIP('not outgoing', {mt});
        if (message.private) return SKIP('is private');

        const account_id = conversation.account_id || CHATWOOT_ACCOUNT_ID;
        const conversation_id = conversation.id || message.conversation_id;
// —— 去重：同一 message 在 created/updated/重试场景只发一次 —— //
        const msgId = message.id || message.source_id || message.message_id;
        const stamp = message.updated_at || message.created_at || Date.now();
        if (msgId) {
            const deKey = `cw:dedupe:${msgId}:${stamp}`;
            const seen = await redis.get(deKey);
            if (seen) return SKIP('dup event', {msgId, stamp});
            await redis.set(deKey, '1');
            await redis.expire(deKey, 300); // 5 分钟窗口
        }

        if (!WA_DEFAULT_SESSION || !WA_BRIDGE_URL) {
            console.warn('[WEBHOOK] skip: WA_DEFAULT_SESSION/WA_BRIDGE_URL 未配置');
            return SKIP('no WA config');
        }


        // 记录入参（去敏）
        logToCollector('[CW_WEBHOOK] PAYLOAD', {
            event,
            hasMessage: Boolean(message),
            hasConversation: Boolean(conversation)
        });
        // 查客户号码
        const conv = await cw.getConversationDetails(account_id, conversation_id);
        const meta = (conv && conv.meta) || {};
        const sender = meta.sender || {};

        let phone =
            sender.phone_number ||
            (sender.additional_attributes && (sender.additional_attributes.phone_e164 || sender.additional_attributes.phone_raw || sender.additional_attributes.phone)) ||
            sender.identifier;

        if (!phone) return res.json({ok: true, skipped: 'no phone'});
        const chatIdOrPhone = String(phone).replace(/^wa:[^:]*:/, '').replace(/^wa:/, '');

        const headers = {};
        if (WA_BRIDGE_TOKEN) headers['x-api-token'] = WA_BRIDGE_TOKEN;

        const text = message.content || '';
        // 尽量多兜几个位置，防止不同版本 payload 有差异
        const attachmentsRaw =
            message.attachments ||
            body.attachments ||
            (body.data && body.data.attachments) ||
            [];
        const attachments = Array.isArray(attachmentsRaw) ? attachmentsRaw : [];

        // === 发送媒体 ===
        if (attachments.length > 0) {
            const a = attachments[0] || {};

            // 1) 识别媒体大类：image/video/audio/file
            let mediaType = 'file';
            const ft = String(a.file_type || '').toLowerCase();
            if (ft.startsWith('image')) mediaType = 'image';
            else if (ft.startsWith('video')) mediaType = 'video';
            else if (ft.startsWith('audio')) mediaType = 'audio';

            // 2) 从 data_url / file_url 里拿出 b64 或 URL
            let b64;
            let url;
            let mimetype = 'application/octet-stream';

            const dataUrl = a.data_url || a.dataUrl;
            if (dataUrl) {
                if (/^data:/i.test(dataUrl)) {
                    // data:xxx;base64,... 这种，才按 base64 解析
                    const m = dataUrl.match(/^data:(.*?);base64,(.*)$/i);
                    if (m) {
                        mimetype = m[1] || mimetype;
                        b64 = m[2];
                    }
                } else if (/^https?:\/\//i.test(dataUrl)) {
                    // Chatwoot 默认就是把文件的下载地址放在 data_url 里
                    url = dataUrl;
                }
            }

            // 某些版本可能有 file_url，就当备选
            if (!b64 && !url && a.file_url && /^https?:\/\//i.test(a.file_url)) {
                url = a.file_url;
            }

            if (!b64 && !url) {
                // 连链接都没有，就直接记个日志跳过，避免发一个空的 /send/media
                logToCollector('[CW->WA] SKIP_MEDIA_NO_URL', {
                    msgId: message.id,
                    attachment: { id: a.id, file_type: a.file_type }
                });
            } else {
                const payload = {
                    caption: (message.content || '') || undefined
                };

                if (b64) {
                    payload.b64 = b64;
                    payload.mimetype = mimetype;
                    if (a.filename) payload.filename = a.filename;
                } else if (url) {
                    payload.url = url;
                }

                const { sessionId: finalSession, to: resolvedTo } = await resolveWaTarget({
                    redis,
                    conversation_id: conversation.id || conversation.conversation_id,
                    sender,
                    fallback: chatIdOrPhone,
                    WA_DEFAULT_SESSION
                });

                const toForBridge = /@g\.us$/i.test(resolvedTo)
                    ? resolvedTo
                    : String(resolvedTo).replace(/[^\d]/g, '');

                if (!toForBridge) {
                    return res.json({ ok: true, skipped: 'no target' });
                }

                logToCollector('[CW->WA] SEND_MEDIA', {
                    url: `${WA_BRIDGE_URL}/send/media`,
                    session: finalSession,
                    to: toForBridge,
                    type: mediaType,
                    hasB64: Boolean(payload.b64),
                    hasUrl: Boolean(payload.url),
                    captionLen: (payload.caption || '').length
                });

                await postWithRetry(
                    `${WA_BRIDGE_URL}/send/media`,
                    {
                        sessionId: finalSession,
                        to: toForBridge,
                        type: mediaType,
                        url: payload.url,
                        b64: payload.b64,
                        mimetype: payload.mimetype,
                        filename: payload.filename,
                        caption: payload.caption
                    },
                    headers
                );
            }
        } else if (text && text.trim()) {
            const {sessionId: finalSession, to: resolvedTo} = await resolveWaTarget({
                redis,
                conversation_id: conversation.id || conversation.conversation_id,
                sender,
                fallback: chatIdOrPhone,
                WA_DEFAULT_SESSION
            });
            // 规范 to：群聊保留 @g.us；私聊传纯数字
            const toForBridge = /@g\.us$/i.test(resolvedTo) ? resolvedTo : String(resolvedTo).replace(/[^\d]/g, '');
            if (!toForBridge) return res.json({ok: true, skipped: 'no target'});

            logToCollector('[CW->WA] SEND_TEXT', {
                url: `${WA_BRIDGE_URL}/send/text`,
                session: finalSession,
                to: toForBridge,
                textLen: text.length
            });

            await postWithRetry(
                `${WA_BRIDGE_URL}/send/text`,
                {sessionId: finalSession, to: toForBridge, text},
                headers
            );
        }

        return res.json({ok: true});
    } catch (err) {
        console.error('[WEBHOOK_ERROR]', err.response?.data || err.message);
        return res.status(500).json({ok: false, error: err.message, bridge: err.response?.data});
    }
});

app.listen(PORT, '0.0.0.0', () => {
    console.log(`[collector] up on 0.0.0.0:${PORT}`);
});
