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
// ===== 新增：LID 检测函数 =====
function extractPhoneFromName(name) {
    if (!name) return null;
    const match = String(name).match(/\+?[\d\s\-]{7,}/);
    if (match) {
        const digits = match[0].replace(/[^\d]/g, '');
        if (digits.length >= 7 && digits.length <= 15) return digits;
    }
    return null;
}

function isLidFormat(jidOrPhone) {
    const s = String(jidOrPhone || '');
    if (/@lid$/i.test(s)) return true;
    const digits = s.replace(/\D/g, '');
    if (/^6789\d{10,}/.test(digits)) return true;
    return false;
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
const syncingConversations = new Map();

// 核心策略：同步进行中时，完全阻止所有发送操作，不依赖内容匹配
const syncLockManager = {

    // 获取锁 - 如果已有锁在进行中，返回失败
    acquireLock(conversation_id) {
        const existing = syncingConversations.get(conversation_id);

        if (existing) {
            const elapsed = Date.now() - existing.startTime;

            // 如果锁未释放且未超时，拒绝新的同步
            if (!existing.released && elapsed < 5 * 60 * 1000) {
                logToCollector('[SYNC_LOCK] Rejected - lock active', {
                    conversation_id,
                    lockId: existing.lockId,
                    elapsed: Math.round(elapsed / 1000),
                    completed: existing.completed
                });
                return {
                    success: false,
                    reason: 'sync_in_progress',
                    elapsed: Math.round(elapsed / 1000),
                    completed: existing.completed
                };
            }

            // 超时或已释放，强制清理
            logToCollector('[SYNC_LOCK] Force cleaning stale lock', {
                conversation_id,
                elapsed: Math.round(elapsed / 1000)
            });
        }

        // 生成唯一的锁 ID
        const lockId = `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

        const newLock = {
            lockId,
            startTime: Date.now(),
            completed: false,
            released: false,
            messageCount: 0
        };

        syncingConversations.set(conversation_id, newLock);

        logToCollector('[SYNC_LOCK] Acquired', {
            conversation_id,
            lockId
        });

        return { success: true, lock: newLock };
    },

    // 设置消息数量（用于日志）
    setMessageCount(conversation_id, count) {
        const lock = syncingConversations.get(conversation_id);
        if (lock) {
            lock.messageCount = count;
        }
    },

    // 检查是否应该阻止发送 - 核心方法
    // 只要有活跃的锁（未释放），就阻止发送
    shouldBlockSend(conversation_id) {
        const lock = syncingConversations.get(conversation_id);

        if (!lock) {
            return { block: false, reason: 'no_lock' };
        }

        if (lock.released) {
            return { block: false, reason: 'lock_released' };
        }

        // 有活跃的锁，阻止发送
        const elapsed = Date.now() - lock.startTime;
        return {
            block: true,
            reason: lock.completed ? 'sync_completed_waiting' : 'sync_in_progress',
            elapsed: Math.round(elapsed / 1000),
            lockId: lock.lockId
        };
    },

    // 标记完成并安排释放
    markComplete(conversation_id, delayMs = 20000) {
        const lock = syncingConversations.get(conversation_id);
        if (!lock) return;

        lock.completed = true;
        lock.completedAt = Date.now();

        const lockId = lock.lockId;

        logToCollector('[SYNC_LOCK] Marked complete', {
            conversation_id,
            lockId,
            messageCount: lock.messageCount,
            willReleaseIn: delayMs
        });

        // 延迟释放
        setTimeout(() => {
            const currentLock = syncingConversations.get(conversation_id);
            if (currentLock && currentLock.lockId === lockId && !currentLock.released) {
                currentLock.released = true;
                logToCollector('[SYNC_LOCK] Released', { conversation_id, lockId });

                // 再过10秒后完全删除
                setTimeout(() => {
                    const stillThere = syncingConversations.get(conversation_id);
                    if (stillThere && stillThere.lockId === lockId) {
                        syncingConversations.delete(conversation_id);
                        logToCollector('[SYNC_LOCK] Deleted', { conversation_id, lockId });
                    }
                }, 10000);
            }
        }, delayMs);
    },

    // 强制释放锁
    forceRelease(conversation_id) {
        const lock = syncingConversations.get(conversation_id);
        if (lock) {
            lock.released = true;
            syncingConversations.delete(conversation_id);
            logToCollector('[SYNC_LOCK] Force released', { conversation_id, lockId: lock.lockId });
        }
    },

    // 获取锁状态（调试用）
    getStatus(conversation_id) {
        const lock = syncingConversations.get(conversation_id);
        if (!lock) return null;
        return {
            lockId: lock.lockId,
            startTime: lock.startTime,
            elapsed: Date.now() - lock.startTime,
            completed: lock.completed,
            released: lock.released,
            messageCount: lock.messageCount
        };
    },

    // 获取所有活跃锁（调试用）
    getAllLocks() {
        const result = {};
        for (const [convId, lock] of syncingConversations) {
            result[convId] = this.getStatus(convId);
        }
        return result;
    }
};

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

        // 重写来自 Chatwoot 的 localhost:3000
        if (src.hostname === 'localhost' && (src.port === '' || src.port === '3000')) {
            const base = new URL(CW_INTERNAL_BASE);
            src.protocol = base.protocol;
            src.hostname = base.hostname;
            src.port = base.port;
            return src.toString();
        }

        // ===== 新增：内网 HTTPS -> HTTP 转换 =====
        const internalPatterns = [
            /^192\.168\./, /^10\./, /^172\.(1[6-9]|2[0-9]|3[0-1])\./,
            /^127\./, /localhost/i
        ];
        const isInternal = internalPatterns.some(p => p.test(src.hostname));
        if (isInternal && src.protocol === 'https:') {
            src.protocol = 'http:';
            console.log('[URL_REWRITE] https->http:', u, '->', src.toString());
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

    // 5) 分别获取 phone 和 phone_lid
    let to = '';      // 真实电话
    let to_lid = '';  // 隐私号

    // 从 Redis 映射中获取
    if (m?.phone) {
        to = String(m.phone).replace(/\D/g, '');
    }
    if (m?.phone_lid) {
        to_lid = String(m.phone_lid).replace(/\D/g, '');
    }

    // 如果 Redis 没有，尝试从 identifier 或 fallback 获取
    if (!to && !to_lid) {
        const digits = idDigits || String(fallback || '').replace(/\D/g, '');
        if (digits) {
            // 无法判断是电话还是 LID，假设是电话
            to = digits;
        }
    }

    // 6) 群聊特殊处理
    const chatIdRaw = m?.chatId || '';
    if (/@g\.us$/i.test(chatIdRaw)) {
        to = chatIdRaw;
        to_lid = '';
    }

    // 7) 记录日志
    logToCollector('[CW_TARGET]', {
        contact_id: sender?.id || null,
        conversation_id,
        sessionId,
        to: to || 'none',
        to_lid: to_lid || 'none',
        source: m ? 'redis' : (idDigits ? 'identifier' : 'fallback')
    });

    return { sessionId, to, to_lid };
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
        timeout: 120000,
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

async function markMessageAsFailed(account_id, conversation_id, message_id, errorReason) {
    try {
        logToCollector('[CW_MSG_FAILED]', {
            account_id,
            conversation_id,
            message_id,
            error: errorReason
        });

        if (conversation_id && account_id) {
            try {
                await cw.createOutgoingMessage({
                    account_id,
                    conversation_id,
                    content: `⚠️ 消息发送失败: ${errorReason}`,
                    private: true,
                    content_attributes: {
                        message_type: 'activity',
                        is_error: true
                    }
                });
            } catch (e) {
                console.error('[markMessageAsFailed] Failed to create error note:', e.message);
            }
        }

        return true;
    } catch (e) {
        console.error('[markMessageAsFailed] Error:', e.message);
        return false;
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
const cors = require('cors');

// 使用 cors 包处理跨域
app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'x-api-token', 'X-Api-Token', 'Authorization', 'x-chatwoot-webhook-token'],
    credentials: false
}));
app.use((req, res, next) => {
    console.log(`[CORS] ${req.method} ${req.path} from ${req.headers.origin || 'no-origin'}`);
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type, x-api-token, X-Api-Token, Authorization');
    res.header('Access-Control-Max-Age', '86400');
    if (req.method === 'OPTIONS') {
        console.log('[CORS] Responding to OPTIONS preflight');
        return res.status(200).end();
    }
    next();
});

app.use('/media', express.static(MEDIA_DIR));
app.use(bodyParser.json({limit: '25mb'}));
app.use(morgan('dev'));
// 统一鉴权：头 x-api-token 或 query ?token=
app.use((req, res, next) => {
    if (req.method === 'OPTIONS') {
        return next();
    }
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
            phone_lid,
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
            rawPhone_lid: phone_lid,
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

            // 计算 chatId（优先用真实电话，其次用隐私号）
            const calcChatId = (() => {
                if (phone) {
                    const digits = String(phone).replace(/\D/g, '');
                    if (digits) return `${digits}@c.us`;
                }
                if (phone_lid) {
                    const digits = String(phone_lid).replace(/\D/g, '');
                    if (digits) return `${digits}@lid`;
                }
                return normalizeWaTo(String(req.body?.chatId || '')) || null;
            })();

            const payload = {
                sessionId,
                phone: String(phone || ''),            // 真实电话
                phone_lid: String(phone_lid || ''),    // 隐私号
                phone_e164: String(phoneE164 || ''),
                name: String(name || ''),
                chatId: calcChatId,
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
                `phone: ${payload.phone}\n` +
                `phone_lid: ${payload.phone_lid}\n` +
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
app.post('/message-status', async (req, res) => {
    try {
        const { sessionId, messageId, ack, status, timestamp } = req.body || {};

        if (ack === 0 || status === 'failed') {
            console.log(`[MSG_STATUS] FAILED: ${messageId?.substring(0, 30)}...`);
            logToCollector('[MSG_STATUS] FAILED', { sessionId, messageId, ack, status });
        }

        res.json({ ok: true });
    } catch (e) {
        res.status(500).json({ ok: false, error: e?.message });
    }
});

/**
 * 接口2：我方发送消息入站（同步 WhatsApp 端发送的消息到 Chatwoot）
 */
app.post('/ingest-outgoing', async (req, res) => {
    try {
        const {
            sessionId, messageId, phone, phone_lid, name,
            text, type, timestamp, to, chatId,
            fromMe, direction, attachment
        } = req.body || {};

        console.log(`[INGEST_OUT] msgId=${messageId?.substring(0, 25)}..., to=${to?.substring(0, 20)}`);

        if (!fromMe || direction !== 'outgoing') {
            return res.json({ ok: true, skipped: 'not outgoing' });
        }

        if (/@g\.us$/i.test(to) || /@broadcast/i.test(to)) {
            return res.json({ ok: true, skipped: 'group/broadcast' });
        }

        // 去重
        if (redis) {
            const key = `wa:outgoing:${messageId}`;
            if (await redis.get(key)) {
                return res.json({ ok: true, skipped: 'duplicate' });
            }
            await redis.set(key, '1');
            await redis.expire(key, 300);
        }

        const targetPhone = phone || phone_lid || to?.replace(/@.*/, '').replace(/\D/g, '');
        if (!targetPhone) {
            return res.json({ ok: true, skipped: 'no phone' });
        }

        const inbox_id = await resolveInboxId();

        const contact = await cw.ensureContact({
            account_id: CHATWOOT_ACCOUNT_ID,
            rawPhone: targetPhone,
            rawName: name || targetPhone,
            sessionId
        });

        const conv = await cw.ensureConversation({
            account_id: CHATWOOT_ACCOUNT_ID,
            inbox_id,
            contact_id: contact.id
        });
        const conversation_id = conv.id || conv;

        // 处理附件
        const attachments = normalizeAttachments({ attachment, messageId });

        // 创建消息
        let created;
        try {
            // 尝试用 Chatwoot API 创建 outgoing 消息
            const msgPayload = {
                content: text || '',
                message_type: 'outgoing',
                private: false
            };

            if (attachments.length > 0) {
                // 有附件：使用 FormData
                const FormData = require('form-data');
                const form = new FormData();
                form.append('content', text || '');
                form.append('message_type', 'outgoing');
                form.append('private', 'false');

                for (const att of attachments) {
                    if (att.data_url) {
                        const m = att.data_url.match(/^data:(.+);base64,(.+)$/);
                        if (m) {
                            const buffer = Buffer.from(m[2], 'base64');
                            form.append('attachments[]', buffer, {
                                filename: att.filename || 'file',
                                contentType: m[1]
                            });
                        }
                    }
                }

                created = await cw.request(
                    'POST',
                    `/api/v1/accounts/${CHATWOOT_ACCOUNT_ID}/conversations/${conversation_id}/messages`,
                    form,
                    form.getHeaders()
                );
            } else {
                // 无附件：JSON 请求
                created = await cw.request(
                    'POST',
                    `/api/v1/accounts/${CHATWOOT_ACCOUNT_ID}/conversations/${conversation_id}/messages`,
                    msgPayload
                );
            }
        } catch (e) {
            console.log(`[INGEST_OUT] Create message error:`, e?.message);
            // 降级：尝试简单的消息创建
            created = { id: null };
        }

        console.log(`[INGEST_OUT] Created: ${created?.id || 'unknown'}`);
        res.json({ ok: true, conversation_id, message_id: created?.id });
    } catch (e) {
        console.error('[INGEST_OUT_ERROR]', e?.message);
        res.status(500).json({ ok: false, error: e?.message });
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



/**
 * POST /sync-messages
 * 同步 WhatsApp 消息到 Chatwoot（增强版 - 支持批量创建）
 *
 * Body:
 * {
 *   conversation_id: 149,           // Chatwoot 会话 ID（必填）
 *   hours: 12,                       // 同步最近多少小时（默认 12）
 *   after: "2025-01-01T00:00:00Z",   // 或指定开始时间 ISO 格式
 *   before: "2025-01-01T12:00:00Z",  // 或指定结束时间 ISO 格式
 *   direction: "both",               // "incoming" | "outgoing" | "both"
 *   replace: false,                  // 是否删除 Chatwoot 中该时间段的消息再同步
 *   dryRun: false,                   // 测试模式，不实际执行
 *   messageIds: null,                // 只同步指定的消息 ID（用于重试）
 *   alignMessages: true,             // 是否启用消息对齐（默认 true）
 *   batchSize: 50,                   // 批量创建时每批数量（默认 50）
 *   useBatchCreate: true             // 是否使用批量创建（默认 true）
 * }
 */
app.post('/sync-messages', async (req, res) => {
    let conversation_id;
    try {
        const {
            conversation_id: convId,
            hours = 12,
            after,
            before,
            direction = 'both',
            replace = false,
            dryRun = false,
            messageIds = null,
            alignMessages = true,
            batchSize = 50,          // 新增：批量大小
            useBatchCreate = true    // 新增：是否使用批量创建
        } = req.body || {};

        conversation_id = convId;

        if (!conversation_id) {
            return res.status(400).json({ ok: false, error: 'conversation_id required' });
        }

        logToCollector('[SYNC] Start', {
            conversation_id, hours, after, before, direction, dryRun,
            messageIds: messageIds?.length || 0,
            alignMessages,
            batchSize,
            useBatchCreate
        });

        // 1. 获取会话详情
        const conv = await cw.getConversationDetails(CHATWOOT_ACCOUNT_ID, conversation_id);
        if (!conv) {
            return res.status(404).json({ ok: false, error: 'Conversation not found' });
        }

        const sender = conv.meta?.sender || {};
        const inboxId = conv.inbox_id;

        logToCollector('[SYNC] Conversation', {
            contact_id: sender.id,
            contact_name: sender.name,
            inbox_id: inboxId
        });

        // 2. 从 Redis 获取 WhatsApp 映射
        let waMapping = null;
        if (redis) {
            const keys = [
                `cw:mapping:conv:${conversation_id}`,
                `cw:mapping:contact:${sender.id}`,
                `wa:conv:${conversation_id}`
            ];

            for (const key of keys) {
                const data = await redis.get(key);
                if (data) {
                    try {
                        waMapping = JSON.parse(data);
                        logToCollector('[SYNC] Found mapping', { key, data: waMapping });
                        break;
                    } catch (_) {}
                }
            }
        }

        // 如果没有映射，尝试从联系人信息推断
        if (!waMapping) {
            const phone = sender.phone_number?.replace(/[^\d]/g, '') ||
                sender.identifier?.replace(/[^\d]/g, '');

            if (phone) {
                waMapping = {
                    sessionId: WA_DEFAULT_SESSION.split(',')[0]?.trim(),
                    phone: phone,
                    chatId: `${phone}@c.us`
                };
                logToCollector('[SYNC] Inferred mapping from phone', waMapping);
            }
        }

        if (!waMapping?.sessionId) {
            return res.status(400).json({
                ok: false,
                error: 'No WhatsApp mapping found',
                hint: 'Contact needs to have exchanged messages first'
            });
        }

        const { sessionId, chatId, phone, phone_lid } = waMapping;
        const waChatId = chatId || (phone ? `${phone}@c.us` : (phone_lid ? `${phone_lid}@lid` : null));

        if (!waChatId) {
            return res.status(400).json({ ok: false, error: 'Cannot determine WhatsApp chat ID' });
        }

        // 3. 计算时间范围
        let afterTs, beforeTs;
        const nowTs = Math.floor(Date.now() / 1000);

        if (after) {
            afterTs = Math.floor(new Date(after).getTime() / 1000);
        } else {
            afterTs = nowTs - hours * 3600;
        }

        if (before) {
            beforeTs = Math.floor(new Date(before).getTime() / 1000);
        } else {
            beforeTs = nowTs;
        }

        const afterISO = new Date(afterTs * 1000).toISOString();
        const beforeISO = new Date(beforeTs * 1000).toISOString();

        logToCollector('[SYNC] Time range', { after: afterISO, before: beforeISO });

        // 4. 从 WhatsApp 获取消息
        const waUrl = `${WA_BRIDGE_URL}/messages-sync/${sessionId}/${encodeURIComponent(waChatId)}`;
        const waParams = new URLSearchParams({
            limit: '99999',
            after: String(afterTs),
            before: String(beforeTs),
            includeMedia: '1',
            includeBase64: '1'
        });

        const waHeaders = {};
        if (WA_BRIDGE_TOKEN) waHeaders['x-api-token'] = WA_BRIDGE_TOKEN;

        let waMessages = [];
        try {
            const waResp = await axios.get(`${waUrl}?${waParams}`, {
                headers: waHeaders,
                timeout: 1800000
            });
            waMessages = waResp.data?.messages || [];
            logToCollector('[SYNC] WA messages', { count: waMessages.length });
        } catch (e) {
            logToCollector('[SYNC] WA fetch error', { error: e?.message });
            return res.status(500).json({ ok: false, error: `Failed to fetch WA messages: ${e?.message}` });
        }

        // 5. 获取 Chatwoot 现有消息（使用批量查询 API）
        let cwMessages = [];
        try {
            cwMessages = await cw.batchQueryMessages({
                account_id: CHATWOOT_ACCOUNT_ID,
                conversation_id,
                after: afterISO,
                before: beforeISO
            });
            logToCollector('[SYNC] CW messages (batch API)', { count: cwMessages.length });
        } catch (e) {
            logToCollector('[SYNC] CW batch query failed, fallback', { error: e?.message });
            try {
                cwMessages = await cw.getConversationMessages({
                    account_id: CHATWOOT_ACCOUNT_ID,
                    conversation_id,
                    after: afterTs * 1000,
                    before: beforeTs * 1000
                });
                logToCollector('[SYNC] CW messages (fallback)', { count: cwMessages.length });
            } catch (e2) {
                logToCollector('[SYNC] CW fetch error', { error: e2?.message });
            }
        }

        // 6. 消息对齐逻辑
        let timeOffset = 0;
        let alignmentInfo = null;

        if (alignMessages && cwMessages.length > 0 && waMessages.length > 0) {
            logToCollector('[SYNC] Starting message alignment');
            const offsetResult = cw.calculateTimeOffset(cwMessages, waMessages);
            timeOffset = offsetResult.offset;
            alignmentInfo = offsetResult;

            logToCollector('[SYNC] Time offset calculated', {
                offset: timeOffset,
                confidence: offsetResult.confidence,
                samples: offsetResult.samples
            });

            // 验证首尾消息
            if (waMessages.length > 0) {
                const firstWa = waMessages[0];
                const lastWa = waMessages[waMessages.length - 1];
                const firstWaAdjustedTs = firstWa.timestamp + timeOffset;
                const lastWaAdjustedTs = lastWa.timestamp + timeOffset;

                const matchedFirst = cw.findMessageByContentAndTime(cwMessages, firstWa.body, firstWaAdjustedTs, 60);
                const matchedLast = cw.findMessageByContentAndTime(cwMessages, lastWa.body, lastWaAdjustedTs, 60);

                alignmentInfo.firstMatch = !!matchedFirst;
                alignmentInfo.lastMatch = !!matchedLast;

                logToCollector('[SYNC] Alignment verification', {
                    firstWaContent: (firstWa.body || '').substring(0, 30),
                    firstMatch: !!matchedFirst,
                    lastWaContent: (lastWa.body || '').substring(0, 30),
                    lastMatch: !!matchedLast
                });
            }
        }

        // 7. 比对并找出需要同步的消息
        const cwSourceIds = new Set(cwMessages.map(m => m.source_id).filter(Boolean));
        const cwContentIndex = new Map();

        for (const m of cwMessages) {
            const ts = m.created_at_unix || Math.floor(new Date(m.created_at).getTime() / 1000);
            const contentKey = `${ts}_${(m.content || '').trim().toLowerCase().substring(0, 50)}`;
            cwContentIndex.set(contentKey, m);
        }

        const messageIdSet = messageIds && Array.isArray(messageIds) && messageIds.length > 0
            ? new Set(messageIds)
            : null;

        const toSync = [];
        const skipped = [];

        for (const waMsg of waMessages) {
            // 如果指定了 messageIds，只处理这些消息
            if (messageIdSet && !messageIdSet.has(waMsg.id)) {
                skipped.push({ id: waMsg.id, reason: 'not_in_retry_list' });
                continue;
            }

            // 检查 source_id 是否已存在
            if (!messageIdSet && cwSourceIds.has(waMsg.id)) {
                skipped.push({ id: waMsg.id, reason: 'source_id_exists' });
                continue;
            }

            // 检查内容+时间是否已存在
            const adjustedTs = waMsg.timestamp + timeOffset;
            const contentKey = `${adjustedTs}_${(waMsg.body || '').trim().toLowerCase().substring(0, 50)}`;

            if (!messageIdSet && cwContentIndex.has(contentKey)) {
                skipped.push({ id: waMsg.id, reason: 'content_time_match' });
                continue;
            }

            // 根据方向过滤
            if (direction === 'incoming' && waMsg.fromMe) {
                skipped.push({ id: waMsg.id, reason: 'direction_filter' });
                continue;
            }
            if (direction === 'outgoing' && !waMsg.fromMe) {
                skipped.push({ id: waMsg.id, reason: 'direction_filter' });
                continue;
            }

            toSync.push(waMsg);
        }

        logToCollector('[SYNC] Analysis', {
            waTotal: waMessages.length,
            cwTotal: cwMessages.length,
            toSync: toSync.length,
            skipped: skipped.length,
            timeOffset,
            alignment: alignmentInfo
        });

        // Dry run 模式
        if (dryRun) {
            return res.json({
                ok: true,
                dryRun: true,
                summary: {
                    waMessages: waMessages.length,
                    cwMessages: cwMessages.length,
                    toSync: toSync.length,
                    skipped: skipped.length,
                    timeOffset,
                    alignment: alignmentInfo
                },
                messagesToSync: toSync.slice(0, 20).map(m => ({
                    id: m.id,
                    fromMe: m.fromMe,
                    type: m.type,
                    body: (m.body || '').substring(0, 100),
                    timestamp: m.timestamp
                })),
                skipped: skipped.slice(0, 20)
            });
        }

        // 8. 如果需要替换，使用批量删除
        if (replace && cwMessages.length > 0) {
            // ★★★ 关键修复：使用消息 ID 列表进行精确删除 ★★★
            // 时间范围删除可能因为 created_at 字段不一致而失败
            const messageIds = cwMessages.map(m => m.id).filter(Boolean);

            logToCollector('[SYNC] Replace mode - batch deleting CW messages', {
                count: cwMessages.length,
                messageIds: messageIds.slice(0, 10)  // 只记录前10个ID
            });

            try {
                // 优先使用消息 ID 列表删除（更精确）
                if (messageIds.length > 0) {
                    const deleteResult = await cw.batchDeleteMessages({
                        account_id: CHATWOOT_ACCOUNT_ID,
                        conversation_id,
                        message_ids: messageIds,  // ★★★ 使用 ID 列表 ★★★
                        hard_delete: true
                    });

                    logToCollector('[SYNC] Batch delete by IDs complete', {
                        requested: messageIds.length,
                        deleted: deleteResult.deleted,
                        failed: deleteResult.failed
                    });
                } else {
                    // 降级：使用时间范围删除
                    const deleteResult = await cw.batchDeleteMessages({
                        account_id: CHATWOOT_ACCOUNT_ID,
                        conversation_id,
                        after: afterISO,
                        before: beforeISO,
                        hard_delete: true
                    });

                    logToCollector('[SYNC] Batch delete by time range complete', {
                        deleted: deleteResult.deleted,
                        failed: deleteResult.failed
                    });
                }
            } catch (e) {
                logToCollector('[SYNC] Batch delete failed', { error: e?.message });
                // 删除失败不阻止同步继续
            }

            // 替换模式下，同步所有 WA 消息
            toSync.length = 0;
            toSync.push(...waMessages.filter(m => {
                if (direction === 'incoming' && m.fromMe) return false;
                if (direction === 'outgoing' && !m.fromMe) return false;
                return true;
            }));
        }

        // 9. 按时间顺序排序
        toSync.sort((a, b) => a.timestamp - b.timestamp);

        // 【Bug 3 修复】使用 syncLockManager 设置同步锁
        const lockResult = syncLockManager.acquireLock(conversation_id);
        if (!lockResult.success) {
            logToCollector('[SYNC] Rejected - sync already in progress', {
                conversation_id,
                reason: lockResult.reason,
                elapsed: lockResult.elapsed
            });
            return res.status(429).json({
                ok: false,
                error: '同步正在进行中，请稍后再试',
                retryAfter: Math.max(30 - (lockResult.elapsed || 0), 10)
            });
        }
        const syncLock = lockResult.lock;
        logToCollector('[SYNC] Lock acquired', { conversation_id, lockId: syncLock.lockId });

        let syncResults = [];
        let successCount = 0;
        let failedCount = 0;
        let skippedCount = 0;

        // ========== 关键改动：使用批量创建 ==========
        if (useBatchCreate && toSync.length > 0) {
            logToCollector('[SYNC] Using batch create', {
                total: toSync.length,
                batchSize
            });

            syncLockManager.setMessageCount(conversation_id, toSync.length);

            // 预处理消息：下载媒体文件
            const preparedMessages = [];

            for (const waMsg of toSync) {
                const prepared = await prepareMessageForBatch(waMsg, WA_BRIDGE_URL, WA_BRIDGE_TOKEN);
                if (prepared) {
                    preparedMessages.push(prepared);
                }
            }

            logToCollector('[SYNC] Messages prepared', {
                total: toSync.length,
                prepared: preparedMessages.length
            });

            // 分批创建
            for (let i = 0; i < preparedMessages.length; i += batchSize) {
                const batch = preparedMessages.slice(i, i + batchSize);
                const batchNum = Math.floor(i / batchSize) + 1;
                const totalBatches = Math.ceil(preparedMessages.length / batchSize);

                logToCollector('[SYNC] Processing batch', {
                    batch: batchNum,
                    total: totalBatches,
                    size: batch.length
                });

                try {
                    // 尝试使用批量创建 API
                    const result = await cw.batchCreateMessages({
                        account_id: CHATWOOT_ACCOUNT_ID,
                        conversation_id,
                        messages: batch
                    });

                    successCount += result.created || 0;
                    failedCount += result.failed || 0;
                    skippedCount += result.skipped || 0;

                    if (result.created_ids) {
                        result.created_ids.forEach(id => {
                            syncResults.push({ id, success: true, batch: batchNum });
                        });
                    }

                    logToCollector('[SYNC] Batch complete', {
                        batch: batchNum,
                        created: result.created,
                        failed: result.failed,
                        skipped: result.skipped
                    });

                } catch (e) {
                    logToCollector('[SYNC] Batch create failed, fallback to single', {
                        batch: batchNum,
                        error: e?.message
                    });

                    // 降级到逐条创建
                    for (const msg of batch) {
                        try {
                            await createSingleMessage(CHATWOOT_ACCOUNT_ID, conversation_id, msg);
                            successCount++;
                            syncResults.push({ source_id: msg.source_id, success: true });
                        } catch (err) {
                            const errMsg = err?.message || '';
                            if (errMsg.includes('duplicate') || errMsg.includes('same_second')) {
                                skippedCount++;
                                syncResults.push({ source_id: msg.source_id, success: true, note: 'duplicate' });
                            } else {
                                failedCount++;
                                syncResults.push({ source_id: msg.source_id, success: false, error: errMsg });
                            }
                        }
                        await new Promise(r => setTimeout(r, 50));
                    }
                }

                // 批次间延迟
                if (i + batchSize < preparedMessages.length) {
                    await new Promise(r => setTimeout(r, 200));
                }
            }

        } else {
            // ========== 原有逻辑：逐条同步 ==========
            logToCollector('[SYNC] Using single create', { total: toSync.length });

            for (const waMsg of toSync) {
                const msgKey = `${waMsg.body || ''}`.substring(0, 100);
                syncLock.msgIds.add(msgKey);

                let lastError = null;
                let success = false;

                for (let attempt = 1; attempt <= 3; attempt++) {
                    try {
                        await syncOneMessage({
                            account_id: CHATWOOT_ACCOUNT_ID,
                            conversation_id,
                            message: waMsg,
                            waBridgeUrl: WA_BRIDGE_URL,
                            waBridgeToken: WA_BRIDGE_TOKEN
                        });
                        success = true;
                        successCount++;
                        syncResults.push({ id: waMsg.id, success: true });
                        break;
                    } catch (e) {
                        lastError = e?.message || '';

                        if (lastError.includes('record_invalid') ||
                            lastError.includes('duplicate') ||
                            lastError.includes('same_second') ||
                            lastError.includes('Duplicate message')) {
                            success = true;
                            skippedCount++;
                            syncResults.push({ id: waMsg.id, success: true, note: 'duplicate_skipped' });
                            break;
                        }

                        if (attempt < 3) {
                            await new Promise(r => setTimeout(r, 500 * attempt));
                        }
                    }
                }

                if (!success) {
                    failedCount++;
                    syncResults.push({ id: waMsg.id, success: false, error: lastError });
                }

                await new Promise(r => setTimeout(r, 80));
            }
        }

        logToCollector('[SYNC] Complete', {
            synced: successCount,
            failed: failedCount,
            skipped: skippedCount
        });

        // 【v3】同步完成后等待 25 秒再释放锁，确保所有 webhook 都被阻止
        syncLockManager.markComplete(conversation_id, 20000);

        res.json({
            ok: true,
            summary: {
                waMessages: waMessages.length,
                cwMessages: cwMessages.length,
                synced: successCount,
                failed: failedCount,
                skipped: skippedCount + skipped.length,
                timeOffset,
                alignment: alignmentInfo,
                usedBatchCreate: useBatchCreate
            },
            results: syncResults.slice(0, 100)  // 限制返回数量
        });

    } catch (e) {
        if (typeof conversation_id !== 'undefined') {
            // 【Bug 3 修复】使用 syncLockManager 强制释放锁
            syncLockManager.forceRelease(conversation_id);
        }
        logToCollector('[SYNC] Error', { error: e?.message, stack: e?.stack });
        res.status(500).json({ ok: false, error: e?.message });
    }
});


/**
 * 预处理消息用于批量创建
 * @param {Object} waMsg - WhatsApp 消息
 * @param {string} waBridgeUrl - 桥接器 URL
 * @param {string} waBridgeToken - 桥接器 Token
 * @returns {Object} 处理后的消息对象
 */
async function prepareMessageForBatch(waMsg, waBridgeUrl, waBridgeToken) {
    const { id, fromMe, type, body, timestamp, media } = waMsg;

    const prepared = {
        content: body || '',
        message_type: fromMe ? 1 : 0,  // 1=outgoing, 0=incoming
        timestamp: timestamp,
        source_id: id,
        attachments: [],
        // ★★★ 关键修复：为 outgoing 消息设置 status: 'sent' ★★★
        // 同步的历史消息已发送成功，不应该显示为 pending
        ...(fromMe ? { status: 'sent' } : {})
    };

    // 处理媒体附件
    if (media && !media.error) {
        if (media.data_url) {
            // 已经是 base64
            prepared.attachments.push({
                data_url: media.data_url,
                file_type: media.mimetype,
                filename: media.filename || `media_${Date.now()}`
            });
        } else if (media.fileUrl && waBridgeUrl) {
            // 需要下载
            try {
                let mediaUrl = media.fileUrl;
                if (!mediaUrl.startsWith('http')) {
                    mediaUrl = `${waBridgeUrl}${mediaUrl}`;
                }

                const headers = {};
                if (waBridgeToken) headers['x-api-token'] = waBridgeToken;

                const resp = await axios.get(mediaUrl, {
                    responseType: 'arraybuffer',
                    headers,
                    timeout: 30000
                });

                const buffer = Buffer.from(resp.data);
                const mime = media.mimetype || resp.headers['content-type'] || 'application/octet-stream';
                const b64 = `data:${mime};base64,${buffer.toString('base64')}`;

                prepared.attachments.push({
                    data_url: b64,
                    file_type: mime,
                    filename: media.filename || `media_${Date.now()}`
                });
            } catch (e) {
                console.warn(`[prepareMessageForBatch] ${id}: Download failed: ${e?.message}`);
            }
        }
    }

    return prepared;
}

/**
 * 创建单条消息（批量创建降级使用）
 */
async function createSingleMessage(account_id, conversation_id, msg) {
    if (msg.message_type === 1) {
        // outgoing
        return await cw.createOutgoingMessage({
            account_id,
            conversation_id,
            content: msg.content || '',
            attachments: msg.attachments || [],
            source_id: msg.source_id
        });
    } else {
        // incoming
        return await cw.createIncomingMessage({
            account_id,
            conversation_id,
            content: msg.content || '',
            attachments: msg.attachments || [],
            source_id: msg.source_id
        });
    }
}


/**
 * 同步单条消息到 Chatwoot（保持兼容）
 */
async function syncOneMessage({ account_id, conversation_id, message, waBridgeUrl, waBridgeToken }) {
    const { id, fromMe, type, body, timestamp, media } = message;

    let attachments = [];

    if (media && !media.error) {
        if (media.data_url) {
            console.log(`[syncOneMessage] ${id}: Using data_url (${media.mimetype})`);
            attachments.push({
                data_url: media.data_url,
                file_type: media.mimetype,
                filename: media.filename || `media_${Date.now()}`
            });
        } else if (media.fileUrl && waBridgeUrl) {
            try {
                let mediaUrl = media.fileUrl;
                if (!mediaUrl.startsWith('http')) {
                    mediaUrl = `${waBridgeUrl}${mediaUrl}`;
                }

                const headers = {};
                if (waBridgeToken) headers['x-api-token'] = waBridgeToken;

                const resp = await axios.get(mediaUrl, {
                    responseType: 'arraybuffer',
                    headers,
                    timeout: 30000
                });

                const buffer = Buffer.from(resp.data);
                const mime = media.mimetype || resp.headers['content-type'] || 'application/octet-stream';
                const b64 = `data:${mime};base64,${buffer.toString('base64')}`;

                attachments.push({
                    data_url: b64,
                    file_type: mime,
                    filename: media.filename || `media_${Date.now()}`
                });

                console.log(`[syncOneMessage] ${id}: Downloaded OK, size=${buffer.length}`);
            } catch (e) {
                console.error(`[syncOneMessage] ${id}: Download failed:`, e?.message);
            }
        }
    }

    console.log(`[syncOneMessage] ${id}: fromMe=${fromMe}, type=${type}, body=${(body || '').substring(0, 50)}, attachments=${attachments.length}`);

    if (fromMe) {
        return await cw.createOutgoingMessage({
            account_id,
            conversation_id,
            content: body || '',
            attachments,
            source_id: id
        });
    } else {
        return await cw.createIncomingMessage({
            account_id,
            conversation_id,
            content: body || '',
            attachments,
            source_id: id
        });
    }
}
/**
 * 同步单条消息到 Chatwoot（保持原有逻辑不变）
 */
async function syncOneMessage({ account_id, conversation_id, message, waBridgeUrl, waBridgeToken }) {
    const { id, fromMe, type, body, timestamp, media } = message;

    let attachments = [];

    if (media && !media.error) {
        // 优先使用 data_url (base64)
        if (media.data_url) {
            console.log(`[syncOneMessage] ${id}: Using data_url (${media.mimetype})`);
            attachments.push({
                data_url: media.data_url,
                file_type: media.mimetype,
                filename: media.filename || `media_${Date.now()}`
            });
        }
        // 备用：从纳管器下载
        else if (media.fileUrl && waBridgeUrl) {
            try {
                let mediaUrl = media.fileUrl;
                if (!mediaUrl.startsWith('http')) {
                    mediaUrl = `${waBridgeUrl}${mediaUrl}`;
                }
                console.log(`[syncOneMessage] ${id}: Downloading from ${mediaUrl}`);

                const headers = {};
                if (waBridgeToken) headers['x-api-token'] = waBridgeToken;

                const resp = await axios.get(mediaUrl, {
                    responseType: 'arraybuffer',
                    headers,
                    timeout: 30000
                });

                const buffer = Buffer.from(resp.data);
                const mime = media.mimetype || resp.headers['content-type'] || 'application/octet-stream';
                const b64 = `data:${mime};base64,${buffer.toString('base64')}`;

                attachments.push({
                    data_url: b64,
                    file_type: mime,
                    filename: media.filename || `media_${Date.now()}`
                });

                console.log(`[syncOneMessage] ${id}: Downloaded OK, size=${buffer.length}`);
            } catch (e) {
                console.error(`[syncOneMessage] ${id}: Download failed:`, e?.message);
            }
        }
    }

    console.log(`[syncOneMessage] ${id}: fromMe=${fromMe}, type=${type}, body=${(body || '').substring(0, 50)}, attachments=${attachments.length}`);

    try {
        if (fromMe) {
            // 我方发送的消息 -> outgoing
            await cw.createOutgoingMessage({
                account_id,
                conversation_id,
                content: body || '',
                attachments,
                source_id: id
            });
        } else {
            // 对方发送的消息 -> incoming
            await cw.createIncomingMessage({
                account_id,
                conversation_id,
                content: body || '',
                attachments,
                source_id: id
            });
        }
    } catch (e) {
        console.error(`[syncOneMessage] ${id} failed:`, e?.message);
        throw e;
    }
}


/**
 * 同步单条消息到 Chatwoot
 */

async function syncOneMessage({ account_id, conversation_id, message, waBridgeUrl, waBridgeToken }) {
    const { id, fromMe, type, body, timestamp, media } = message;

    let attachments = [];

    if (media && !media.error) {
        // 优先使用 data_url (base64)
        if (media.data_url) {
            console.log(`[syncOneMessage] ${id}: Using data_url (${media.mimetype})`);
            attachments.push({
                data_url: media.data_url,
                file_type: media.mimetype,
                filename: media.filename || `media_${Date.now()}`
            });
        }
        // 备用：从纳管器下载
        else if (media.fileUrl && waBridgeUrl) {
            try {
                let mediaUrl = media.fileUrl;
                if (!mediaUrl.startsWith('http')) {
                    mediaUrl = `${waBridgeUrl}${mediaUrl}`;
                }
                console.log(`[syncOneMessage] ${id}: Downloading from ${mediaUrl}`);

                const headers = {};
                if (waBridgeToken) headers['x-api-token'] = waBridgeToken;

                const resp = await axios.get(mediaUrl, {
                    responseType: 'arraybuffer',
                    headers,
                    timeout: 30000
                });

                const base64 = Buffer.from(resp.data).toString('base64');
                const contentType = resp.headers['content-type'] || media.mimetype || 'application/octet-stream';

                attachments.push({
                    data_url: `data:${contentType};base64,${base64}`,
                    file_type: contentType,
                    filename: media.filename || `media_${Date.now()}`
                });
                console.log(`[syncOneMessage] ${id}: Downloaded OK`);
            } catch (e) {
                console.warn(`[syncOneMessage] ${id}: Download failed: ${e?.message}`);
            }
        } else {
            console.warn(`[syncOneMessage] ${id}: No media source available`);
        }
    }

    console.log(`[syncOneMessage] ${id}: fromMe=${fromMe}, type=${type}, body=${(body||'').substring(0,30)}, attachments=${attachments.length}`);

    try {
        if (fromMe) {
            return await cw.createOutgoingMessage({
                account_id,
                conversation_id,
                content: body || '',
                attachments,
                source_id: id
            });
        } else {
            return await cw.createIncomingMessage({
                account_id,
                conversation_id,
                content: body || '',
                attachments
            });
        }
    } catch (e) {
        console.error(`[syncOneMessage] ${id} failed:`, e?.message);
        throw e;
    }
}


/**
 * GET /sync-status/:conversation_id
 * 获取会话的同步状态
 */
app.get('/sync-status/:conversation_id', async (req, res) => {
    try {
        const { conversation_id } = req.params;

        const conv = await cw.getConversationDetails(CHATWOOT_ACCOUNT_ID, conversation_id);
        if (!conv) {
            return res.status(404).json({ ok: false, error: 'Conversation not found' });
        }

        const sender = conv.meta?.sender || {};

        // 获取映射信息
        let waMapping = null;
        if (redis) {
            const keys = [
                `cw:mapping:conv:${conversation_id}`,
                `cw:mapping:contact:${sender.id}`
            ];

            for (const key of keys) {
                const data = await redis.get(key);
                if (data) {
                    try {
                        waMapping = JSON.parse(data);
                        break;
                    } catch (_) {}
                }
            }
        }

        // 尝试从联系人信息推断
        if (!waMapping) {
            const phone = sender.phone_number?.replace(/[^\d]/g, '') ||
                sender.identifier?.replace(/[^\d]/g, '');
            if (phone) {
                waMapping = {
                    sessionId: WA_DEFAULT_SESSION.split(',')[0]?.trim(),
                    phone: phone,
                    chatId: `${phone}@c.us`,
                    inferred: true
                };
            }
        }

        res.json({
            ok: true,
            conversation_id: parseInt(conversation_id),
            contact: {
                id: sender.id,
                name: sender.name,
                phone: sender.phone_number,
                identifier: sender.identifier
            },
            waMapping: waMapping ? {
                sessionId: waMapping.sessionId,
                chatId: waMapping.chatId,
                phone: waMapping.phone,
                phone_lid: waMapping.phone_lid,
                inferred: waMapping.inferred || false
            } : null,
            canSync: !!(waMapping?.sessionId && (waMapping.chatId || waMapping.phone || waMapping.phone_lid))
        });
    } catch (e) {
        res.status(500).json({ ok: false, error: e?.message });
    }
});
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
        const message_id = message.id;  // 【Bug 1 修复】保存消息 ID 用于失败标记
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

        // 【v3】使用 shouldBlockSend - 同步进行中时完全阻止所有发送
        const blockCheck = syncLockManager.shouldBlockSend(conversation_id);
        if (blockCheck.block) {
            logToCollector('[CW_WEBHOOK] Blocked by sync lock', {
                conversation_id,
                reason: blockCheck.reason,
                elapsed: blockCheck.elapsed,
                lockId: blockCheck.lockId
            });
            return SKIP(`sync_lock: ${blockCheck.reason}`);
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

        // === 发送媒体（支持多图/多附件）===
        if (attachments.length > 0) {
            const mediaList = [];

            for (const a of attachments) {
                if (!a) continue;

                // 从 file_type 判断媒体类型
                let mediaType = 'file';
                const ft = String(a.file_type || '').toLowerCase();
                if (ft.startsWith('image')) mediaType = 'image';
                else if (ft.startsWith('video')) mediaType = 'video';
                else if (ft.startsWith('audio')) mediaType = 'audio';

                let b64, url;
                let mimetype = a.file_type || 'application/octet-stream';

                const dataUrl = a.data_url || a.dataUrl;
                if (dataUrl) {
                    if (/^data:/i.test(dataUrl)) {
                        const m = dataUrl.match(/^data:(.*?);base64,(.*)$/i);
                        if (m) {
                            mimetype = m[1] || mimetype;
                            b64 = m[2];
                        }
                    } else if (/^https?:\/\//i.test(dataUrl)) {
                        url = rewriteCwDataUrl(dataUrl);
                    }
                }

                if (!b64 && !url && a.file_url && /^https?:\/\//i.test(a.file_url)) {
                    url = rewriteCwDataUrl(a.file_url);
                }

                if (!b64 && !url) continue;

                // ===== 关键修复：确保 MIME 类型是完整格式 =====
                // Chatwoot 可能返回 'image' 而不是 'image/png'
                if (mimetype && !mimetype.includes('/')) {
                    // 从文件名推断
                    const fn = (a.file_name || a.filename || '').toLowerCase();
                    if (mimetype === 'image') {
                        if (fn.endsWith('.png')) mimetype = 'image/png';
                        else if (fn.endsWith('.gif')) mimetype = 'image/gif';
                        else if (fn.endsWith('.webp')) mimetype = 'image/webp';
                        else mimetype = 'image/jpeg';  // 默认
                    } else if (mimetype === 'video') {
                        if (fn.endsWith('.webm')) mimetype = 'video/webm';
                        else if (fn.endsWith('.mov')) mimetype = 'video/quicktime';
                        else if (fn.endsWith('.avi')) mimetype = 'video/x-msvideo';
                        else mimetype = 'video/mp4';  // 默认
                    } else if (mimetype === 'audio') {
                        if (fn.endsWith('.mp3')) mimetype = 'audio/mpeg';
                        else if (fn.endsWith('.wav')) mimetype = 'audio/wav';
                        else if (fn.endsWith('.ogg') || fn.endsWith('.opus')) mimetype = 'audio/ogg';
                        else mimetype = 'audio/mpeg';  // 默认
                    } else {
                        mimetype = 'application/octet-stream';
                    }
                }

                mediaList.push({
                    type: mediaType,
                    url,
                    b64,
                    mimetype,
                    filename: a.file_name || a.filename
                });
            }

            if (mediaList.length > 0) {
                const {sessionId: finalSession, to, to_lid} = await resolveWaTarget({
                    redis,
                    conversation_id: conversation.id || conversation.conversation_id,
                    sender,
                    fallback: chatIdOrPhone,
                    WA_DEFAULT_SESSION
                });

                if (!to && !to_lid) {
                    logToCollector('[CW->WA] SKIP_MEDIA', {reason: 'no recipient'});
                    return res.json({ok: true, skipped: 'no recipient'});
                }

                logToCollector('[CW->WA] SEND_MEDIA', {
                    session: finalSession,
                    to: to || 'none',
                    to_lid: to_lid || 'none',
                    count: mediaList.length,
                    message_id
                });

                const payload = {
                    sessionId: finalSession,
                    to: to || '',
                    to_lid: to_lid || '',
                    caption: message.content || ''
                };

                if (mediaList.length === 1) {
                    const m = mediaList[0];
                    payload.type = m.type;
                    payload.mimetype = m.mimetype;
                    if (m.b64) payload.b64 = m.b64;
                    else payload.url = m.url;
                    if (m.filename) payload.filename = m.filename;
                } else {
                    payload.attachments = mediaList;
                }

                // 【Bug 1 修复】同步发送，等待结果
                try {
                    const result = await postWithRetry(`${WA_BRIDGE_URL}/send/media`, payload, headers);

                    const success = result?.ok || result?.success;
                    const failedItems = (result?.results || []).filter(r => !r.ok);

                    logToCollector('[CW->WA] MEDIA_RESULT', {
                        success,
                        total: result?.total,
                        failed: failedItems.length,
                        message_id
                    });

                    if (!success || failedItems.length > 0) {
                        const errorMsg = failedItems.map(f => f.error).join('; ') || 'media send failed';
                        await markMessageAsFailed(account_id, conversation_id, message_id, errorMsg);
                        return res.json({
                            ok: false,
                            error: errorMsg,
                            message_id,
                            failed_items: failedItems
                        });
                    }

                    return res.json({
                        ok: true,
                        sent: true,
                        mediaCount: mediaList.length,
                        message_id
                    });

                } catch (err) {
                    const errorMsg = err?.response?.data?.error || err?.message || 'unknown error';
                    console.error('[CW->WA] MEDIA_ERROR', errorMsg);
                    logToCollector('[CW->WA] MEDIA_ERROR', {
                        error: errorMsg,
                        message_id
                    });

                    await markMessageAsFailed(account_id, conversation_id, message_id, errorMsg);

                    return res.json({
                        ok: false,
                        error: errorMsg,
                        message_id
                    });
                }
            }
        }

        // === 发送纯文本 ===
        if (text && text.trim()) {
            const { sessionId: finalSession, to, to_lid } = await resolveWaTarget({
                redis,
                conversation_id: conversation.id || conversation.conversation_id,
                sender,
                fallback: chatIdOrPhone,
                WA_DEFAULT_SESSION
            });

            if (!to && !to_lid) {
                logToCollector('[CW->WA] SKIP_TEXT', { reason: 'no recipient' });
                return res.json({ ok: true, skipped: 'no recipient' });
            }

            logToCollector('[CW->WA] SEND_TEXT', {
                session: finalSession,
                to: to || 'none',
                to_lid: to_lid || 'none',
                len: text.length,
                message_id
            });

            // 【Bug 1 修复】同步发送，等待结果
            try {
                const result = await postWithRetry(
                    `${WA_BRIDGE_URL}/send/text`,
                    { sessionId: finalSession, to: to || '', to_lid: to_lid || '', text },
                    headers
                );

                const success = result?.ok;

                logToCollector('[CW->WA] TEXT_RESULT', {
                    success,
                    msgId: result?.msgId,
                    message_id
                });

                if (!success) {
                    const errorMsg = result?.error || 'text send failed';
                    await markMessageAsFailed(account_id, conversation_id, message_id, errorMsg);
                    return res.json({
                        ok: false,
                        error: errorMsg,
                        message_id
                    });
                }

                return res.json({
                    ok: true,
                    sent: true,
                    message_id,
                    waMessageId: result?.msgId
                });

            } catch (err) {
                const errorMsg = err?.response?.data?.error || err?.message || 'unknown error';
                console.error('[CW->WA] TEXT_ERROR', errorMsg);
                logToCollector('[CW->WA] TEXT_ERROR', {
                    error: errorMsg,
                    message_id
                });

                await markMessageAsFailed(account_id, conversation_id, message_id, errorMsg);

                return res.json({
                    ok: false,
                    error: errorMsg,
                    message_id
                });
            }
        }

        return res.json({ ok: true, skipped: 'no content' });
    } catch (err) {
        console.error('[WEBHOOK_ERROR]', err.response?.data || err.message);
        return res.status(500).json({ok: false, error: err.message, bridge: err.response?.data});
    }
});

// ===== 【调试端点】同步锁状态查询和强制释放 =====
app.get('/sync-lock/:conversation_id', (req, res) => {
    const { conversation_id } = req.params;
    const status = syncLockManager.getStatus(Number(conversation_id));
    res.json({ ok: true, conversation_id: Number(conversation_id), lock: status });
});

// 查看所有活跃的锁
app.get('/sync-locks', (req, res) => {
    const allLocks = syncLockManager.getAllLocks();
    res.json({ ok: true, locks: allLocks, count: Object.keys(allLocks).length });
});

app.delete('/sync-lock/:conversation_id', (req, res) => {
    const { conversation_id } = req.params;
    syncLockManager.forceRelease(Number(conversation_id));
    res.json({ ok: true, released: true });
});

app.listen(PORT, '0.0.0.0', () => {
    console.log(`[collector] up on 0.0.0.0:${PORT}`);
});