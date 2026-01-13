const path = require('path');
const express = require('express');
require('dotenv').config();

// 直接用环境变量或从 env.js 里取数，这里给一个兜底常量即可
const PROTOCOL_TIMEOUT_MS = Number(process.env.PROTOCOL_TIMEOUT_MS || 120000);

// 关键：一次性正确引入 Client 和 NoAuth（还有 MessageMedia）
const { Client, NoAuth, MessageMedia } = require('whatsapp-web.js');

const axios = require('axios');
const COLLECTOR_BASE  = process.env.COLLECTOR_BASE || `http://127.0.0.1:${process.env.COLLECTOR_PORT || 7001}`;
const COLLECTOR_TOKEN = process.env.COLLECTOR_INGEST_TOKEN || process.env.API_TOKEN || '';

const { sessionManager, SESSION_STATUS } = require('./session-manager');

// ★★★ V3.2 新增：联系人同步模块 ★★★
let contactSync = null;
try {
    contactSync = require('./contact-sync');
    console.log('[BOOT] ContactSync module loaded');
} catch (e) {
    console.log('[BOOT] ContactSync module not found, sync disabled');
}

// SESSIONS 现在由 SessionManager 动态管理
// 保留解析逻辑用于后备
const SESSIONS_ENV = (process.env.SESSIONS || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
process.on('unhandledRejection', (reason, promise) => {
    console.error('[GLOBAL] Unhandled Rejection:', reason);
    // 不要退出进程，只记录错误
});

process.on('uncaughtException', (error) => {
    console.error('[GLOBAL] Uncaught Exception:', error);
    // 对于致命错误，记录后优雅退出
    if (error.message?.includes('FATAL') || error.message?.includes('out of memory')) {
        console.error('[GLOBAL] Fatal error, exiting...');
        process.exit(1);
    }
    // 其他异常继续运行
});

// 创建 sessions Proxy 以保持向后兼容
let _sessionsStore = {};
const sessions = new Proxy(_sessionsStore, {
    get(target, prop) {
        // 优先从 sessionManager 获取
        const smSession = sessionManager.getSession(prop);
        if (smSession) {
            return {
                client: smSession.client,
                status: smSession.sessionStatus,
                chats: smSession.chats || [],
                refreshTimer: target[prop]?.refreshTimer
            };
        }
        return target[prop];
    },
    set(target, prop, value) {
        target[prop] = value;
        return true;
    }
});

function sleep(ms){ return new Promise(r=>setTimeout(r, ms)); }
function rnd(a,b){ return Math.floor(Math.random()*(b-a+1))+a; }

// ★★★ V3 新增：检查 session 是否真正可用 ★★★
function isSessionReady(sessionId) {
    const smSession = sessionManager.getSession(sessionId);
    if (!smSession) return { ready: false, reason: 'session_not_found' };

    // 检查是否有重启锁
    if (sessionManager.hasActiveRestartLock && sessionManager.hasActiveRestartLock(sessionId)) {
        return { ready: false, reason: 'restart_in_progress' };
    }

    // 检查状态
    const status = smSession.sessionStatus;
    if (status !== 'ready') {
        return { ready: false, reason: `status_${status}` };
    }

    // 检查 client
    if (!smSession.client) {
        return { ready: false, reason: 'no_client' };
    }

    // 检查 client 的 pupPage 是否有效
    try {
        if (smSession.client.pupPage && smSession.client.pupPage.isClosed()) {
            return { ready: false, reason: 'page_closed' };
        }
    } catch (_) {}

    return { ready: true };
}

// 发送限流（每号每分钟/同会话冷却）
// 发送限流（统一读取 .env.chatwoot 的 RATE_PER_MIN / SAME_CHAT_COOLDOWN_MS）
const SEND_RPM         = Number(process.env.RATE_PER_MIN || process.env.SEND_RPM || 12);
const CHAT_COOLDOWN_MS = Number(process.env.SAME_CHAT_COOLDOWN_MS || process.env.CHAT_COOLDOWN_MS || 6000);

const lastSendAtByChat    = new Map(); // `${sessionId}|${jid}` -> ts
let   sendTokens          = SEND_RPM;
let   lastFill            = Date.now();
async function takeSendToken(){
    const now = Date.now();
    const refill = ((now - lastFill)/60000) * SEND_RPM;
    sendTokens = Math.min(SEND_RPM, sendTokens + refill);
    lastFill = now;
    if (sendTokens >= 1){ sendTokens -= 1; return; }
    await sleep(1000);
    return takeSendToken();
}

// jid/phone 处理
const DIGITS = s => String(s||'').replace(/\D/g,'');
const toJid  = s => /@g\.us$|@c\.us$|@lid$/.test(s) ? s : (DIGITS(s) ? `${DIGITS(s)}@c.us` : String(s||'').trim());

// 是否包含中日韩字符
const hasCJK = (s) => /[\u4E00-\u9FFF\u3040-\u30FF\uAC00-\uD7AF]/.test(String(s||''));
const ALLOW_DOC = process.env.ALLOW_DOC === '1';
// 新增（放在 axios 定义后面）
async function postWithRetry(url, data, headers, tries = 6) {
    let delay = 500;
    for (let i = 0; i < tries; i++) {
        try {
            return await axios.post(url, data, { headers, timeout: 10000 });
        } catch (e) {
            if (i === tries - 1) throw e;
            await new Promise(r => setTimeout(r, delay));
            delay = Math.min(4000, delay * 2);
        }
    }
}
const sentMessageCache = new Map();
const SENT_CACHE_TTL = 60000;

// 【新增】发送媒体去重缓存（防止重试导致重复发送）
const sendMediaCache = new Map();
const SEND_MEDIA_CACHE_TTL = 300000;  // 5分钟

setInterval(() => {
    const now = Date.now();
    for (const [key, ts] of sentMessageCache.entries()) {
        if (now - ts > SENT_CACHE_TTL) sentMessageCache.delete(key);
    }
    // 清理发送媒体缓存
    for (const [key, data] of sendMediaCache.entries()) {
        if (now - data.ts > SEND_MEDIA_CACHE_TTL) sendMediaCache.delete(key);
    }
}, 30000);

function markSentFromChatwoot(msgId) {
    if (msgId) sentMessageCache.set(msgId, Date.now());
}

function isSentFromChatwoot(msgId) {
    return sentMessageCache.has(msgId);
}

function parseRecipient(to) {
    const raw = String(to || '').trim();
    if (!raw) return { kind: 'invalid', digits: '', jid: '' };
    // 群聊：保留原 JID
    if (/@g\.us$/i.test(raw)) return { kind: 'group', digits: '', jid: raw };
    // 个人：提取纯数字；不要把 @lid/@c.us 直接喂给 getNumberId
    const digits = raw.replace(/[^\d]/g, '');
    return digits ? { kind: 'user', digits, jid: '' } : { kind: 'invalid', digits: '', jid: '' };
}
// 估算"人类打字用时"计划（多段 typing）
function buildTypingSchedule(text, opts={}){
    const t = String(text||'');
    const wpm = Number(opts.wpm || (hasCJK(t) ? 180 : 140)); // CJK 更快（拼音输入）
    // 把英文按词，CJK按字符估算 token 数
    const tokens = hasCJK(t) ? t.replace(/\s+/g,'').length
        : t.trim().split(/\s+/).reduce((n,w)=> n + Math.max(1, Math.ceil(w.length/5)), 0);
    const totalMs = Math.max(1200, Math.round((tokens * 60000) / Math.max(60, wpm)) + rnd(400,1200));
    // 分段：每段 1.2s ~ 4s
    let remain = totalMs, plan=[];
    while(remain>0){
        const seg = Math.min(remain, rnd(1200, 4000));
        plan.push(seg); remain -= seg;
    }
    // 标点/换行稍作停顿（加一些"犹豫"）
    const hesitation = (t.match(/[.,!?;:，。？！；：\n]/g)||[]).length;
    for(let i=0;i<Math.min(plan.length, hesitation);i++) plan[i]+=rnd(200,600);
    return plan;
}

async function humanTypeThenSendText(chat, text, opts={}){
    const plan = buildTypingSchedule(text, opts);
    for(const seg of plan){
        await chat.sendStateTyping();
        await sleep(seg);
    }
    await chat.clearState();
    // 最终再随机犹豫一下
    await sleep(rnd(120, 420));
    return chat.sendMessage(String(text||''), { linkPreview: true });
}

// 拿 Chat（兼容传 phone 或 jid）
async function getChat(sessionId, idOrPhone){
    const client = sessions[sessionId]?.client;
    if (!client) throw new Error('session not found');
    const jid = toJid(idOrPhone);
    return client.getChatById(jid);
}

// 同会话冷却
async function ensureChatCooldown(sessionId, chatId){
    const key  = `${sessionId}|${chatId}`;
    const last = lastSendAtByChat.get(key) || 0;
    const delta= Date.now() - last;
    if (delta < CHAT_COOLDOWN_MS) await sleep(CHAT_COOLDOWN_MS - delta);
    lastSendAtByChat.set(key, Date.now());
}

// 加载媒体：本地文件 / 远端URL / base64
// ===== 工具：加载媒体（支持 filePath / url / b64；修复 Windows 路径误判 URL）=====

function guessMimeByExt(p){
    const ext = path.extname(p||'').toLowerCase();
    if (['.jpg','.jpeg'].includes(ext)) return 'image/jpeg';
    if (ext === '.png')  return 'image/png';
    if (ext === '.webp') return 'image/webp';
    if (ext === '.gif')  return 'image/gif';
    if (ext === '.mp4')  return 'video/mp4';
    if (ext === '.mov')  return 'video/quicktime';
    if (ext === '.webm') return 'video/webm';
    if (ext === '.mkv')  return 'video/x-matroska';
    if (ext === '.avi')  return 'video/x-msvideo';
    if (ext === '.3gp')  return 'video/3gpp';
    if (ext === '.mp3')  return 'audio/mpeg';
    if (ext === '.m4a')  return 'audio/mp4';
    if (ext === '.ogg' || ext === '.oga' || ext === '.opus') return 'audio/ogg';
    if (ext === '.wav')  return 'audio/wav';
    if (ext === '.pdf')  return 'application/pdf';
    return 'application/octet-stream';
}
function ensureFilenameByMime(name, mime) {
    const extFromMime =
        mime?.includes('jpeg') ? '.jpg' :
            mime?.includes('jpg')  ? '.jpg' :
                mime?.includes('png')  ? '.png' :
                    mime?.includes('webp') ? '.webp' :
                        mime?.includes('gif')  ? '.gif' :
                            mime?.includes('mp4')  ? '.mp4' :
                                mime?.includes('webm') ? '.webm' :
                                    mime?.includes('ogg')  ? '.ogg' :
                                        mime?.includes('opus') ? '.ogg' :
                                            mime?.includes('mp3')  ? '.mp3' :
                                                mime?.includes('wav')  ? '.wav' :
                                                    mime?.includes('pdf')  ? '.pdf' : '';

    const base = (name || 'upload').replace(/\.[a-z0-9]+$/i, '');
    return extFromMime ? `${base}${extFromMime}` : (name || 'upload.bin');
}
// ===== 新增：规范化 MIME 类型 =====
function normalizeMime(mime, filename) {
    if (!mime) return 'application/octet-stream';

    // 如果已经是完整格式，直接返回
    if (mime.includes('/')) return mime;

    // 从文件名推断
    const fn = (filename || '').toLowerCase();

    if (mime === 'image' || mime.startsWith('image')) {
        if (fn.endsWith('.png')) return 'image/png';
        if (fn.endsWith('.gif')) return 'image/gif';
        if (fn.endsWith('.webp')) return 'image/webp';
        return 'image/jpeg';
    }

    if (mime === 'video' || mime.startsWith('video')) {
        if (fn.endsWith('.webm')) return 'video/webm';
        if (fn.endsWith('.mov')) return 'video/quicktime';
        if (fn.endsWith('.avi')) return 'video/x-msvideo';
        return 'video/mp4';
    }

    if (mime === 'audio' || mime.startsWith('audio')) {
        if (fn.endsWith('.mp3')) return 'audio/mpeg';
        if (fn.endsWith('.wav')) return 'audio/wav';
        if (fn.endsWith('.ogg') || fn.endsWith('.opus')) return 'audio/ogg';
        return 'audio/mpeg';
    }

    return 'application/octet-stream';
}
async function loadMedia({ filePath, url, b64, mimetype, filename }){
    // 1) 如果传的是 Windows 驱动器路径却误放在 url 字段，自动矫正
    if (!filePath && url && /^[a-zA-Z]:[\\/]/.test(url)) {
        filePath = url; url = null;
    }

    // 2) base64 直接构建
    if (b64) {
        const clean = b64.replace(/^data:.*;base64,/, '');
        const mt = mimetype || 'application/octet-stream';
        const fn = filename || ('file' + Date.now());
        return new MessageMedia(mt, clean, fn);
    }

    // 3) 本地路径：优先从文件读，最稳
    if (filePath) {
        const abs = path.resolve(String(filePath));
        const buf = await fs.promises.readFile(abs);
        const base64 = buf.toString('base64');
        const mt = mimetype || guessMimeByExt(abs);
        const fn = filename || path.basename(abs);
        return new MessageMedia(mt, base64, fn);
    }

    // 4) URL 下载：自己用 axios 下载，避免 MessageMedia.fromUrl 的 HTTPS 验证问题
    if (url && /^http?:\/\//i.test(url)) {
        try {
            const resp = await axios.get(url, {
                responseType: 'arraybuffer',
                timeout: 60000,
                maxContentLength: 50 * 1024 * 1024,
                httpsAgent: new (require('http').Agent)({ rejectUnauthorized: false })
            });
            const buf = Buffer.from(resp.data);
            const base64 = buf.toString('base64');

            let mt = mimetype || resp.headers['content-type'] || '';
            mt = mt.split(';')[0].trim() || guessMimeByExt(url);

            let fn = filename;
            if (!fn) {
                try {
                    const urlPath = new URL(url).pathname;
                    fn = path.basename(urlPath) || ('media_' + Date.now());
                } catch (_) {
                    fn = 'media_' + Date.now();
                }
            }
            fn = ensureFilenameByMime(fn, mt);

            console.log(`[loadMedia] Downloaded ${buf.length} bytes, mime=${mt}`);
            return new MessageMedia(mt, base64, fn);
        } catch (e) {
            console.error(`[loadMedia] axios download failed: ${e.message}, trying fromUrl...`);
            return await MessageMedia.fromUrl(url, { unsafeMime: true, filename: filename || undefined });
        }
    }

    throw new Error('no media source');
}
async function sendVideo(chat, media, caption) {
    const chatId = chat?.id?._serialized || 'unknown';
    const isLid = /@lid$/i.test(chatId);

    // 检查视频大小
    const sizeBytes = Buffer.from(media.data, 'base64').length;
    const sizeMB = sizeBytes / (1024 * 1024);

    console.log(`[sendVideo] chatId=${chatId}, isLid=${isLid}, size=${sizeMB.toFixed(2)}MB, mime=${media.mimetype}, file=${media.filename}`);

    // 如果视频太大，直接用文档模式
    if (sizeMB > 16) {
        console.log(`[sendVideo] Video too large, using document mode`);
        try {
            const opts = { sendMediaAsDocument: true };
            if (caption) opts.caption = caption;
            const msg = await chat.sendMessage(media, opts);
            if (msg) return { ok: true, msg, method: 'as_document' };
        } catch (e) {
            console.log(`[sendVideo] document mode failed:`, e?.message);
        }
        return { ok: false, error: 'Video too large' };
    }

    // 方案1：直接发送（不带任何额外选项）
    try {
        const opts = {};
        if (caption) opts.caption = caption;
        const msg = await chat.sendMessage(media, opts);
        if (msg) {
            console.log(`[sendVideo] OK: default`);
            return { ok: true, msg, method: 'default' };
        }
    } catch (e) {
        console.log(`[sendVideo] default failed:`, e?.message);
    }

    // 方案2：对于小视频尝试 GIF 模式
    if (sizeMB < 8) {
        try {
            const opts = { sendVideoAsGif: true };
            if (caption) opts.caption = caption;
            const msg = await chat.sendMessage(media, opts);
            if (msg) {
                console.log(`[sendVideo] OK: as_gif`);
                return { ok: true, msg, method: 'as_gif' };
            }
        } catch (e) {
            console.log(`[sendVideo] as_gif failed:`, e?.message);
        }
    }

    // 方案3：文档模式（保底）
    try {
        const opts = { sendMediaAsDocument: true };
        if (caption) opts.caption = caption;
        const msg = await chat.sendMessage(media, opts);
        if (msg) {
            console.log(`[sendVideo] OK: as_document (fallback)`);
            return { ok: true, msg, method: 'as_document' };
        }
    } catch (e) {
        console.log(`[sendVideo] as_document failed:`, e?.message);
    }

    return { ok: false, error: 'All video methods failed' };
}
// === media save helpers ===
const fs   = require('fs');

const MEDIA_DIR = process.env.MEDIA_DIR || path.join(__dirname, 'media');
if (!fs.existsSync(MEDIA_DIR)) fs.mkdirSync(MEDIA_DIR, { recursive: true });

function mimeToExt(m) {
    if (!m) return '';
    const t = m.toLowerCase();
    if (t.includes('jpeg')) return '.jpg';
    if (t.includes('jpg'))  return '.jpg';
    if (t.includes('png'))  return '.png';
    if (t.includes('webp')) return '.webp';
    if (t.includes('gif'))  return '.gif';
    if (t.includes('mp4'))  return '.mp4';
    if (t.includes('ogg'))  return '.ogg';
    if (t.includes('opus')) return '.opus';
    if (t.includes('mp3'))  return '.mp3';
    if (t.includes('pdf'))  return '.pdf';
    if (t.includes('vnd.ms-excel') || t.includes('spreadsheet')) return '.xlsx';
    if (t.includes('msword') || t.includes('wordprocessingml')) return '.docx';
    return '';
}

function ensureDir(...segs) {
    const p = path.join(...segs);
    if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
    return p;
}

// 写入 base64 -> 文件，返回 { filePath, fileUrl, bytes }
function saveBase64ToFile(sessionId, chatId, messageId, media) {
    const { data, mimetype } = media; // data: base64 string
    const buf = Buffer.from(data, 'base64');
    const ext = mimeToExt(mimetype) || '';
    const dir = ensureDir(MEDIA_DIR, sessionId, chatId);
    const fname = `${messageId}${ext}`;
    const abs = path.join(dir, fname);
    fs.writeFileSync(abs, buf);
    const rel = abs.replace(MEDIA_DIR, '').replace(/^[\\/]/, '').split(path.sep).join('/');
    return { filePath: abs, fileUrl: `/files/${rel}`, bytes: buf.length, mimetype };
}


async function autoTakeover(page, sessionId) {
    if (!AUTO_CLICK_USE_HERE || !page) return;

    // 防抖：本轮导航只执行一次
    if (page.__waPopupHandled) return;
    page.__waPopupHandled = true;
    setTimeout(() => { page.__waPopupHandled = false; }, 2000);

    const LABELS = [
        'Use here','在此使用','在这里使用','在此使用 WhatsApp','使用此窗口',
        'Usar aquí','Usar aqui','Utiliser ici','Использовать здесь'
    ];

    const tryClick = async () => {
        try {
            const btns = await page.$$('button, [role="button"]');
            for (const b of btns) {
                const txt = await page.evaluate(el =>
                    (el.innerText || el.getAttribute('aria-label') || '').trim(), b);
                if (txt && LABELS.some(k => txt.includes(k))) {
                    console.log(`[popup][${sessionId}] click "${txt}"`);
                    await b.click({ delay: 50 });
                    return true;
                }
            }
        } catch (_) {}
        return false;
    };

    // 快速点一轮
    for (let i = 0; i < 25; i++) {
        if (await tryClick()) break;
        await page.waitForTimeout(300);
    }
}

async function closeExtraWATabs(browser, keepPage) {
    try {
        if (!browser) return;

        const doClose = async () => {
            const pages = await browser.pages();
            const waPages = pages.filter(p => p.url().includes('web.whatsapp.com'));
            // 优先保留：传入的 keepPage；若没有，则保留"最近创建的那一个"
            const keeper = keepPage || waPages[waPages.length - 1];
            for (const p of waPages) {
                if (p !== keeper) {
                    console.log('[guard] closed extra WA tab');
                    await p.close().catch(() => {});
                }
            }
        };

        await doClose();

        if (!browser.__waGuardBound) {
            browser.on('targetcreated', async target => {
                const p = await target.page().catch(() => null);
                if (p && p.url().includes('web.whatsapp.com')) {
                    // 延迟收敛，给 wweb.js 自己的 pupPage 留出初始化时间
                    setTimeout(async () => {
                        try {
                            const page = keepPage || (await browser.pages()).find(pg => pg.url().includes('web.whatsapp.com'));
                            const pages = await browser.pages();
                            for (const it of pages) {
                                if (it.url().includes('web.whatsapp.com') && it !== page) {
                                    console.log('[guard] closed extra WA tab');
                                    await it.close().catch(() => {});
                                }
                            }
                        } catch (_) {}
                    }, 800);
                }
            });
            browser.__waGuardBound = true;
        }
    } catch (_) {}
}
// AdsPower 连接参数
const ADS_BASE = process.env.ADSPOWER_BASE || 'http://127.0.0.1:50325';
const ADS_KEY = process.env.ADSPOWER_API_KEY || '';
const AUTO_CLICK_USE_HERE = String(process.env.AUTO_CLICK_USE_HERE || '1') === '1'; // 1=自动点"Use here"




// SESSIONS 检查（仅警告）
// ============================================================
if (!SESSIONS_ENV.length) {
    console.warn('[BOOT] No SESSIONS in env, will manage all AdsPower profiles');
}
async function stopProfile(user_id){
    const headers = ADS_KEY ? {'X-AdsPower-API-Key': ADS_KEY} : {};
    try{
        await axios.get(`${ADS_BASE}/api/v1/browser/stop?user_id=${encodeURIComponent(user_id)}`, { headers, timeout: 10000 });
    }catch(_){}
}

async function startProfile(user_id){
    const headers = ADS_KEY ? {'X-AdsPower-API-Key': ADS_KEY} : {};
    // 兜底：先停再启，避免拿到旧 WS
    await stopProfile(user_id);
    await new Promise(r=>setTimeout(r, 800));
    const { data } = await axios.get(`${ADS_BASE}/api/v1/browser/start?user_id=${encodeURIComponent(user_id)}`, { headers, timeout: 20000 });
    if (data.code !== 0) throw new Error('AdsPower start failed: ' + JSON.stringify(data));
    return data.data.ws.puppeteer; // 形如 ws://127.0.0.1:xxxx/devtools/browser/...
}


// [NEW] 统一获取 chats：优先实时从 webjs 拉，失败时回退到缓存
async function listChatsLiveOrCache(sessionId) {
    const sess   = sessions[sessionId];
    const client = sess?.client;

    if (client && typeof client.getChats === 'function') {
        try {
            return await client.getChats();
        } catch (_) { /* fall through to cache */ }
    }
    return Array.isArray(sess?.chats) ? sess.chats : [];
}

async function initSessionViaAdsPower(config) {
    // 支持两种调用方式：字符串（向后兼容）或配置对象
    const sessionId = typeof config === 'string' ? config : config.sessionId;
    const sessionName = typeof config === 'object' ? config.sessionName : sessionId;

    console.log(`[${sessionId}] Initializing session (name: ${sessionName})...`);


    // ★★★ V2 新增：包裹整个初始化过程 ★★★
    try {
        const ws = await startProfile(sessionId);

        // 新增：配置 web 版本缓存策略
        const WWEB_CACHE = String(process.env.WWEB_CACHE || 'local').toLowerCase();
        let webVersionCache;
        if (WWEB_CACHE === 'none') webVersionCache = {type: 'none'};
        else if (WWEB_CACHE === 'memory') webVersionCache = {type: 'memory'};
        else webVersionCache = {type: 'local'};

        // 可选：启动即清理 .wwebjs_cache
        if (String(process.env.WWEB_CACHE_CLEAN || '0') === '1') {
            try {
                fs.rmSync(path.join(process.cwd(), '.wwebjs_cache'), {recursive: true, force: true});
            } catch (_) {
            }
        }

        const client = new Client({
            authStrategy: new NoAuth(),
            puppeteer: {
                browserWSEndpoint: ws,
                headless: false,
                defaultViewport: null,
                ignoreHTTPSErrors: true,
                args: [
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-features=AutomationControlled'
                ]
            },
            webVersionCache,
            takeoverOnConflict: true,
            takeoverTimeoutMs: 1000
        });

        // 注册到 SessionManager
        sessionManager.registerSession(sessionId, client);

        // 本地 sessions 也保存一份（向后兼容）
        sessions[sessionId] = {
            client,
            status: 'initializing',
            chats: []
        };

        // 不再提示 QR（但保留事件以便调试）
        client.on('qr', () => {
            console.log(`[${sessionId}] QR shown (should be ignored when using AdsPower).`);
        });

        // —— 只保留一套 guard & 预加载逻辑 ——
        async function waitConnected(client, sessionId, tries = 12) {
            for (let i = 0; i < tries; i++) {
                try {
                    // ★ 增强：检查 client 有效性
                    if (!client || !client.pupPage) {
                        console.log(`[${sessionId}] waitConnected abort: client/page invalid`);
                        return false;
                    }

                    if (client.pupPage.isClosed()) {
                        console.log(`[${sessionId}] waitConnected abort: page closed`);
                        return false;
                    }

                    const st = await Promise.race([
                        client.getState(),
                        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000))
                    ]).catch(() => null);

                    if (st === 'CONNECTED') return true;
                } catch (_) {}

                await new Promise(r => setTimeout(r, 700));
            }
            console.log(`[${sessionId}] waitConnected timeout, continue anyway`);
            return false;
        }

        async function preloadChatsSafe(client, sessionId) {
            // ★ 增强：前置检查
            if (!client) {
                console.log(`[${sessionId}] preload skip: client is null`);
                return false;
            }

            for (let i = 1; i <= 5; i++) {
                try {
                    // ★ 增强：每次循环都检查 client 状态
                    if (!client.pupPage) {
                        console.log(`[${sessionId}] preload skip: pupPage is null (attempt ${i})`);
                        return false;
                    }

                    if (client.pupPage.isClosed()) {
                        console.log(`[${sessionId}] preload skip: page closed (attempt ${i})`);
                        return false;
                    }

                    // ★ 增强：检查 client 是否还能响应
                    const state = await Promise.race([
                        client.getState(),
                        new Promise((_, reject) => setTimeout(() => reject(new Error('state timeout')), 5000))
                    ]).catch(() => null);

                    if (!state) {
                        console.log(`[${sessionId}] preload skip: client not responding (attempt ${i})`);
                        await new Promise(r => setTimeout(r, 2000));
                        continue;
                    }

                    const chats = await client.getChats();

                    // ★ 增强：检查 sessions 是否还存在
                    if (!sessions[sessionId]) {
                        console.log(`[${sessionId}] preload skip: session removed during preload`);
                        return false;
                    }

                    sessions[sessionId].chats = chats;
                    sessionManager.updateChats(sessionId, chats);
                    console.log(`[${sessionId}] Preloaded ${chats.length} chats.`);
                    return true;

                } catch (e) {
                    console.log(`[${sessionId}] preload retry(${i}): ${e?.message || e}`);

                    // ★ 增强：特定错误提前退出
                    const abortPatterns = ['Target closed', 'Session closed', 'Execution context', 'Protocol error'];
                    if (abortPatterns.some(p => e?.message?.includes(p))) {
                        console.log(`[${sessionId}] preload abort: browser/context destroyed`);
                        return false;
                    }

                    await new Promise(r => setTimeout(r, 2000));
                }
            }

            console.log(`[${sessionId}] preload failed after 5 retries`);
            return false;
        }






        // 统一的页面守护
        const pageGuard = async () => {
            try {
                const page = client.pupPage;
                if (page) {
                    await autoTakeover(page, sessionId);
                    if (!AUTO_CLICK_USE_HERE || !page) return;
                    await closeExtraWATabs(page.browser(), page);
                }
            } catch (_) {
            }
        };

        client.on('loading_screen', pageGuard);

        client.once('ready', async () => {
            sessions[sessionId].status = 'ready';
            sessionManager.updateStatus(sessionId, 'ready');
            console.log(`[${sessionId}] Client is ready (AdsPower profile: ${sessionName}).`);

            await pageGuard();
            try {
                if (client.pupPage) {
                    client.pupPage.setDefaultTimeout(PROTOCOL_TIMEOUT_MS);
                    client.pupPage.setDefaultNavigationTimeout(PROTOCOL_TIMEOUT_MS);
                }
            } catch (_) {
            }

            await waitConnected(client, sessionId);
            await preloadChatsSafe(client, sessionId);

            // === 后台定时刷新缓存 ===
            if (sessions[sessionId].refreshTimer) {
                clearInterval(sessions[sessionId].refreshTimer);
                delete sessions[sessionId].refreshTimer;
            }
            sessions[sessionId].refreshTimer = setInterval(async () => {
                try {
                    const chats = await listChatsLiveOrCache(sessionId);
                    sessions[sessionId].chats = chats;
                    sessionManager.updateChats(sessionId, chats);
                } catch (_) {
                }
            }, 4000);

            // === 新增：收集历史消息用于同步 ===
            if (String(process.env.SYNC_ON_STARTUP || '1') === '1') {
                try {
                    await sessionManager.collectHistoryForStartupSync(sessionId, client);
                } catch (e) {
                    console.warn(`[${sessionId}] History collection failed:`, e.message);
                }
            }
        });

        client.on('auth_failure', msg => {
            sessions[sessionId].status = 'auth_failure';
            sessionManager.updateStatus(sessionId, 'auth_failure');
            console.error(`[${sessionId}] Auth failure: ${msg}`);
        });

        client.on('disconnected', async (reason) => {
            console.log(`[${sessionId}] Client disconnected: ${reason}`);

            // 1. 更新状态
            if (sessions[sessionId]) {
                sessions[sessionId].status = 'disconnected';
            }
            sessionManager.updateStatus(sessionId, 'disconnected');

            // 2. 清理定时器
            if (sessions[sessionId]?.refreshTimer) {
                clearInterval(sessions[sessionId].refreshTimer);
                delete sessions[sessionId].refreshTimer;
            }

            // 3. ★★★ 关键修复：委托给 SessionManager 处理重连 ★★★
            // 不再直接递归调用 initSessionViaAdsPower
            // 使用 setTimeout 确保当前事件处理完成后再触发重连
            setTimeout(async () => {
                try {
                    // 检查是否已经有重连在进行
                    if (sessionManager.hasActiveRestartLock(sessionId)) {
                        console.log(`[${sessionId}] Restart already in progress, skipping...`);
                        return;
                    }

                    console.log(`[${sessionId}] Initiating safe restart via SessionManager...`);
                    const result = await sessionManager.safeRestartSession(sessionId, `disconnected:${reason}`);

                    if (!result.success) {
                        console.error(`[${sessionId}] Safe restart failed: ${result.error}`);
                    }
                } catch (e) {
                    console.error(`[${sessionId}] Reconnect error (caught):`, e?.message || e);
                    // 错误已被捕获，不会导致进程崩溃
                }
            }, 1000); // 延迟 1 秒再重连
        });

        client.on('message_ack', async (msg, ack) => {
            try {
                if (!msg.fromMe) return;
                const msgId = msg.id?._serialized || '';
                if (ack === 0) {
                    console.error(`[${sessionId}] MSG SEND FAILED: ${msgId.substring(0, 30)}...`);
                    try {
                        await postWithRetry(`${COLLECTOR_BASE}/message-status`, {
                            sessionId, messageId: msgId, ack, status: 'failed', timestamp: Date.now()
                        }, {'x-api-token': COLLECTOR_TOKEN});
                    } catch (_) {
                    }
                }
            } catch (_) {
            }
        });


        // === 监听 message_create 同步其他设备发送的消息（增强版）===
        client.on('message_create', async (message) => {
            try {
                // 1. 只处理自己发送的消息
                if (!message.fromMe) return;

                const msgId = message.id?._serialized || '';
                if (!msgId) return;

                // ★★★ 修复竞态条件：延迟检查，给 markSentFromChatwoot 足够时间执行 ★★★
                // sendMessage() 和 message_create 事件可能在同一个事件循环中触发
                // 导致 isSentFromChatwoot 检查时 msgId 还未被标记
                await new Promise(r => setTimeout(r, 100));

                // 2. 检查是否是从 Chatwoot 发送的（避免循环）
                if (isSentFromChatwoot(msgId)) {
                    // 静默跳过，不打印日志（太频繁）
                    return;
                }

                // 3. 跳过群组和广播消息
                const toJid = message.to || '';
                if (/@g\.us$/i.test(toJid) || /@broadcast/i.test(toJid)) {
                    return;
                }

                // 4. 提取收件人信息
                let phone = '', phone_lid = '';
                if (/@c\.us$/i.test(toJid)) {
                    phone = toJid.replace(/@.*/, '').replace(/\D/g, '');
                } else if (/@lid$/i.test(toJid)) {
                    phone_lid = toJid.replace(/@.*/, '').replace(/\D/g, '');
                }

                // 5. 获取聊天和联系人信息
                const chat = await message.getChat().catch(() => null);
                const contact = await chat?.getContact().catch(() => null);

                // 如果是 @lid，尝试从 chat 获取真实电话
                if (phone_lid && !phone && chat?.id?._serialized) {
                    if (/@c\.us$/i.test(chat.id._serialized)) {
                        phone = chat.id._serialized.replace(/@.*/, '').replace(/\D/g, '');
                    }
                }

                console.log(`[${sessionId}] OUTGOING (other device): ${msgId.substring(0, 30)}..., type=${message.type}, to=${phone || phone_lid || 'unknown'}`);

                // 6. 构建同步 payload
                const payload = {
                    sessionId,
                    sessionName: sessionManager.getSessionName(sessionId) || sessionId,  // ★ 新增：动态名称
                    messageId: msgId,
                    phone,
                    phone_lid,
                    name: contact?.pushname || contact?.name || chat?.name || '',
                    text: message.body || '',
                    type: message.type || 'chat',
                    timestamp: message.timestamp ? (message.timestamp * 1000) : Date.now(),
                    to: toJid,
                    chatId: chat?.id?._serialized || toJid,
                    fromMe: true,
                    direction: 'outgoing'
                };

                // ★★★ V5.3：出站消息引用处理（与入站消息保持一致）★★★
                const hasQuote = message.hasQuotedMsg ||
                    !!(message._data?.quotedStanzaID) ||
                    !!(message._data?.quotedMsg);

                if (hasQuote) {
                    try {
                        let quotedMsg = null;

                        // 方法1：通过 getQuotedMessage API
                        if (message.hasQuotedMsg) {
                            quotedMsg = await message.getQuotedMessage();
                        }

                        // 方法2：如果API失败，尝试从 _data 构建引用信息
                        if (!quotedMsg && message._data?.quotedMsg) {
                            const qd = message._data.quotedMsg;
                            quotedMsg = {
                                id: { _serialized: qd.id || message._data.quotedStanzaID || '' },
                                body: qd.body || qd.caption || '',
                                type: qd.type || 'chat',
                                fromMe: !!qd.fromMe,
                                timestamp: qd.t || 0
                            };
                        }

                        // 方法3：如果只有 quotedStanzaID，构建最小引用信息
                        if (!quotedMsg && message._data?.quotedStanzaID) {
                            quotedMsg = {
                                id: { _serialized: message._data.quotedStanzaID },
                                body: '',
                                type: 'chat',
                                fromMe: false,
                                timestamp: 0
                            };
                        }

                        if (quotedMsg) {
                            payload.quotedMsg = {
                                id: quotedMsg.id?._serialized || quotedMsg.id?.id || '',
                                body: quotedMsg.body || quotedMsg.caption || '',
                                type: quotedMsg.type || 'chat',
                                fromMe: !!quotedMsg.fromMe,
                                timestamp: quotedMsg.timestamp || 0,
                                hasMedia: !!quotedMsg.hasMedia
                            };
                            console.log(`[${sessionId}] Outgoing quote detected: ${payload.quotedMsg.id?.substring(0, 30)}..., body="${(payload.quotedMsg.body || '').substring(0, 20)}..."`);
                        }
                    } catch (e) {
                        console.log(`[${sessionId}] Failed to get quoted message for outgoing: ${e.message}`);
                    }
                }

                // 7. 媒体处理（增强版：多次重试 + 延迟 + DOM 备选）
                const MEDIA_TYPES = new Set(['image', 'video', 'audio', 'ptt', 'sticker', 'document']);
                if (message.hasMedia || MEDIA_TYPES.has(message.type)) {
                    try {
                        let media = null;
                        const MAX_MEDIA_RETRIES = 3;

                        // ★★★ 增强：多次重试下载媒体 ★★★
                        for (let attempt = 1; attempt <= MAX_MEDIA_RETRIES && !media?.data; attempt++) {
                            // 方法1：使用 API 下载
                            try {
                                const mediaPromise = message.downloadMedia();
                                const timeoutPromise = new Promise((_, reject) =>
                                    setTimeout(() => reject(new Error('timeout')), 15000)  // 增加到15秒
                                );
                                media = await Promise.race([mediaPromise, timeoutPromise]);

                                if (media?.data) {
                                    console.log(`[${sessionId}] Media API OK on attempt ${attempt}`);
                                    break;
                                }
                            } catch (e1) {
                                console.log(`[${sessionId}] Media API attempt ${attempt}/${MAX_MEDIA_RETRIES} failed: ${e1?.message}`);
                            }

                            // 如果第一次失败，等待后重试（媒体可能还在上传）
                            if (attempt < MAX_MEDIA_RETRIES && !media?.data) {
                                const waitTime = attempt * 2000;  // 2秒, 4秒
                                console.log(`[${sessionId}] Waiting ${waitTime}ms before retry...`);
                                await new Promise(r => setTimeout(r, waitTime));
                            }
                        }

                        // 方法2：DOM 提取备选（仅图片/贴纸）- 增强版
                        if (!media?.data && (message.type === 'image' || message.type === 'sticker')) {
                            console.log(`[${sessionId}] Trying DOM extraction for outgoing media...`);

                            // 等待一下让图片加载到 DOM
                            await new Promise(r => setTimeout(r, 1500));

                            try {
                                const clientRef = sessions[sessionId]?.client;
                                if (clientRef?.pupPage) {
                                    const extracted = await clientRef.pupPage.evaluate(async (msgId) => {
                                        // 尝试多种选择器
                                        const msgEl = document.querySelector(`[data-id="${msgId}"]`);
                                        if (!msgEl) return {error: 'msg_not_found'};

                                        // 尝试多种图片选择器
                                        const img = msgEl.querySelector('img[src^="blob:"]')
                                            || msgEl.querySelector('img[draggable="false"]')
                                            || msgEl.querySelector('img[src*="media"]')
                                            || msgEl.querySelector('img:not([src^="data:image/svg"])');

                                        if (img?.src?.startsWith('blob:')) {
                                            try {
                                                const response = await fetch(img.src);
                                                const blob = await response.blob();
                                                return new Promise((resolve) => {
                                                    const reader = new FileReader();
                                                    reader.onloadend = () => {
                                                        const base64 = reader.result;
                                                        const matches = base64.match(/^data:(.+);base64,(.+)$/);
                                                        if (matches) {
                                                            resolve({mimetype: matches[1], data: matches[2]});
                                                        } else {
                                                            resolve({error: 'invalid_base64'});
                                                        }
                                                    };
                                                    reader.readAsDataURL(blob);
                                                });
                                            } catch (e) {
                                                return {error: 'blob_fetch_failed: ' + e.message};
                                            }
                                        }

                                        // 如果 img.src 是普通 URL，尝试获取
                                        if (img?.src && !img.src.startsWith('data:')) {
                                            try {
                                                const response = await fetch(img.src);
                                                const blob = await response.blob();
                                                return new Promise((resolve) => {
                                                    const reader = new FileReader();
                                                    reader.onloadend = () => {
                                                        const base64 = reader.result;
                                                        const matches = base64.match(/^data:(.+);base64,(.+)$/);
                                                        if (matches) {
                                                            resolve({mimetype: matches[1], data: matches[2]});
                                                        } else {
                                                            resolve({error: 'invalid_base64'});
                                                        }
                                                    };
                                                    reader.readAsDataURL(blob);
                                                });
                                            } catch (e) {
                                                return {error: 'url_fetch_failed: ' + e.message};
                                            }
                                        }

                                        return {error: 'no_image_src', imgSrc: img?.src?.substring(0, 50)};
                                    }, message.id._serialized);

                                    if (extracted?.data) {
                                        media = extracted;
                                        console.log(`[${sessionId}] DOM extraction OK for outgoing`);
                                    } else {
                                        console.log(`[${sessionId}] DOM extraction failed: ${extracted?.error}`);
                                    }
                                }
                            } catch (domErr) {
                                console.log(`[${sessionId}] DOM extraction error: ${domErr?.message}`);
                            }
                        }

                        // 构建附件
                        if (media && media.mimetype && media.data) {
                            let filename = media.filename;
                            if (!filename) {
                                const ext = media.mimetype?.split('/')[1]?.split(';')[0] || 'bin';
                                filename = `${message.type || 'file'}_${Date.now()}.${ext}`;
                            }

                            payload.attachment = {
                                data_url: `data:${media.mimetype};base64,${media.data}`,
                                mime: media.mimetype,
                                filename: filename,
                                size: media.data.length
                            };

                            console.log(`[${sessionId}] Media ready: ${media.mimetype}, ${(media.data.length / 1024).toFixed(1)}KB`);
                        } else {
                            console.log(`[${sessionId}] Media unavailable for outgoing message after all attempts`);
                        }

                    } catch (mediaErr) {
                        console.error(`[${sessionId}] Media processing error:`, mediaErr?.message);
                    }
                }

                // 8. 发送到 collector
                try {
                    await postWithRetry(
                        `${COLLECTOR_BASE}/ingest-outgoing`,
                        payload,
                        {'x-api-token': COLLECTOR_TOKEN}
                    );
                    console.log(`[${sessionId}] Outgoing sync OK: ${msgId.substring(0, 25)}...`);
                } catch (pushErr) {
                    console.error(`[${sessionId}] Outgoing sync FAIL:`, pushErr?.message);
                }

            } catch (e) {
                console.error(`[${sessionId}] message_create error:`, e?.message || e);
            }
        });


        // 原有的 message 事件处理（入站消息）- 保持不变
        client.on('message', async (message) => {
            try {
                const sess = sessions[sessionId] || (sessions[sessionId] = {});

                const chat = await message.getChat();

                // 初始化缓存数组
                const arr = Array.isArray(sess.chats) ? sess.chats : (sess.chats = []);

                // 定位并更新/插入该会话
                const idSer = chat?.id?._serialized;
                const idx = arr.findIndex(c => c?.id?._serialized === idSer);
                if (idx === -1) {
                    arr.push(chat);
                } else {
                    arr[idx] = chat;
                }

                // 轻量补齐一些常用字段，便于接口直接读取
                const rec = arr[arr.length - 1];
                rec.__lastMessage = {
                    id: message.id?._serialized || message.id,
                    fromMe: !!message.fromMe,
                    type: message.type,
                    body: message.body,
                    timestamp: message.timestamp || Date.now()
                };
                const AUTO_SAVE_MEDIA = process.env.AUTO_SAVE_MEDIA !== '0';
                if (AUTO_SAVE_MEDIA) {
                    try {
                        const chatIdSer = chat.id?._serialized;
                        const MEDIA_TYPES = new Set(['image', 'video', 'audio', 'ptt', 'sticker', 'document']);
                        if (message.hasMedia || MEDIA_TYPES.has(message.type)) {
                            const media = await message.downloadMedia().catch(() => null);
                            if (media && media.data) {
                                const saved = saveBase64ToFile(sessionId, chatIdSer, (message.id?._serialized || message.id), media);
                                rec.__lastMessage.media = {
                                    mimetype: saved.mimetype,
                                    fileUrl: saved.fileUrl,
                                    bytes: saved.bytes
                                };
                            }
                        }
                    } catch (e) {
                        console.log(`[${sessionId}] media auto-save fail:`, e?.message || e);
                    }
                }
                rec.__name = chat.name || chat.pushname || chat.formattedTitle || '';

                // ===== 区分真实电话(@c.us)和隐私号(@lid) =====
                const fromJidRaw = message.from || '';
                let phone = '';
                let phone_lid = '';

                if (/@c\.us$/i.test(fromJidRaw)) {
                    phone = fromJidRaw.replace(/@.*/, '').replace(/\D/g, '');
                } else if (/@lid$/i.test(fromJidRaw)) {
                    phone_lid = fromJidRaw.replace(/@.*/, '').replace(/\D/g, '');
                    if (chat?.id?._serialized && /@c\.us$/i.test(chat.id._serialized)) {
                        phone = chat.id._serialized.replace(/@.*/, '').replace(/\D/g, '');
                    }
                } else {
                    if (chat?.id?._serialized) {
                        if (/@c\.us$/i.test(chat.id._serialized)) {
                            phone = chat.id._serialized.replace(/@.*/, '').replace(/\D/g, '');
                        } else if (/@lid$/i.test(chat.id._serialized)) {
                            phone_lid = chat.id._serialized.replace(/@.*/, '').replace(/\D/g, '');
                        }
                    }
                }

                console.log(`[${sessionId}] Parsed recipient: phone=${phone || 'none'}, lid=${phone_lid || 'none'}`);

                rec.__jid = fromJidRaw || (chat?.id?._serialized || '');
                rec.__server = chat.id?.server || '';
                rec.__phone = phone;
                rec.__phone_lid = phone_lid;

                const jidSer = chat?.id?._serialized || '';
                const fromJid = message.from || '';
                const isGroup = !!chat.isGroup || /@g\.us$/i.test(jidSer) || /@g\.us$/i.test(fromJid);
                const isStatus = fromJid === 'status@broadcast'
                    || /@broadcast/i.test(fromJid)
                    || /@broadcast/i.test(jidSer)
                    || String((chat.id && chat.id.user) || '').toLowerCase() === 'status';

                if (isGroup || isStatus) {
                    console.log(`[${sessionId}] skip group/status message: ${message.id?._serialized || message.id}`);
                    return;
                }

                try {
                    const payload = {
                        sessionId: sessionId,
                        sessionName: sessionManager.getSessionName(sessionId) || sessionId,  // ★ 新增：动态名称
                        phone: rec.__phone || '',
                        phone_lid: rec.__phone_lid || '',
                        name: rec.__name || '',
                        text: message.body || '',
                        type: message.type || 'chat',
                        messageId: (message.id && (message.id._serialized || message.id.id)) || '',
                        timestamp: message.timestamp ? (message.timestamp * 1000) : Date.now(),
                        from: message.from || '',
                        server: (chat.id && chat.id.server) || '',
                        chatId: chat?.id?._serialized || ''
                    };

                    // ★★★ V5.2增强：引用消息检测（主方法 + 备用方法）★★★
                    // 主方法：使用 hasQuotedMsg API
                    // 备用方法：检查 _data.quotedStanzaID（某些情况下 hasQuotedMsg 可能为 false 但实际有引用）
                    const hasQuote = message.hasQuotedMsg ||
                        !!(message._data?.quotedStanzaID) ||
                        !!(message._data?.quotedMsg);

                    if (hasQuote) {
                        try {
                            let quotedMsg = null;

                            // 方法1：通过 getQuotedMessage API
                            if (message.hasQuotedMsg) {
                                quotedMsg = await message.getQuotedMessage();
                            }

                            // 方法2：如果API失败，尝试从 _data 构建引用信息
                            if (!quotedMsg && message._data?.quotedMsg) {
                                const qd = message._data.quotedMsg;
                                quotedMsg = {
                                    id: { _serialized: qd.id || message._data.quotedStanzaID || '' },
                                    body: qd.body || qd.caption || '',
                                    type: qd.type || 'chat',
                                    fromMe: !!qd.fromMe,
                                    timestamp: qd.t || 0
                                };
                            }

                            if (quotedMsg) {
                                payload.quotedMsg = {
                                    id: quotedMsg.id?._serialized || quotedMsg.id?.id || '',
                                    body: quotedMsg.body || '',
                                    type: quotedMsg.type || 'chat',
                                    fromMe: !!quotedMsg.fromMe,
                                    timestamp: quotedMsg.timestamp || 0,
                                    hasMedia: !!quotedMsg.hasMedia
                                };
                                console.log(`[${sessionId}] Quoted message: ${payload.quotedMsg.id?.substring(0, 30)}...`);
                            }
                        } catch (e) {
                            console.log(`[${sessionId}] Failed to get quoted message: ${e.message}`);
                        }
                    }

                    // ★★★ V4 改进：媒体处理（增加重试机制和更长超时）★★★
                    if (message.hasMedia === true && typeof message.downloadMedia === 'function') {
                        try {
                            let media = null;
                            const maxRetries = 3;

                            // ★★★ V5.3.15：增加初始等待时间，让媒体有时间加载 ★★★
                            await new Promise(r => setTimeout(r, 2000));

                            // 重试下载媒体
                            for (let attempt = 1; attempt <= maxRetries && !media?.data; attempt++) {
                                try {
                                    console.log(`[${sessionId}] Downloading media (attempt ${attempt}/${maxRetries})...`);
                                    const mediaPromise = message.downloadMedia();
                                    const timeoutPromise = new Promise((_, reject) =>
                                        setTimeout(() => reject(new Error('timeout')), 15000)  // 15秒超时
                                    );
                                    media = await Promise.race([mediaPromise, timeoutPromise]);

                                    if (media?.data) {
                                        console.log(`[${sessionId}] Media download OK on attempt ${attempt}`);
                                        break;
                                    }
                                } catch (e1) {
                                    const errMsg = e1?.message || 'unknown';
                                    console.log(`[${sessionId}] Media download attempt ${attempt} failed: ${errMsg}`);

                                    // ★★★ V5.3.14修复：如果是 addAnnotations 错误，直接跳到 DOM 提取 ★★★
                                    if (errMsg.includes('addAnnotations') || errMsg.includes('Evaluation failed')) {
                                        console.log(`[${sessionId}] Detected library internal error, skipping to DOM extraction`);
                                        break;  // 不再重试 API，直接尝试 DOM 提取
                                    }

                                    if (attempt < maxRetries) {
                                        await new Promise(r => setTimeout(r, 2000));  // 等待 2 秒后重试
                                    }
                                }
                            }

                            // ★★★ 如果 API 失败，尝试 DOM 提取（V5.3.15增强：多次重试）★★★
                            if (!media?.data && (message.type === 'image' || message.type === 'sticker')) {
                                console.log(`[${sessionId}] API failed, extracting from DOM...`);

                                const clientRef = sessions[sessionId]?.client;
                                if (clientRef?.pupPage) {
                                    // ★★★ V5.3.15：DOM 提取重试机制 ★★★
                                    const domRetries = 3;
                                    const domRetryDelays = [2000, 3000, 5000];  // 逐渐增加等待时间

                                    for (let domAttempt = 1; domAttempt <= domRetries && !media?.data; domAttempt++) {
                                        try {
                                            // 等待 DOM 渲染
                                            const waitTime = domRetryDelays[domAttempt - 1] || 5000;
                                            console.log(`[${sessionId}] DOM extraction attempt ${domAttempt}/${domRetries}, waiting ${waitTime}ms...`);
                                            await new Promise(r => setTimeout(r, waitTime));

                                            const extracted = await clientRef.pupPage.evaluate(async (msgId) => {
                                                // ★★★ V5.3.15：先尝试滚动到最新消息区域 ★★★
                                                const scrollContainer = document.querySelector('[data-testid="conversation-panel-messages"]')
                                                    || document.querySelector('div[class*="message-list"]')
                                                    || document.querySelector('div[role="region"][tabindex="-1"]');
                                                if (scrollContainer) {
                                                    scrollContainer.scrollTop = scrollContainer.scrollHeight;
                                                    await new Promise(r => setTimeout(r, 500));  // 等待滚动完成
                                                }

                                                // 尝试多种选择器找到消息元素
                                                let msgEl = document.querySelector(`[data-id="${msgId}"]`);

                                                // 如果找不到，尝试其他方式
                                                if (!msgEl) {
                                                    // 尝试通过部分 ID 匹配
                                                    const partialId = msgId.split('_').pop();
                                                    msgEl = document.querySelector(`[data-id*="${partialId}"]`);
                                                }

                                                // ★★★ V5.3.15：如果还找不到，尝试查找最近的图片消息 ★★★
                                                if (!msgEl) {
                                                    // 获取所有消息元素
                                                    const allMsgs = document.querySelectorAll('[data-id]');
                                                    // 找到最后一个包含图片的消息
                                                    for (let i = allMsgs.length - 1; i >= 0; i--) {
                                                        const msg = allMsgs[i];
                                                        if (msg.querySelector('img[src^="blob:"]')) {
                                                            msgEl = msg;
                                                            break;
                                                        }
                                                    }
                                                }

                                                if (!msgEl) return {error: 'msg_not_found'};

                                                // 检查是否需要点击下载
                                                const downloadBtn = msgEl.querySelector('[data-icon="media-download"]')
                                                    || msgEl.querySelector('[data-icon="download"]')
                                                    || msgEl.querySelector('span[data-icon="media-download"]')?.closest('button, div[role="button"]');

                                                if (downloadBtn) {
                                                    downloadBtn.click();
                                                    await new Promise(r => setTimeout(r, 8000));  // 等待下载完成
                                                }

                                                // 尝试多种方式获取图片
                                                const img = msgEl.querySelector('img[src^="blob:"]')
                                                    || msgEl.querySelector('img[draggable="false"][src^="blob:"]')
                                                    || msgEl.querySelector('img[data-plain-text]')
                                                    || msgEl.querySelector('div[data-testid="image-thumb"] img');

                                                if (img?.src?.startsWith('blob:')) {
                                                    try {
                                                        const response = await fetch(img.src);
                                                        const blob = await response.blob();
                                                        return new Promise((resolve) => {
                                                            const reader = new FileReader();
                                                            reader.onloadend = () => {
                                                                const base64 = reader.result;
                                                                const matches = base64.match(/^data:(.+);base64,(.+)$/);
                                                                if (matches) {
                                                                    resolve({mimetype: matches[1], data: matches[2]});
                                                                } else {
                                                                    resolve({error: 'invalid_base64'});
                                                                }
                                                            };
                                                            reader.onerror = () => resolve({error: 'reader_error'});
                                                            reader.readAsDataURL(blob);
                                                        });
                                                    } catch (e) {
                                                        return {error: 'blob_fetch_failed: ' + e.message};
                                                    }
                                                }

                                                // 返回更详细的错误信息
                                                const hasImg = !!msgEl.querySelector('img');
                                                return {error: `no_blob_src (hasImg: ${hasImg})`};
                                            }, message.id._serialized);

                                            if (extracted?.data) {
                                                media = extracted;
                                                console.log(`[${sessionId}] DOM extraction OK on attempt ${domAttempt}`);
                                            } else {
                                                console.log(`[${sessionId}] DOM extraction attempt ${domAttempt} failed: ${extracted?.error || 'unknown'}`);
                                            }
                                        } catch (domErr) {
                                            console.log(`[${sessionId}] DOM extraction attempt ${domAttempt} error: ${domErr?.message}`);
                                        }
                                    }
                                }
                            }

                            if (media && media.mimetype && media.data) {
                                payload.attachment = {
                                    data_url: `data:${media.mimetype};base64,${media.data}`,
                                    mime: media.mimetype,
                                    name: media.filename || undefined
                                };
                                console.log(`[${sessionId}] media OK: ${media.mimetype}, ${media.data.length} bytes`);
                            } else {
                                console.log(`[${sessionId}] media unavailable after all attempts`);
                            }
                        } catch (mediaErr) {
                            console.log(`[${sessionId}] media error: ${mediaErr?.message}`);
                        }
                    }

                    console.log(`[${sessionId}] >>> PUSH: phone=${payload.phone}, text="${(payload.text || '').slice(0, 30)}...", hasQuote=${!!payload.quotedMsg}, quoteBody=${payload.quotedMsg?.body?.substring(0, 20) || 'none'}`);

                    await postWithRetry(`${COLLECTOR_BASE}/ingest`, payload, {'x-api-token': COLLECTOR_TOKEN});

                    console.log(`[${sessionId}] <<< PUSH OK`);

                } catch (e) {
                    console.error(`[${sessionId}] <<< PUSH FAIL: ${e?.response?.status || ''} ${e?.message || e}`);
                }

                sess.__debounced = sess.__debounced || {};
                clearTimeout(sess.__debounced.msgUpdate);
                sess.__debounced.msgUpdate = setTimeout(async () => {
                    try {
                        sess.chats = await listChatsLiveOrCache(sessionId);
                    } catch (_) {
                    }
                }, 1000);

            } catch (err) {
                console.error(`[${sessionId}] Error updating chat cache:`, err.message);
            }
        });

        // ★★★ 新增：监听消息撤回/删除事件 ★★★
        client.on('message_revoke_everyone', async (revokedMsg, oldMsg) => {
            try {
                // ★★★ V5.3.13修复：使用oldMsg的ID，这才是被撤回的原始消息ID ★★★
                // revokedMsg.id 是"撤回通知"的ID（新生成的）
                // oldMsg.id 才是原始消息的ID
                const msgId = oldMsg?.id?._serialized || oldMsg?.id?.id || revokedMsg?.id?._serialized || '';
                const from = revokedMsg?.from || oldMsg?.from || '';
                const chatId = revokedMsg?.to || oldMsg?.to || from;

                // 解析电话号码
                let phone = '';
                let phone_lid = '';

                if (/@c\.us$/i.test(chatId)) {
                    phone = chatId.replace(/@.*/, '').replace(/\D/g, '');
                } else if (/@lid$/i.test(chatId)) {
                    phone_lid = chatId.replace(/@.*/, '').replace(/\D/g, '');
                }

                console.log(`[${sessionId}] Message revoked: ${msgId?.substring(0, 40)}... (original msg)`);

                // 发送撤回事件到 Collector
                const payload = {
                    type: 'message_revoke',
                    sessionId: sessionId,
                    sessionName: sessionManager.getSessionName(sessionId) || sessionId,
                    phone: phone,
                    phone_lid: phone_lid,
                    messageId: msgId,
                    revokedBy: from,
                    originalBody: oldMsg?.body || '',
                    originalType: oldMsg?.type || '',
                    timestamp: Date.now()
                };

                await postWithRetry(
                    `${COLLECTOR_BASE}/ingest`,
                    payload,
                    { 'x-api-token': COLLECTOR_TOKEN }
                ).catch(e => {
                    console.error(`[${sessionId}] Failed to push revoke event: ${e.message}`);
                });

            } catch (e) {
                console.error(`[${sessionId}] Error handling message_revoke_everyone:`, e.message);
            }
        });

        // 实时跟踪会话属性变化
        client.on('chat_update', (chat) => {
            try {
                const sess = sessions[sessionId] || (sessions[sessionId] = {});
                const arr = Array.isArray(sess.chats) ? sess.chats : (sess.chats = []);
                const idS = chat?.id?._serialized;
                const i = arr.findIndex(c => c?.id?._serialized === idS);
                if (i === -1) arr.push(chat);
                else arr[i] = chat;
            } catch (_) {
            }
        });

        await client.initialize();

        setTimeout(() => {
            try {
                const page = sessions[sessionId]?.client?.pupPage;
                if (page && !page.__waGuardBound) {
                    page.on('framenavigated', async () => {
                        await autoTakeover(page, sessionId);
                        if (!AUTO_CLICK_USE_HERE || !page) return;
                        await closeExtraWATabs(page.browser(), page);
                    });
                    page.__waGuardBound = true;
                }
            } catch (_) {
            }
        }, 500);

        setTimeout(() => {
            const page = sessions[sessionId]?.client?.pupPage;
            if (page) autoTakeover(page, sessionId).catch(() => {
            });
        }, 1200);
    } catch (initError) {
        console.error(`[${sessionId}] Init failed:`, initError?.message || initError);

        // 更新状态为错误
        if (sessions[sessionId]) {
            sessions[sessionId].status = 'error';
        }
        sessionManager.updateStatus(sessionId, 'error');

        // 向上抛出，让调用者处理
        throw initError;
    }
}

// 重写启动逻辑
(async () => {
    try {
        // 初始化 SessionManager，获取要管理的会话配置
        const sessionConfigs = await sessionManager.initialize(process.env.SESSIONS);

        // ★ 注册会话初始化函数（用于自动恢复）
        sessionManager.setInitSessionFunction(initSessionViaAdsPower);

        if (sessionConfigs.length === 0) {
            console.error('[BOOT] No sessions to manage. Check AdsPower connection or SESSIONS env.');
            process.exit(1);
        }

        console.log(`[BOOT] Initializing ${sessionConfigs.length} sessions...`);

        // 依次初始化每个会话
        for (const config of sessionConfigs) {
            try {
                await initSessionViaAdsPower(config);
                // 间隔 2 秒，避免同时启动太多浏览器
                await new Promise(r => setTimeout(r, 2000));
            } catch (e) {
                console.error(`[${config.sessionId}] init failed:`, e?.message || e);

                // ★★★ V3.1 修复：初始化失败后延迟重启 ★★★
                console.log(`[${config.sessionId}] Scheduling restart in 10 seconds...`);
                setTimeout(async () => {
                    try {
                        console.log(`[${config.sessionId}] Attempting restart after init failure...`);
                        const result = await sessionManager.safeRestartSession(config.sessionId, 'init_failed');
                        if (!result.success) {
                            console.error(`[${config.sessionId}] Restart failed: ${result.error}`);
                        }
                    } catch (restartErr) {
                        console.error(`[${config.sessionId}] Restart error:`, restartErr?.message);
                    }
                }, 10000);  // 10 秒后重试
            }
        }

        console.log('[BOOT] All sessions initialized');

        // 触发历史同步
        if (String(process.env.SYNC_ON_STARTUP || '1') === '1') {
            setTimeout(async () => {
                try {
                    const pending = sessionManager.getAllPendingHistory();
                    if (pending.length > 0) {
                        console.log(`[BOOT] Triggering history sync for ${pending.length} sessions...`);

                        await axios.post(`${COLLECTOR_BASE}/batch-sync-history`, {
                            sessions: pending
                        }, {
                            headers: { 'x-api-token': COLLECTOR_TOKEN },
                            timeout: 120000
                        }).catch(e => {
                            console.warn('[BOOT] History sync request failed:', e.message);
                        });
                    }
                } catch (e) {
                    console.error('[BOOT] History sync error:', e.message);
                }
            }, 10000);
        }

        // 启动心跳监控
        sessionManager.startHeartbeat();

        // ★★★ V3.2 新增：初始化联系人同步定时任务 ★★★
        if (contactSync) {
            contactSync.initialize(sessionManager);
        }

    } catch (e) {
        console.error('[BOOT] Fatal error:', e.message);
        process.exit(1);
    }
})();

// 让浏览器能直接访问已保存的媒体文件
const app = express();
app.use('/files', express.static(MEDIA_DIR));
app.use(express.json());
app.use((req, res, next) => {
    const need = process.env.BRIDGE_TOKEN || process.env.MANAGER_TOKEN || '';
    if (!need) return next();

    const got = req.headers['x-api-token'] || req.query.token || '';
    if (got === need) return next();

    const mask = (s) => s ? `${String(s).slice(0,2)}***${String(s).slice(-2)}` : '';
    console.warn('[manager AUTH 401]', {
        path: req.path, given: mask(got), expect: mask(need),
        from: req.ip || req.connection?.remoteAddress
    });
    return res.status(401).json({ ok:false, error:'bad token' });
});

// Helper: get chat's contact name or ID and phone
function formatChat(chat) {
    return {
        id: chat.id._serialized,
        name: chat.name || chat.formattedTitle || chat.id.user || '',
        phone: chat.id.user,
    };
}

// sessions 端点
app.get('/sessions', (req, res) => {
    res.json(sessionManager.getStatusSummary());
});

// 新增：获取单个会话详情
app.get('/sessions/:id', (req, res) => {
    const { id } = req.params;
    const session = sessionManager.getSession(id);
    if (!session) {
        return res.status(404).json({ ok: false, error: 'Session not found' });
    }
    res.json({
        ok: true,
        session: {
            sessionId: session.sessionId,
            sessionName: session.sessionName,
            status: session.sessionStatus,
            chatsCached: (session.chats || []).length,
            lastHeartbeat: session.lastHeartbeat,
            profileInfo: session.profileInfo,
            // ★ 新增：恢复相关信息
            restartCount: session.restartCount || 0,
            maxRetries: sessionManager.maxRestartRetries,
            lastRestartTime: session.lastRestartTime || null,
            lastError: session.lastError || null
        }
    });
});

// ========== ★★★ V3.2 新增：联系人同步 API ★★★ ==========

/**
 * POST /contact-sync/trigger
 * 手动触发联系人同步
 *
 * 请求体:
 * {
 *   sessionId: string  // 可选，指定单个 session，不传则同步所有
 * }
 */
app.post('/contact-sync/trigger', async (req, res) => {
    if (!contactSync) {
        return res.status(501).json({ ok: false, error: 'ContactSync module not loaded' });
    }

    try {
        const { sessionId } = req.body || {};

        console.log(`[API] Contact sync triggered, sessionId=${sessionId || 'all'}`);

        // 异步执行，立即返回
        const promise = contactSync.triggerSync(sessionId);

        // 不等待完成，直接返回
        res.json({
            ok: true,
            message: sessionId
                ? `Sync started for session ${sessionId}`
                : 'Sync started for all sessions'
        });

        // 后台执行并记录结果
        promise.then(results => {
            console.log('[API] Contact sync completed');
        }).catch(e => {
            console.error('[API] Contact sync failed:', e.message);
        });

    } catch (e) {
        console.error('[API] Contact sync trigger error:', e.message);
        res.status(500).json({ ok: false, error: e.message });
    }
});

/**
 * GET /contact-sync/status
 * 获取同步配置状态
 */
app.get('/contact-sync/status', (req, res) => {
    if (!contactSync) {
        return res.json({
            ok: true,
            enabled: false,
            reason: 'module_not_loaded'
        });
    }

    res.json({
        ok: true,
        enabled: contactSync.SYNC_ENABLED,
        schedule: `${contactSync.SYNC_HOUR}:${String(contactSync.SYNC_MINUTE).padStart(2, '0')} Beijing time`,
        filter: "last 24 hours"
    });
});

// 新增：手动触发心跳检查
app.get('/heartbeat', async (req, res) => {
    try {
        const results = await sessionManager.performHeartbeatCheck();
        res.json({ ok: true, results });
    } catch (e) {
        res.status(500).json({ ok: false, error: e.message });
    }
});

// ★ 新增：手动重启会话
app.post('/sessions/:id/restart', async (req, res) => {
    try {
        const { id } = req.params;
        const session = sessionManager.getSession(id);
        if (!session) {
            return res.status(404).json({ ok: false, error: 'Session not found' });
        }

        console.log(`[API] Manual restart requested for session ${id}`);
        const result = await sessionManager.restartSession(id);

        if (result.success) {
            res.json({
                ok: true,
                message: `Session ${id} restart initiated`,
                restartCount: session.restartCount
            });
        } else {
            res.status(400).json({
                ok: false,
                error: result.error,
                restartCount: session.restartCount,
                maxRetries: sessionManager.maxRestartRetries
            });
        }
    } catch (e) {
        res.status(500).json({ ok: false, error: e.message });
    }
});

// ★ 新增：重置会话状态（从 error 状态恢复）
app.post('/sessions/:id/reset', async (req, res) => {
    try {
        const { id } = req.params;
        const session = sessionManager.getSession(id);
        if (!session) {
            return res.status(404).json({ ok: false, error: 'Session not found' });
        }

        const previousStatus = session.sessionStatus;
        const success = sessionManager.resetSession(id);

        if (success) {
            res.json({
                ok: true,
                message: `Session ${id} reset successfully`,
                previousStatus,
                newStatus: 'disconnected',
                restartCount: 0
            });
        } else {
            res.status(400).json({ ok: false, error: 'Failed to reset session' });
        }
    } catch (e) {
        res.status(500).json({ ok: false, error: e.message });
    }
});

// ★ 新增：获取健康检查配置
app.get('/health-config', (req, res) => {
    res.json({
        ok: true,
        config: {
            healthCheckEnabled: sessionManager.healthCheckEnabled,
            healthCheckIntervalMs: sessionManager.healthCheckInterval,
            autoRestartEnabled: sessionManager.autoRestartEnabled,
            maxRestartRetries: sessionManager.maxRestartRetries,
            restartCooldownMs: sessionManager.restartCooldown
        }
    });
});

// 新增：获取会话的联系人列表
app.get('/contacts/:sessionId', async (req, res) => {
    try {
        const { sessionId } = req.params;
        const count = parseInt(req.query.count) || 20;

        const session = sessionManager.getSession(sessionId);
        if (!session?.client) {
            return res.status(400).json({ ok: false, error: 'Session not found or not ready' });
        }

        const chats = await session.client.getChats();
        const privateChats = chats
            .filter(c => !c.isGroup)
            .slice(0, count)
            .map(c => ({
                id: c.id?._serialized,
                name: c.name || c.pushname || '',
                phone: c.id?.user || '',
                unreadCount: c.unreadCount || 0,
                timestamp: c.timestamp || null
            }));

        res.json({ ok: true, contacts: privateChats });
    } catch (e) {
        res.status(500).json({ ok: false, error: e.message });
    }
});


// Endpoint: list chats for a session
app.get('/chats/:sessionId', async (req, res) => {
    const { sessionId } = req.params;
    const count = parseInt(req.query.count) || 15;
    const session = sessions[sessionId];
    if (!session) {
        return res.status(400).json({ error: 'Session not found.' });
    }
    // ★★★ V5修复：改用 client 检查 ★★★
    if (!session.client) {
        return res.status(400).json({ error: 'Session not ready (no client).' });
    }
    try {
        const chats = session.chats.filter(c => !c.isGroup);
        const score = c => (c.timestamp || c.lastMessage?.timestamp || 0);
        const sortedChats = chats.sort((a, b) => score(b) - score(a));
        const limited = sortedChats.slice(0, count).map(formatChat);
        res.json(limited);
    } catch (err) {
        console.error(`[${sessionId}] get chats error:`, err.message);
        res.status(500).json({ error: 'Error getting chats.' });
    }
});

// 未读列表（实时）
app.get('/unread/:sessionId', async (req, res) => {
    try {
        const { sessionId } = req.params;
        const count = Math.max(1, parseInt(req.query.count || '15', 10));

        const sess = sessions[sessionId];
        const client = sess?.client;
        if (!client) return res.status(400).json({ ok:false, error:'session not found' });

        const chats = await listChatsLiveOrCache(sessionId);
        const recent = chats.slice(0, count);

        const unread = recent
            .filter(c => !c.isGroup)
            .filter(c => (c.unreadCount || 0) > 0)
            .map(c => ({
                id: c.id?._serialized,
                phone: c.id?.user || '',
                name: c.name || c.pushname || c.formattedTitle || '',
                unreadCount: c.unreadCount || 0,
                isGroup: !!c.isGroup,
                timestamp: c.timestamp || c.lastMessage?.timestamp || null
            }));

        return res.json({ ok:true, result: unread, count: unread.length });
    } catch (e) {
        return res.status(500).json({ ok:false, error: e?.message || String(e) });
    }
});

// === 拉取会话消息 ===
app.get('/messages/:sessionId/:chatId', async (req, res) => {
    const { sessionId, chatId } = req.params;
    const limit = parseInt(req.query.limit) || 50;
    const includeMedia = String(req.query.includeMedia || '0') === '1';

    const session = sessions[sessionId];
    if (!session) return res.status(400).json({ error: 'Session not found.' });
    // ★★★ V5修复：改用 client 检查 ★★★
    if (!session.client) return res.status(400).json({ error: 'Session not ready (no client).' });

    try {
        const chat = await session.client.getChatById(chatId);
        const messages = await chat.fetchMessages({ limit });

        const out = [];
        for (const msg of messages) {
            const idSer = msg.id?._serialized || msg.id;
            const caption = (msg.body && msg.body.trim().length > 0)
                ? msg.body
                : ((msg._data && msg._data.caption) ? String(msg._data.caption) : '');

            const item = {
                id: idSer,
                fromMe: !!msg.fromMe,
                type: msg.type,
                body: caption || (msg.type === 'chat' ? msg.body : ''),
                timestamp: msg.timestamp || Date.now(),
                quotedMsg: null  // ★ V5新增
            };

            // ★★★ V5.2增强：获取引用消息（主方法 + 备用方法）★★★
            const hasQuote = msg.hasQuotedMsg ||
                !!(msg._data?.quotedStanzaID) ||
                !!(msg._data?.quotedMsg);

            if (hasQuote) {
                try {
                    let quotedMsg = null;

                    // 方法1：通过 getQuotedMessage API
                    if (msg.hasQuotedMsg) {
                        quotedMsg = await msg.getQuotedMessage();
                    }

                    // 方法2：如果API失败，尝试从 _data 构建引用信息
                    if (!quotedMsg && msg._data?.quotedMsg) {
                        const qd = msg._data.quotedMsg;
                        quotedMsg = {
                            id: { _serialized: qd.id || msg._data.quotedStanzaID || '' },
                            body: qd.body || qd.caption || '',
                            type: qd.type || 'chat',
                            fromMe: !!qd.fromMe,
                            timestamp: qd.t || 0
                        };
                    }

                    if (quotedMsg) {
                        item.quotedMsg = {
                            id: quotedMsg.id?._serialized || quotedMsg.id?.id || '',
                            body: quotedMsg.body || '',
                            type: quotedMsg.type || 'chat',
                            fromMe: !!quotedMsg.fromMe,
                            timestamp: quotedMsg.timestamp || 0
                        };
                    }
                } catch (qe) {
                    item.quotedMsg = { error: qe.message };
                }
            }

            if (includeMedia) {
                const MEDIA_TYPES = new Set(['image', 'video', 'audio', 'ptt', 'sticker', 'document']);
                if (msg.hasMedia || MEDIA_TYPES.has(msg.type)) {
                    try {
                        const media = await msg.downloadMedia();
                        if (media && media.data) {
                            const saved = saveBase64ToFile(sessionId, chatId, idSer, media);
                            item.media = {
                                mimetype: saved.mimetype,
                                fileUrl: saved.fileUrl,
                                bytes: saved.bytes
                            };
                        }
                    } catch(e) {
                        item.media = { error: e?.message || String(e) };
                    }
                }
            }

            out.push(item);
        }

        res.json(out);
    } catch (err) {
        console.error(`[${sessionId}] get messages error:`, err.message);
        res.status(500).json({ error: 'Error getting messages.' });
    }
});


// Helper: wait for typing simulation
function wait(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// ====== [routes] 拟人文本发送 ======
app.post('/send-text/:sessionId', async (req,res)=>{
    try{
        const { sessionId } = req.params;
        const { chatIdOrPhone, text, wpm } = req.body || {};
        if (!chatIdOrPhone || !text) throw new Error('need chatIdOrPhone & text');

        const chat = await getChat(sessionId, chatIdOrPhone);

        await takeSendToken();
        await ensureChatCooldown(sessionId, chat.id._serialized);

        const msg = await humanTypeThenSendText(chat, text, { wpm });
        res.json({ ok:true, id: msg.id?._serialized || null });
    }catch(e){
        res.status(400).json({ ok:false, error: e.message || String(e) });
    }
});

// ★★★ 新增：引用回复发送 API ★★★
/**
 * POST /send/reply/:sessionId
 * 发送带引用的回复消息
 *
 * 请求体:
 * {
 *   chatIdOrPhone: string,      // 聊天ID或电话号码
 *   text: string,               // 消息内容
 *   quotedMessageId: string     // 被引用的消息ID (WA message ID)
 * }
 */
app.post('/send/reply/:sessionId', async (req, res) => {
    const { sessionId } = req.params;
    // ★★★ V5.3.9新增：接收quotedMessageSuffix ★★★
    const { chatIdOrPhone, text, quotedMessageId, quotedMessageSuffix } = req.body;

    try {
        // ★★★ V5.3.12修复：使用正确的session获取方式 ★★★
        const session = sessions[sessionId];
        if (!session || session.status !== 'ready') {
            return res.status(503).json({ ok: false, error: 'session not ready' });
        }
        const client = session.client;
        if (!client) {
            return res.status(503).json({ ok: false, error: 'client not available' });
        }

        // 获取chat对象
        const chat = await client.getChatById(chatIdOrPhone);
        if (!chat) {
            return res.status(404).json({ ok: false, error: 'chat not found' });
        }

        let finalQuotedId = quotedMessageId;

        // ★★★ V5.3.9新增：如果有后缀，尝试在当前聊天中查找正确的消息ID ★★★
        // ★★★ V5.3.13修复：优先选择Phone格式（@c.us），因为手机端更容易识别 ★★★
        if (quotedMessageSuffix) {
            try {
                // 获取最近消息（100条通常足够找到被引用的消息）
                const messages = await chat.fetchMessages({ limit: 100 });

                // 用后缀匹配 - 可能有多个匹配（LID和Phone格式）
                const allMatched = messages.filter(m => {
                    if (!m.id?._serialized) return false;
                    const msgSuffix = m.id._serialized.split('_').pop();
                    return msgSuffix === quotedMessageSuffix;
                });

                if (allMatched.length > 0) {
                    // ★★★ V5.3.13优化：优先选择Phone格式 ★★★
                    const phoneMatch = allMatched.find(m => m.id._serialized.includes('@c.us'));
                    const matched = phoneMatch || allMatched[0];

                    finalQuotedId = matched.id._serialized;
                    console.log(`[send/reply] Found message by suffix: ${quotedMessageSuffix} -> ${finalQuotedId.substring(0, 40)} (phone_preferred=${!!phoneMatch})`);
                } else {
                    console.log(`[send/reply] No message found with suffix: ${quotedMessageSuffix}, using original ID`);
                }
            } catch (e) {
                console.warn(`[send/reply] Suffix search failed: ${e.message}, using original ID`);
            }
        }

        // ★★★ V5.3.13说明：关于手机端引用不显示的问题 ★★★
        // 经过分析，同一条消息在不同设备上的LID是不同的：
        // - Dylan设备: true_17932042137754@lid_xxx
        // - Chloe设备: false_27153387237439@lid_xxx
        // LID本身不同（17932042137754 vs 27153387237439），无法通过格式转换解决。
        // 这是WhatsApp协议层面的限制，只能依赖后缀匹配。
        // 网页端能正常显示是因为whatsapp-web.js可能内部做了处理。

        // 发送引用消息
        console.log(`[send/reply] Sending with quotedMessageId: ${finalQuotedId?.substring(0, 40)}`);
        const msg = await chat.sendMessage(text, { quotedMessageId: finalQuotedId });

        return res.json({
            ok: true,
            msgId: msg.id._serialized,
            timestamp: msg.timestamp
        });

    } catch (e) {
        console.error(`[send/reply] Error: ${e.message}`);
        return res.status(500).json({ ok: false, error: e.message });
    }
});

// ★★★ V5.3.13新增：查找消息ID API ★★★
/**
 * POST /lookup-message/:sessionId
 * 在指定session的聊天中查找消息ID
 *
 * 用途：发送引用消息前，先在接收方设备上查找正确的消息ID格式
 *
 * 请求体:
 * {
 *   chatIdOrPhone: string,      // 聊天对象的ID或电话号码
 *   messageSuffix: string       // 消息ID后缀
 * }
 *
 * 返回:
 * {
 *   ok: true,
 *   messageId: string,          // 找到的完整消息ID
 *   found: boolean
 * }
 */
app.post('/lookup-message/:sessionId', async (req, res) => {
    const { sessionId } = req.params;
    const { chatIdOrPhone, messageSuffix } = req.body;

    try {
        if (!chatIdOrPhone || !messageSuffix) {
            return res.status(400).json({ ok: false, error: 'need chatIdOrPhone & messageSuffix' });
        }

        const session = sessions[sessionId];
        if (!session || session.status !== 'ready') {
            return res.status(503).json({ ok: false, error: 'session not ready', found: false });
        }

        const client = session.client;
        if (!client) {
            return res.status(503).json({ ok: false, error: 'client not available', found: false });
        }

        // 获取chat对象
        let chat;
        try {
            chat = await client.getChatById(chatIdOrPhone);
        } catch (e) {
            console.log(`[lookup-message] Chat not found: ${chatIdOrPhone}, trying with @c.us`);
            // 尝试添加@c.us后缀
            if (!chatIdOrPhone.includes('@')) {
                chat = await client.getChatById(`${chatIdOrPhone}@c.us`);
            }
        }

        if (!chat) {
            return res.json({ ok: true, found: false, error: 'chat not found' });
        }

        // 获取最近消息
        const messages = await chat.fetchMessages({ limit: 100 });

        // 用后缀匹配
        const matched = messages.find(m => {
            if (!m.id?._serialized) return false;
            const msgSuffix = m.id._serialized.split('_').pop();
            return msgSuffix === messageSuffix;
        });

        if (matched) {
            console.log(`[lookup-message] Found: suffix=${messageSuffix} -> ${matched.id._serialized.substring(0, 45)}`);
            return res.json({
                ok: true,
                found: true,
                messageId: matched.id._serialized
            });
        } else {
            console.log(`[lookup-message] Not found: suffix=${messageSuffix} in chat ${chatIdOrPhone}`);
            return res.json({
                ok: true,
                found: false
            });
        }

    } catch (e) {
        console.error(`[lookup-message] Error: ${e.message}`);
        return res.status(500).json({ ok: false, error: e.message, found: false });
    }
});

// ★★★ V5.3.13新增：通过电话号码查找session ★★★
/**
 * GET /resolve-session/:phone
 * 查找哪个session对应指定的电话号码
 */
app.get('/resolve-session/:phone', (req, res) => {
    const { phone } = req.params;
    const cleanPhone = phone.replace(/\D/g, '');

    for (const [sid, sess] of Object.entries(sessions)) {
        if (!sess || sess.status !== 'ready') continue;

        const widUser = sess.client?.info?.wid?.user || sess.info?.wid?.user;
        if (widUser === cleanPhone) {
            console.log(`[resolve-session] Found: ${cleanPhone} -> ${sid}`);
            return res.json({ ok: true, found: true, sessionId: sid });
        }
    }

    console.log(`[resolve-session] Not found: ${cleanPhone}`);
    return res.json({ ok: true, found: false });
});

// ★★★ V5.3.13新增：获取session信息 ★★★
/**
 * GET /session/:sessionId
 * 获取指定session的信息（包括电话号码）
 */
app.get('/session/:sessionId', (req, res) => {
    const { sessionId } = req.params;
    const session = sessions[sessionId];

    if (!session) {
        return res.status(404).json({ ok: false, error: 'session not found' });
    }

    const info = {
        status: session.status,
        name: session.name,
        wid: session.client?.info?.wid || session.info?.wid || null
    };

    console.log(`[session] Info for ${sessionId}: wid=${info.wid?.user || 'none'}`);
    return res.json({ ok: true, info });
});

// ★★★ 新增：删除消息 API ★★★
/**
 * POST /delete-message/:sessionId
 * 删除 WhatsApp 消息
 *
 * 请求体:
 * {
 *   chatIdOrPhone: string,    // 聊天ID或电话号码（可选，用于辅助查找）
 *   messageId: string,        // 要删除的消息ID (WA message ID)
 *   everyone: boolean         // true=所有人可见的撤回（默认）, false=仅自己
 * }
 */
app.post('/delete-message/:sessionId', async (req, res) => {
    try {
        const { sessionId } = req.params;
        const { chatIdOrPhone, messageId, everyone = true } = req.body || {};

        if (!messageId) {
            return res.status(400).json({ ok: false, error: 'need messageId' });
        }

        const session = sessions[sessionId];
        if (!session || !session.client) {
            return res.status(400).json({ ok: false, error: 'session not ready' });
        }

        console.log(`[${sessionId}] Deleting message: ${messageId?.substring(0, 40)}..., everyone: ${everyone}`);

        // 方法1: 通过 Store 直接获取消息
        let message = null;

        try {
            const msgData = await session.client.pupPage.evaluate(async (msgId) => {
                const msg = window.Store.Msg.get(msgId);
                if (msg) {
                    return window.WWebJS.getMessageModel(msg);
                }
                return null;
            }, messageId);

            if (msgData) {
                // 需要使用 whatsapp-web.js 的 Message 类包装
                const { Message } = require('whatsapp-web.js');
                message = new Message(session.client, msgData);
            }
        } catch (e) {
            console.log(`[${sessionId}] Could not get message from Store: ${e.message}`);
        }

        // 方法2: 如果提供了 chatIdOrPhone，遍历聊天消息查找
        if (!message && chatIdOrPhone) {
            try {
                const chat = await getChat(sessionId, chatIdOrPhone);
                const messages = await chat.fetchMessages({ limit: 50 });
                message = messages.find(m =>
                    m.id?._serialized === messageId ||
                    m.id?.id === messageId
                );
            } catch (e) {
                console.log(`[${sessionId}] Could not find message in chat: ${e.message}`);
            }
        }

        if (!message) {
            return res.status(404).json({ ok: false, error: 'message not found' });
        }

        // 检查是否可以删除（只能撤回自己发送的消息给所有人）
        if (!message.fromMe && everyone) {
            return res.status(400).json({
                ok: false,
                error: 'can only delete own messages for everyone'
            });
        }

        // 执行删除
        await message.delete(everyone);

        console.log(`[${sessionId}] Message deleted: ${messageId?.substring(0, 40)}...`);

        res.json({
            ok: true,
            messageId: messageId,
            everyone: everyone,
            deleted: true
        });

    } catch (e) {
        console.error(`[${req.params.sessionId}] delete-message error:`, e.message);
        res.status(400).json({ ok: false, error: e.message || String(e) });
    }
});

// ★★★ 测试接口：获取聊天消息列表 ★★★
/**
 * GET /test/messages/:sessionId/:chatId
 * 获取指定聊天的最近消息（包含引用消息信息）
 *
 * 参数:
 *   sessionId: session ID
 *   chatId: 聊天ID (如 85270379015@c.us 或 27153387237439@lid)
 *
 * Query:
 *   limit: 消息数量，默认20
 */
app.get('/test/messages/:sessionId/:chatId', async (req, res) => {
    try {
        const { sessionId, chatId } = req.params;
        const limit = parseInt(req.query.limit) || 20;

        const session = sessions[sessionId];
        if (!session || !session.client) {
            return res.status(400).json({ ok: false, error: 'session not ready' });
        }

        console.log(`[${sessionId}] Fetching messages: chatId=${chatId}, limit=${limit}`);

        // 获取聊天对象
        let chat;
        try {
            chat = await session.client.getChatById(chatId);
        } catch (e) {
            // 尝试通过电话号码获取
            const phone = chatId.replace(/@.*/, '').replace(/\D/g, '');
            if (phone) {
                try {
                    chat = await session.client.getChatById(`${phone}@c.us`);
                } catch (e2) {
                    try {
                        chat = await session.client.getChatById(`${phone}@lid`);
                    } catch (e3) {
                        // 都失败了
                    }
                }
            }
        }

        if (!chat) {
            return res.status(404).json({ ok: false, error: 'chat not found' });
        }

        // 获取消息
        const messages = await chat.fetchMessages({ limit });

        // 格式化消息（包含引用信息）
        const formattedMessages = [];

        for (const msg of messages) {
            const formatted = {
                id: msg.id?._serialized || msg.id?.id || '',
                body: msg.body || '',
                type: msg.type || 'chat',
                fromMe: !!msg.fromMe,
                timestamp: msg.timestamp || 0,
                timestampDate: msg.timestamp ? new Date(msg.timestamp * 1000).toISOString() : null,
                hasQuotedMsg: !!(msg.hasQuotedMsg || msg._data?.quotedStanzaID || msg._data?.quotedMsg),
                quotedMsg: null
            };

            // ★★★ V5.2增强：如果有引用消息，获取详情（主方法 + 备用方法）★★★
            const hasQuote = msg.hasQuotedMsg ||
                !!(msg._data?.quotedStanzaID) ||
                !!(msg._data?.quotedMsg);

            if (hasQuote) {
                try {
                    let quoted = null;

                    // 方法1：通过 getQuotedMessage API
                    if (msg.hasQuotedMsg) {
                        quoted = await msg.getQuotedMessage();
                    }

                    // 方法2：如果API失败，尝试从 _data 构建引用信息
                    if (!quoted && msg._data?.quotedMsg) {
                        const qd = msg._data.quotedMsg;
                        quoted = {
                            id: { _serialized: qd.id || msg._data.quotedStanzaID || '' },
                            body: qd.body || qd.caption || '',
                            type: qd.type || 'chat',
                            fromMe: !!qd.fromMe,
                            timestamp: qd.t || 0
                        };
                    }

                    if (quoted) {
                        formatted.quotedMsg = {
                            id: quoted.id?._serialized || quoted.id?.id || '',
                            body: quoted.body || '',
                            type: quoted.type || 'chat',
                            fromMe: !!quoted.fromMe,
                            timestamp: quoted.timestamp || 0
                        };
                    }
                } catch (e) {
                    formatted.quotedMsg = { error: e.message };
                }
            }

            formattedMessages.push(formatted);
        }

        res.json({
            ok: true,
            sessionId,
            chatId: chat.id._serialized,
            chatName: chat.name || null,
            messageCount: formattedMessages.length,
            messages: formattedMessages
        });

    } catch (e) {
        console.error(`[${req.params.sessionId}] test/messages error:`, e.message);
        res.status(400).json({ ok: false, error: e.message || String(e) });
    }
});

// ★★★ 测试接口：通过电话号码获取聊天ID ★★★
app.get('/test/chat-id/:sessionId/:phone', async (req, res) => {
    try {
        const { sessionId, phone } = req.params;

        const session = sessions[sessionId];
        if (!session || !session.client) {
            return res.status(400).json({ ok: false, error: 'session not ready' });
        }

        const digits = phone.replace(/\D/g, '');

        // 尝试不同格式
        const attempts = [`${digits}@c.us`, `${digits}@lid`];

        for (const chatId of attempts) {
            try {
                const chat = await session.client.getChatById(chatId);
                if (chat) {
                    return res.json({
                        ok: true,
                        phone: digits,
                        chatId: chat.id._serialized,
                        chatName: chat.name || null,
                        isGroup: chat.isGroup || false
                    });
                }
            } catch (e) {
                // 继续尝试下一个
            }
        }

        res.status(404).json({ ok: false, error: 'chat not found', tried: attempts });

    } catch (e) {
        res.status(400).json({ ok: false, error: e.message });
    }
});

// ★★★ 测试接口：列出最近聊天 ★★★
app.get('/test/chats/:sessionId', async (req, res) => {
    try {
        const { sessionId } = req.params;
        const limit = parseInt(req.query.limit) || 20;

        const session = sessions[sessionId];
        if (!session || !session.client) {
            return res.status(400).json({ ok: false, error: 'session not ready' });
        }

        const allChats = session.chats || await session.client.getChats();

        // 按最后消息时间排序
        const sorted = allChats
            .filter(c => !c.isGroup && !c.id._serialized.includes('status@broadcast'))
            .sort((a, b) => {
                const ta = a.lastMessage?.timestamp || a.timestamp || 0;
                const tb = b.lastMessage?.timestamp || b.timestamp || 0;
                return tb - ta;
            })
            .slice(0, limit);

        const formatted = sorted.map(chat => ({
            chatId: chat.id._serialized,
            name: chat.name || null,
            phone: chat.id._serialized.replace(/@.*/, ''),
            lastMessage: chat.lastMessage ? {
                body: chat.lastMessage.body?.substring(0, 50) || '',
                timestamp: chat.lastMessage.timestamp,
                fromMe: chat.lastMessage.fromMe
            } : null,
            unreadCount: chat.unreadCount || 0
        }));

        res.json({
            ok: true,
            sessionId,
            count: formatted.length,
            chats: formatted
        });

    } catch (e) {
        res.status(400).json({ ok: false, error: e.message });
    }
});

// === 发送媒体 ===
app.post('/send-media/:sessionId', async (req,res)=>{
    try{
        const { sessionId } = req.params;
        let {
            chatIdOrPhone,
            mediaType,
            filePath, url, b64, mimetype, filename,
            caption,
            asVoice,
            sendAsDocument,
            forceOwnDownload
        } = req.body || {};

        if (!chatIdOrPhone) throw new Error('need chatIdOrPhone');
        if (!mediaType)      throw new Error('need mediaType');

        const chat  = await getChat(sessionId, chatIdOrPhone);
        const media = await loadMedia({ filePath, url, b64, mimetype, filename });

        await takeSendToken();
        await ensureChatCooldown(sessionId, chat.id._serialized);
        const plan = [ rnd(800, 2000), rnd(600, 1400) ];
        for (const seg of plan){ await chat.sendStateTyping(); await sleep(seg); }
        await chat.clearState();

        const opts = {};
        if (caption) opts.caption = String(caption);
        if (mediaType === 'audio' && asVoice) opts.sendAudioAsVoice = true;

        if (sendAsDocument === true && ALLOW_DOC) {
            opts.sendMediaAsDocument = true;
        }

        const msg = await chat.sendMessage(media, opts);

        const FORCE = (forceOwnDownload !== false);
        if (FORCE) {
            setTimeout(async ()=>{ try { await msg.downloadMedia(); } catch(_){ } }, 600);
        }

        res.json({ ok:true, id: msg.id?._serialized || null });
    }catch(e){
        res.status(400).json({ ok:false, error: e.message || String(e) });
    }
});


// Endpoint: send message
app.post('/send', async (req, res) => {
    const { sessionId, to, message } = req.body;
    if (!sessionId || !to || !message) {
        return res.status(400).json({ error: 'Missing sessionId, to, or message' });
    }
    const session = sessions[sessionId];
    // ★★★ V5修复：改用 client 检查 ★★★
    if (!session || !session.client) {
        return res.status(400).json({ error: 'Session not ready.' });
    }
    try {
        const normalized = to.includes('@') ? to : `${to}@c.us`;
        const chat = await session.client.getChatById(normalized).catch(() => null);

        const toChat = chat || await session.client.getNumberId(to).then(r => r._serialized).catch(() => null);


        if (!toChat) {
            return res.status(400).json({ error: 'Invalid chat or number.' });
        }
        const chatObj = chat || await session.client.getChatById(toChat);
        await chatObj.sendStateTyping();
        const delay = 500 + Math.random() * 1000;
        await wait(delay);
        await session.client.sendMessage(toChat, message);
        await chatObj.clearState();
        res.json({ ok: true });
    } catch (err) {
        console.error(`[${sessionId}] send message error:`, err.message);
        res.status(500).json({ error: 'Error sending message.' });
    }
});

// ========== LID 转电话号码 ==========
/**
 * POST /resolve-lid
 * 将 LID (@lid) 转换为真实电话号码 (@c.us)
 *
 * 请求体:
 * {
 *   sessionId: string,
 *   lids: string[]  // LID 数组，如 ["123456@lid", "789012@lid"] 或纯数字 ["123456", "789012"]
 * }
 *
 * 响应:
 * {
 *   ok: true,
 *   results: {
 *     "123456": { lid: "123456@lid", phone: "8613800001234", phone_jid: "8613800001234@c.us" },
 *     "789012": { lid: "789012@lid", phone: null }  // 未找到
 *   }
 * }
 */
app.post('/resolve-lid', async (req, res) => {
    try {
        const { sessionId, lids } = req.body || {};

        if (!sessionId) {
            return res.status(400).json({ ok: false, error: 'missing sessionId' });
        }

        const session = sessions[sessionId];
        if (!session || !session.client) {
            return res.status(400).json({ ok: false, error: 'session not ready' });
        }

        if (!Array.isArray(lids) || lids.length === 0) {
            return res.status(400).json({ ok: false, error: 'lids must be a non-empty array' });
        }

        // 限制批量大小
        const maxBatch = 50;
        const limitedLids = lids.slice(0, maxBatch);

        // 标准化 LID 格式
        const normalizedLids = limitedLids.map(lid => {
            const digits = String(lid || '').replace(/\D/g, '');
            return digits ? `${digits}@lid` : null;
        }).filter(Boolean);

        if (normalizedLids.length === 0) {
            return res.status(400).json({ ok: false, error: 'no valid lids provided' });
        }

        console.log(`[resolve-lid] Resolving ${normalizedLids.length} LIDs for session ${sessionId}`);

        const results = {};

        // 检查 client 是否有 getContactLidAndPhone 方法
        if (typeof session.client.getContactLidAndPhone === 'function') {
            try {
                // 批量查询
                const response = await session.client.getContactLidAndPhone(normalizedLids);
                const arr = Array.isArray(response) ? response : (response ? [response] : []);

                for (const item of arr) {
                    // 解析返回的数据
                    // 格式: { lid: '123456@lid', pn: '8613800001234@c.us' }
                    const lidRaw = item?.lid || item?.userId || item?.id || item?.lidUser || '';
                    const pnRaw = item?.pn || item?.phone || item?.number || '';

                    const lidDigits = String(lidRaw).replace(/\D/g, '');
                    const phoneDigits = String(pnRaw).replace(/\D/g, '');

                    if (lidDigits) {
                        results[lidDigits] = {
                            lid: `${lidDigits}@lid`,
                            phone: phoneDigits || null,
                            phone_jid: phoneDigits ? `${phoneDigits}@c.us` : null
                        };
                    }
                }

                console.log(`[resolve-lid] Got ${Object.keys(results).filter(k => results[k].phone).length} phone numbers`);
            } catch (e) {
                console.error(`[resolve-lid] getContactLidAndPhone error:`, e?.message);
            }
        } else {
            console.warn(`[resolve-lid] getContactLidAndPhone not available on client`);
        }

        // 对于没有结果的 LID，添加空结果
        for (const lid of normalizedLids) {
            const digits = lid.replace(/\D/g, '');
            if (!results[digits]) {
                results[digits] = {
                    lid: `${digits}@lid`,
                    phone: null,
                    phone_jid: null
                };
            }
        }

        return res.json({ ok: true, results });
    } catch (e) {
        console.error(`[resolve-lid] Error:`, e?.message);
        return res.status(500).json({ ok: false, error: e?.message });
    }
});

/**
 * GET /resolve-lid/:sessionId/:lid
 * 单个 LID 转电话号码（简化 API）
 */
app.get('/resolve-lid/:sessionId/:lid', async (req, res) => {
    try {
        const { sessionId, lid } = req.params;

        const session = sessions[sessionId];
        if (!session || !session.client) {
            return res.status(400).json({ ok: false, error: 'session not ready' });
        }

        const digits = String(lid || '').replace(/\D/g, '');
        if (!digits) {
            return res.status(400).json({ ok: false, error: 'invalid lid' });
        }

        const normalizedLid = `${digits}@lid`;
        console.log(`[resolve-lid] Single resolve: ${normalizedLid}`);

        let phone = null;
        let phone_jid = null;

        if (typeof session.client.getContactLidAndPhone === 'function') {
            try {
                const response = await session.client.getContactLidAndPhone([normalizedLid]);
                const arr = Array.isArray(response) ? response : (response ? [response] : []);

                for (const item of arr) {
                    const pnRaw = item?.pn || item?.phone || item?.number || '';
                    const phoneDigits = String(pnRaw).replace(/\D/g, '');
                    if (phoneDigits) {
                        phone = phoneDigits;
                        phone_jid = `${phoneDigits}@c.us`;
                        break;
                    }
                }
            } catch (e) {
                console.error(`[resolve-lid] Error:`, e?.message);
            }
        }

        return res.json({
            ok: true,
            lid: normalizedLid,
            phone,
            phone_jid
        });
    } catch (e) {
        return res.status(500).json({ ok: false, error: e?.message });
    }
});

// ========== 获取消息媒体 ==========
/**
 * POST /fetch-media
 * 尝试获取指定消息的媒体内容
 * 用于当 message_create 事件中媒体下载失败时的补充获取
 */
app.post('/fetch-media', async (req, res) => {
    try {
        const { sessionId, messageId } = req.body || {};

        if (!sessionId || !messageId) {
            return res.status(400).json({ ok: false, error: 'missing sessionId or messageId' });
        }

        const session = sessions[sessionId];
        if (!session || !session.client) {
            return res.status(400).json({ ok: false, error: 'session not ready' });
        }

        console.log(`[fetch-media] Fetching media for message: ${messageId.substring(0, 30)}...`);

        // 尝试从 Store 获取消息
        let message = null;
        try {
            const page = session.client.pupPage;
            if (page) {
                message = await page.evaluate(async (msgId) => {
                    try {
                        const msg = window.Store?.Msg?.get(msgId);
                        if (!msg) return null;

                        return {
                            id: msg.id?._serialized,
                            type: msg.type,
                            hasMedia: !!msg.mediaData,
                            body: msg.body
                        };
                    } catch (e) {
                        return null;
                    }
                }, messageId);
            }
        } catch (e) {
            console.log(`[fetch-media] Store lookup failed: ${e?.message}`);
        }

        // 尝试下载媒体（多种方法）
        let media = null;

        // 方法1：直接通过 client 获取消息并下载媒体
        try {
            // 从 messageId 提取 chatId
            const chatIdMatch = messageId.match(/(?:true|false)_(.+?)_/);
            if (chatIdMatch) {
                const chatId = chatIdMatch[1];
                const chat = await session.client.getChatById(chatId).catch(() => null);

                if (chat) {
                    // 获取最近消息
                    const messages = await chat.fetchMessages({ limit: 50 });
                    const targetMsg = messages.find(m => m.id?._serialized === messageId);

                    if (targetMsg && targetMsg.hasMedia) {
                        console.log(`[fetch-media] Found message in chat, downloading media...`);

                        const mediaPromise = targetMsg.downloadMedia();
                        const timeoutPromise = new Promise((_, reject) =>
                            setTimeout(() => reject(new Error('timeout')), 20000)
                        );
                        media = await Promise.race([mediaPromise, timeoutPromise]);

                        if (media?.data) {
                            console.log(`[fetch-media] Media downloaded: ${media.mimetype}`);
                        }
                    }
                }
            }
        } catch (e) {
            console.log(`[fetch-media] Chat fetch method failed: ${e?.message}`);
        }

        // 方法2：DOM 提取（如果上面失败）
        if (!media?.data) {
            try {
                const page = session.client.pupPage;
                if (page) {
                    console.log(`[fetch-media] Trying DOM extraction...`);

                    const extracted = await page.evaluate(async (msgId) => {
                        const msgEl = document.querySelector(`[data-id="${msgId}"]`);
                        if (!msgEl) return { error: 'msg_not_found' };

                        const img = msgEl.querySelector('img[src^="blob:"]')
                            || msgEl.querySelector('img[draggable="false"]')
                            || msgEl.querySelector('img:not([src^="data:image/svg"])');

                        if (img?.src?.startsWith('blob:')) {
                            try {
                                const response = await fetch(img.src);
                                const blob = await response.blob();
                                return new Promise((resolve) => {
                                    const reader = new FileReader();
                                    reader.onloadend = () => {
                                        const base64 = reader.result;
                                        const matches = base64.match(/^data:(.+);base64,(.+)$/);
                                        if (matches) {
                                            resolve({ mimetype: matches[1], data: matches[2] });
                                        } else {
                                            resolve({ error: 'invalid_base64' });
                                        }
                                    };
                                    reader.readAsDataURL(blob);
                                });
                            } catch (e) {
                                return { error: 'blob_fetch_failed' };
                            }
                        }
                        return { error: 'no_image_src' };
                    }, messageId);

                    if (extracted?.data) {
                        media = extracted;
                        console.log(`[fetch-media] DOM extraction successful`);
                    }
                }
            } catch (e) {
                console.log(`[fetch-media] DOM extraction failed: ${e?.message}`);
            }
        }

        if (media && media.mimetype && media.data) {
            const ext = media.mimetype?.split('/')[1]?.split(';')[0] || 'bin';
            return res.json({
                ok: true,
                media: {
                    mimetype: media.mimetype,
                    data: media.data,
                    filename: media.filename || `media_${Date.now()}.${ext}`
                }
            });
        }

        return res.json({ ok: false, error: 'media not available' });
    } catch (e) {
        console.error(`[fetch-media] Error: ${e?.message}`);
        return res.status(500).json({ ok: false, error: e?.message });
    }
});

// ========== 发文本 ==========
app.post('/send/text', async (req, res) => {
    const ok  = (code, data) => res.status(code).json(data);
    const bad = (code, msg)  => ok(code, { ok: false, error: msg });

    try {
        const { sessionId, to, to_lid, text } = req.body || {};
        if (!sessionId || !text) return bad(400, 'missing sessionId/text');
        if (!to && !to_lid) return bad(400, 'missing to or to_lid');

        // ★★★ V3 增强：检查 session 是否真正可用 ★★★
        const readyCheck = isSessionReady(sessionId);
        if (!readyCheck.ready) {
            console.warn(`[send/text] Session ${sessionId} not ready: ${readyCheck.reason}`);
            return bad(503, `session not ready: ${readyCheck.reason}`);
        }

        const session = sessions[sessionId];
        if (!session || !session.client) return bad(400, 'session not ready');

        // ★★★ 新增：解析电话号码和 LID，支持回退机制 ★★★
        let phoneJid = null;  // 电话号码对应的 JID
        let lidJid = null;    // LID 对应的 JID

        // 解析电话号码
        if (to) {
            const { kind, digits, jid } = parseRecipient(to);
            if (kind === 'group') {
                phoneJid = jid;
            } else if (kind === 'user') {
                const r = await session.client.getNumberId(digits).catch(() => null);
                phoneJid = r?._serialized || null;
                if (!phoneJid && digits.length >= 7) {
                    phoneJid = `${digits}@c.us`;
                }
            }
        }

        // 解析 LID
        if (to_lid) {
            lidJid = `${to_lid.replace(/\D/g, '')}@lid`;
        }

        // 确定发送优先级
        const hasPhone = !!phoneJid;
        const hasLid = !!lidJid;

        if (!hasPhone && !hasLid) {
            return bad(400, 'recipient not found');
        }

        const escapeWhatsAppMarkdown = (str) => {
            if (!str) return str;
            return str.replace(/([*_~`])/g, '\u200B$1');
        };

        const safeText = escapeWhatsAppMarkdown(String(text));

        // ★★★ 发送逻辑：优先电话，失败后回退到 LID ★★★
        const MAX_RETRIES = 2;
        let lastError = null;

        // 第一优先：使用电话号码发送
        if (hasPhone) {
            console.log(`[send/text] Trying phone first: ${phoneJid}`);

            for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
                try {
                    const msg = await session.client.sendMessage(phoneJid, safeText);
                    const msgId = msg?.id?._serialized || null;

                    markSentFromChatwoot(msgId);
                    console.log(`[send/text] SUCCESS via phone: ${phoneJid}`);

                    return ok(200, { ok: true, to: phoneJid, msgId, method: 'phone' });
                } catch (sendErr) {
                    lastError = sendErr;
                    const errMsg = sendErr?.message || '';

                    // 检查是否是 getChat undefined 错误
                    if (errMsg.includes('getChat') || errMsg.includes('undefined')) {
                        console.warn(`[send/text] Phone attempt ${attempt}/${MAX_RETRIES} failed: ${errMsg}`);

                        if (attempt < MAX_RETRIES) {
                            await new Promise(r => setTimeout(r, 1000));
                            const state = await session.client.getState().catch(() => null);
                            if (state !== 'CONNECTED') break;
                        }
                    } else {
                        // 其他错误，如果有 LID 可以回退，否则抛出
                        console.warn(`[send/text] Phone send error: ${errMsg}`);
                        break;
                    }
                }
            }

            // 电话发送失败，如果有 LID 则回退
            if (hasLid) {
                console.log(`[send/text] Phone failed, falling back to LID: ${lidJid}`);
            }
        }

        // 第二优先/回退：使用 LID 发送
        if (hasLid) {
            console.log(`[send/text] Using LID: ${lidJid}`);

            for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
                try {
                    const msg = await session.client.sendMessage(lidJid, safeText);
                    const msgId = msg?.id?._serialized || null;

                    markSentFromChatwoot(msgId);
                    console.log(`[send/text] SUCCESS via LID: ${lidJid}`);

                    return ok(200, { ok: true, to: lidJid, msgId, method: 'lid' });
                } catch (sendErr) {
                    lastError = sendErr;
                    const errMsg = sendErr?.message || '';

                    if (errMsg.includes('getChat') || errMsg.includes('undefined')) {
                        console.warn(`[send/text] LID attempt ${attempt}/${MAX_RETRIES} failed: ${errMsg}`);

                        if (attempt < MAX_RETRIES) {
                            await new Promise(r => setTimeout(r, 1000));
                            const state = await session.client.getState().catch(() => null);
                            if (state !== 'CONNECTED') break;
                        }
                    } else {
                        throw sendErr;
                    }
                }
            }
        }

        // 所有方式都失败
        throw lastError || new Error('send failed after all attempts');
    } catch (e) {
        console.error(`[send/text] Final error: ${e?.message}`);
        return res.status(500).json({ ok: false, error: e?.message });
    }
});




/**
 * GET /messages-sync/:sessionId/:chatId
 * 获取指定时间范围内的消息（用于同步）
 *
 * Query:
 * - limit: 消息数量限制，默认 100
 * - after: 开始时间戳（秒），默认最近 12 小时
 * - before: 结束时间戳（秒），默认当前时间
 * - includeMedia: 是否包含媒体，1=是（返回 fileUrl）
 * - includeBase64: 是否包含 base64 数据，1=是（返回 data_url，用于同步）
 * - timezone: 时区偏移（小时），默认 0
 */
app.get('/messages-sync/:sessionId/:chatId', async (req, res) => {
    const { sessionId, chatId } = req.params;
    const limit = parseInt(req.query.limit) || 100;
    const includeMedia = String(req.query.includeMedia || '0') === '1';
    const includeBase64 = String(req.query.includeBase64 || '0') === '1';
    const timezoneOffset = parseInt(req.query.timezone || '0') * 3600;

    let afterTs = req.query.after ? parseInt(req.query.after) : null;
    let beforeTs = req.query.before ? parseInt(req.query.before) : null;

    if (afterTs && afterTs > 9999999999) afterTs = Math.floor(afterTs / 1000);
    if (beforeTs && beforeTs > 9999999999) beforeTs = Math.floor(beforeTs / 1000);

    const now = Math.floor(Date.now() / 1000);
    if (!afterTs) afterTs = now - 12 * 3600;
    if (!beforeTs) beforeTs = now;

    const session = sessions[sessionId];
    if (!session) return res.status(400).json({ ok: false, error: 'Session not found' });
    // ★★★ V5修复：改用 client 检查，与测试接口保持一致 ★★★
    if (!session.client) return res.status(400).json({ ok: false, error: 'Session not ready (no client)' });

    try {
        console.log(`[messages-sync] session=${sessionId}, chat=${chatId}`);
        console.log(`[messages-sync] time: ${new Date(afterTs * 1000).toISOString()} ~ ${new Date(beforeTs * 1000).toISOString()}`);

        // ★★★ V5修复：先检查 WhatsApp Store 是否可用 ★★★
        let storeReady = false;
        try {
            storeReady = await session.client.pupPage.evaluate(() => {
                return !!(window.Store && window.Store.Chat);
            });
        } catch (e) {
            console.log(`[messages-sync] Store check failed: ${e?.message}`);
        }

        if (!storeReady) {
            console.log(`[messages-sync] WhatsApp Store not ready, returning error (not empty)`);
            // ★★★ 重要：返回错误而不是空数组，避免 Collector 删除现有消息 ★★★
            return res.status(503).json({
                ok: false,
                error: 'WhatsApp Store not ready',
                messages: null,  // null 而不是 []，让 Collector 知道这是错误
                retryable: true
            });
        }

        // ★★★ V5修复：增强聊天查找逻辑 ★★★
        let chat = null;

        // 提取电话号码用于多格式匹配
        const phoneDigits = chatId.replace(/@.*/, '').replace(/\D/g, '');

        // 方法1: 直接获取
        try {
            chat = await session.client.getChatById(chatId);
            console.log(`[messages-sync] Got chat via getChatById: ${chatId}`);
        } catch (e) {
            console.log(`[messages-sync] getChatById failed: ${e?.message}`);
        }

        // 方法2: 尝试 @c.us 格式
        if (!chat && phoneDigits && !chatId.endsWith('@c.us')) {
            try {
                chat = await session.client.getChatById(`${phoneDigits}@c.us`);
                console.log(`[messages-sync] Got chat via @c.us format`);
            } catch (e) {
                // 忽略
            }
        }

        // 方法3: 尝试 @lid 格式
        if (!chat && phoneDigits && !chatId.endsWith('@lid')) {
            try {
                chat = await session.client.getChatById(`${phoneDigits}@lid`);
                console.log(`[messages-sync] Got chat via @lid format`);
            } catch (e) {
                // 忽略
            }
        }

        // 方法4: 从缓存的 chats 中查找
        if (!chat && session.chats && Array.isArray(session.chats)) {
            // 尝试精确匹配
            chat = session.chats.find(c => c?.id?._serialized === chatId);

            // 尝试电话号码匹配
            if (!chat && phoneDigits) {
                chat = session.chats.find(c => {
                    const cPhone = c?.id?._serialized?.replace(/@.*/, '').replace(/\D/g, '');
                    return cPhone === phoneDigits;
                });
            }

            if (chat) {
                console.log(`[messages-sync] Using cached chat: ${chat.id?._serialized}`);
            }
        }

        // 方法5: 刷新聊天列表后重试
        if (!chat) {
            console.log(`[messages-sync] Refreshing chats and retrying...`);
            try {
                const freshChats = await session.client.getChats();
                session.chats = freshChats;

                chat = freshChats.find(c => {
                    const cId = c?.id?._serialized || '';
                    const cPhone = cId.replace(/@.*/, '').replace(/\D/g, '');
                    return cId === chatId || cPhone === phoneDigits;
                });

                if (chat) {
                    console.log(`[messages-sync] Found chat after refresh: ${chat.id?._serialized}`);
                }
            } catch (e) {
                console.log(`[messages-sync] getChats failed: ${e?.message}`);
            }
        }

        // 如果仍然找不到，返回空结果
        if (!chat) {
            console.log(`[messages-sync] Chat not found after all attempts: ${chatId}`);
            return res.json({ ok: true, messages: [], note: 'Chat not accessible' });
        }

        const fetchLimit = Math.min(limit * 5, 500);
        const rawMessages = await chat.fetchMessages({ limit: fetchLimit });

        console.log(`[messages-sync] Fetched ${rawMessages.length} raw messages from WhatsApp`);

        const out = [];
        let mediaCount = 0;
        let mediaErrorCount = 0;

        for (const msg of rawMessages) {
            const msgTs = msg.timestamp || 0;

            if (msgTs < afterTs || msgTs > beforeTs) continue;
            if (out.length >= limit) break;

            const idSer = msg.id?._serialized || msg.id;

            let body = '';
            if (msg.type === 'chat') {
                body = msg.body || '';
            } else if (msg._data?.caption) {
                body = String(msg._data.caption);
            } else if (msg.body) {
                body = msg.body;
            }

            const item = {
                id: idSer,
                fromMe: !!msg.fromMe,
                type: msg.type,
                body: body,
                timestamp: msgTs,
                timestampMs: msgTs * 1000,
                datetime: new Date((msgTs + timezoneOffset) * 1000).toISOString(),
                ack: msg.ack,
                quotedMsg: null  // ★ V5新增：引用消息字段
            };

            // ★★★ V5.2增强：获取引用消息（主方法 + 备用方法）★★★
            const hasQuote = msg.hasQuotedMsg ||
                !!(msg._data?.quotedStanzaID) ||
                !!(msg._data?.quotedMsg);

            if (hasQuote) {
                try {
                    let quotedMsg = null;

                    // 方法1：通过 getQuotedMessage API
                    if (msg.hasQuotedMsg) {
                        quotedMsg = await msg.getQuotedMessage();
                    }

                    // 方法2：如果API失败，尝试从 _data 构建引用信息
                    if (!quotedMsg && msg._data?.quotedMsg) {
                        const qd = msg._data.quotedMsg;
                        quotedMsg = {
                            id: { _serialized: qd.id || msg._data.quotedStanzaID || '' },
                            body: qd.body || qd.caption || '',
                            type: qd.type || 'chat',
                            fromMe: !!qd.fromMe,
                            timestamp: qd.t || 0
                        };
                    }

                    if (quotedMsg) {
                        item.quotedMsg = {
                            id: quotedMsg.id?._serialized || quotedMsg.id?.id || '',
                            body: quotedMsg.body || '',
                            type: quotedMsg.type || 'chat',
                            fromMe: !!quotedMsg.fromMe,
                            timestamp: quotedMsg.timestamp || 0
                        };
                    }
                } catch (qe) {
                    item.quotedMsg = { error: qe.message };
                }
            }

            if (includeMedia || includeBase64) {
                const MEDIA_TYPES = new Set(['image', 'video', 'audio', 'ptt', 'sticker', 'document']);
                if (msg.hasMedia || MEDIA_TYPES.has(msg.type)) {
                    try {
                        let media = null;

                        // ★★★ V4 改进：增加重试机制 ★★★
                        for (let attempt = 1; attempt <= 2 && !media?.data; attempt++) {
                            try {
                                const mediaPromise = msg.downloadMedia();
                                const timeoutPromise = new Promise((_, reject) =>
                                    setTimeout(() => reject(new Error('timeout')), 15000)
                                );
                                media = await Promise.race([mediaPromise, timeoutPromise]);
                            } catch (dlErr) {
                                if (attempt < 2) {
                                    await new Promise(r => setTimeout(r, 1000));
                                }
                            }
                        }

                        if (!media?.data) {
                            console.log(`[messages-sync] API failed, extracting from DOM for ${idSer}...`);

                            try {
                                const extracted = await session.client.pupPage.evaluate(async (msgId, msgType) => {
                                    let msgEl = document.querySelector(`[data-id="${msgId}"]`);

                                    if (!msgEl) {
                                        const container = document.querySelector('[data-testid="conversation-panel-messages"]')
                                            || document.querySelector('._ajyl');
                                        if (container) {
                                            container.scrollTop = 0;
                                            await new Promise(r => setTimeout(r, 500));
                                            msgEl = document.querySelector(`[data-id="${msgId}"]`);
                                        }
                                    }

                                    if (!msgEl) return { error: 'msg_not_found' };

                                    msgEl.scrollIntoView({ behavior: 'instant', block: 'center' });
                                    await new Promise(r => setTimeout(r, 300));

                                    const downloadBtn = msgEl.querySelector('[data-icon="media-download"]')
                                        || msgEl.querySelector('[data-icon="audio-download"]')
                                        || msgEl.querySelector('span[data-icon="media-download"]')?.closest('button, div[role="button"]');

                                    if (downloadBtn) {
                                        downloadBtn.click();
                                        await new Promise(r => setTimeout(r, 6000));
                                    }

                                    if (msgType === 'image' || msgType === 'sticker') {
                                        const img = msgEl.querySelector('img[src^="blob:"]')
                                            || msgEl.querySelector('img[src*="base64"]')
                                            || msgEl.querySelector('[data-testid="media-canvas"] img')
                                            || msgEl.querySelector('img[draggable="false"]');

                                        if (img && img.src) {
                                            if (img.src.startsWith('blob:')) {
                                                try {
                                                    const response = await fetch(img.src);
                                                    const blob = await response.blob();
                                                    return new Promise((resolve) => {
                                                        const reader = new FileReader();
                                                        reader.onloadend = () => {
                                                            const base64 = reader.result;
                                                            const matches = base64.match(/^data:(.+);base64,(.+)$/);
                                                            if (matches) {
                                                                resolve({
                                                                    mimetype: matches[1],
                                                                    data: matches[2]
                                                                });
                                                            } else {
                                                                resolve({ error: 'invalid_base64' });
                                                            }
                                                        };
                                                        reader.onerror = () => resolve({ error: 'reader_error' });
                                                        reader.readAsDataURL(blob);
                                                    });
                                                } catch (e) {
                                                    return { error: 'blob_fetch_failed: ' + e.message };
                                                }
                                            } else if (img.src.startsWith('data:')) {
                                                const matches = img.src.match(/^data:(.+);base64,(.+)$/);
                                                if (matches) {
                                                    return { mimetype: matches[1], data: matches[2] };
                                                }
                                            }
                                        }

                                        const canvas = msgEl.querySelector('canvas');
                                        if (canvas) {
                                            try {
                                                const dataUrl = canvas.toDataURL('image/jpeg', 0.95);
                                                const matches = dataUrl.match(/^data:(.+);base64,(.+)$/);
                                                if (matches) {
                                                    return { mimetype: matches[1], data: matches[2] };
                                                }
                                            } catch (e) {}
                                        }

                                        return { error: 'image_not_found' };
                                    }

                                    return { error: 'type_not_supported_for_dom_extract' };

                                }, msg.id._serialized, msg.type);

                                if (extracted?.data && extracted?.mimetype) {
                                    media = {
                                        mimetype: extracted.mimetype,
                                        data: extracted.data,
                                        filename: null
                                    };
                                    console.log(`[messages-sync] DOM extraction OK: ${extracted.mimetype}`);
                                } else {
                                    console.log(`[messages-sync] DOM extraction failed: ${extracted?.error || 'unknown'}`);

                                    await new Promise(r => setTimeout(r, 2000));
                                    media = await msg.downloadMedia().catch(() => null);
                                }
                            } catch (extractErr) {
                                console.log(`[messages-sync] Extract error: ${extractErr?.message}`);
                            }
                        }

                        if (media?.data) {
                            mediaCount++;

                            const ext = mimeToExt(media.mimetype) || '';
                            const filename = media.filename || `${idSer}${ext}`;

                            item.media = {
                                mimetype: media.mimetype,
                                filename: filename,
                                bytes: Buffer.from(media.data, 'base64').length,
                                data_url: `data:${media.mimetype};base64,${media.data}`
                            };

                            console.log(`[messages-sync] Media OK: ${idSer}, size=${item.media.bytes}`);
                        } else {
                            mediaErrorCount++;
                            console.log(`[messages-sync] Media unavailable for ${idSer}`);
                            item.media = { error: 'media_not_available' };
                        }
                    } catch (e) {
                        mediaErrorCount++;
                        console.log(`[messages-sync] Media error for ${idSer}:`, e?.message);
                        item.media = { error: e?.message };
                    }
                }
            }

            out.push(item);
        }

        out.sort((a, b) => a.timestamp - b.timestamp);

        console.log(`[messages-sync] Returning ${out.length} messages, media: ${mediaCount} ok, ${mediaErrorCount} failed`);

        res.json({
            ok: true,
            sessionId,
            chatId,
            count: out.length,
            mediaCount,
            mediaErrorCount,
            hasBase64: includeBase64,
            timeRange: {
                after: afterTs,
                before: beforeTs,
                afterDate: new Date(afterTs * 1000).toISOString(),
                beforeDate: new Date(beforeTs * 1000).toISOString()
            },
            messages: out
        });
    } catch (err) {
        console.error(`[messages-sync] Error:`, err.message);
        res.status(500).json({ ok: false, error: err.message });
    }
});

/**
 * POST /messages-batch/:sessionId
 * 批量获取多个聊天的消息
 */
app.post('/messages-batch/:sessionId', async (req, res) => {
    const { sessionId } = req.params;
    const { chatIds, after, before, limit = 50, includeMedia = false } = req.body || {};

    const session = sessions[sessionId];
    if (!session) return res.status(400).json({ ok: false, error: 'Session not found' });
    // ★★★ V5修复：改用 client 检查，与测试接口保持一致 ★★★
    if (!session.client) return res.status(400).json({ ok: false, error: 'Session not ready (no client)' });

    if (!Array.isArray(chatIds) || chatIds.length === 0) {
        return res.status(400).json({ ok: false, error: 'chatIds array required' });
    }

    let afterTs = after ? parseInt(after) : Math.floor(Date.now() / 1000) - 12 * 3600;
    let beforeTs = before ? parseInt(before) : Math.floor(Date.now() / 1000);
    if (afterTs > 9999999999) afterTs = Math.floor(afterTs / 1000);
    if (beforeTs > 9999999999) beforeTs = Math.floor(beforeTs / 1000);

    const results = {};

    for (const chatId of chatIds) {
        try {
            const chat = await session.client.getChatById(chatId);
            const rawMessages = await chat.fetchMessages({ limit: Math.min(limit * 3, 200) });

            const messages = [];
            for (const msg of rawMessages) {
                const msgTs = msg.timestamp || 0;
                if (msgTs < afterTs || msgTs > beforeTs) continue;
                if (messages.length >= limit) break;

                const item = {
                    id: msg.id?._serialized || msg.id,
                    fromMe: !!msg.fromMe,
                    type: msg.type,
                    body: msg.body || '',
                    timestamp: msgTs
                };

                if (includeMedia && msg.hasMedia) {
                    try {
                        const media = await msg.downloadMedia();
                        if (media?.data) {
                            const saved = saveBase64ToFile(sessionId, chatId, item.id, media);
                            item.media = { mimetype: saved.mimetype, fileUrl: saved.fileUrl };
                        }
                    } catch (_) {}
                }

                messages.push(item);
            }

            messages.sort((a, b) => a.timestamp - b.timestamp);
            results[chatId] = { ok: true, count: messages.length, messages };
        } catch (e) {
            results[chatId] = { ok: false, error: e?.message };
        }
    }

    res.json({ ok: true, results });
});

app.post('/send/media', async (req, res) => {
    const ok  = (code, data) => res.status(code).json(data);
    const bad = (code, msg)  => ok(code, { ok: false, error: msg });

    try {
        const { sessionId, to, to_lid, url, b64, mimetype, filename, caption, attachments, message_id } = req.body || {};

        console.log(`[send/media] START: to=${to || 'none'}, to_lid=${to_lid || 'none'}, msg_id=${message_id || 'none'}`);

        // 【去重检查】
        if (message_id) {
            const cached = sendMediaCache.get(message_id);
            if (cached) {
                if (cached.result) {
                    console.log(`[send/media] DEDUP: message_id=${message_id} already completed`);
                    return ok(200, cached.result);
                }
                if (cached.processing) {
                    console.log(`[send/media] DEDUP: message_id=${message_id} is processing, waiting...`);
                    for (let i = 0; i < 60; i++) {
                        await new Promise(r => setTimeout(r, 500));
                        const updated = sendMediaCache.get(message_id);
                        if (updated?.result) {
                            return ok(200, updated.result);
                        }
                    }
                    return ok(200, { ok: true, pending: true, message: 'Request is being processed' });
                }
            }
            sendMediaCache.set(message_id, { ts: Date.now(), processing: true, result: null });
        }

        if (!sessionId) return bad(400, 'missing sessionId');
        if (!to && !to_lid) return bad(400, 'missing to or to_lid');

        // ★★★ V3 增强：检查 session 是否真正可用 ★★★
        const readyCheck = isSessionReady(sessionId);
        if (!readyCheck.ready) {
            console.warn(`[send/media] Session ${sessionId} not ready: ${readyCheck.reason}`);
            // 清理缓存状态
            if (message_id) {
                sendMediaCache.delete(message_id);
            }
            return bad(503, `session not ready: ${readyCheck.reason}`);
        }

        const session = sessions[sessionId];
        if (!session || !session.client) return bad(400, 'session not ready');

        // ★★★ 新增：分别解析电话号码和 LID，支持回退机制 ★★★
        let phoneJid = null;  // 电话号码对应的 JID
        let lidJid = null;    // LID 对应的 JID

        // 解析电话号码
        if (to) {
            const { kind, digits, jid } = parseRecipient(to);
            if (kind === 'group') {
                phoneJid = jid;
            } else if (kind === 'user' && digits) {
                const r = await session.client.getNumberId(digits).catch(() => null);
                phoneJid = r?._serialized || (digits.length >= 7 ? `${digits}@c.us` : null);
            }
        }

        // 解析 LID
        if (to_lid) {
            lidJid = `${to_lid.replace(/\D/g, '')}@lid`;
        }

        const hasPhone = !!phoneJid;
        const hasLid = !!lidJid;

        if (!hasPhone && !hasLid) {
            return bad(400, 'recipient not found');
        }

        // 准备媒体项
        const mediaItems = [];
        if (url || b64) mediaItems.push({ url, b64, mimetype, filename, caption });
        if (Array.isArray(attachments)) {
            for (const att of attachments) {
                if (att) {
                    mediaItems.push({
                        url: att.url || att.file_url || att.data_url,
                        b64: att.b64,
                        mimetype: att.mimetype || att.file_type || att.mime,
                        filename: att.filename || att.file_name,
                        caption: att.caption
                    });
                }
            }
        }

        if (mediaItems.length === 0) return bad(400, 'missing media');

        // ★★★ 发送函数：尝试获取 chat 并发送媒体 ★★★
        const trySendMedia = async (targetJid) => {
            let chat = null;
            let useDirectSend = false;

            // 尝试获取 chat 对象
            for (let attempt = 1; attempt <= 2; attempt++) {
                try {
                    chat = await session.client.getChatById(targetJid);
                    console.log(`[send/media] Got chat for ${targetJid}: ${chat?.id?._serialized}`);
                    break;
                } catch (e) {
                    const errMsg = e?.message || '';
                    console.log(`[send/media] getChatById attempt ${attempt} for ${targetJid} failed: ${errMsg}`);

                    if (errMsg.includes('getChat') || errMsg.includes('undefined')) {
                        if (attempt < 2) {
                            await new Promise(r => setTimeout(r, 1000));
                            continue;
                        }
                        useDirectSend = true;
                    } else {
                        const cached = (session.chats || []).find(c => c?.id?._serialized === targetJid);
                        if (cached) {
                            chat = cached;
                            console.log(`[send/media] Using cached chat for ${targetJid}`);
                            break;
                        }
                    }
                }
            }

            if (!chat && !useDirectSend) {
                return { ok: false, error: `Cannot get chat for ${targetJid}` };
            }

            console.log(`[send/media] Sending to ${targetJid}, items: ${mediaItems.length}, direct=${useDirectSend}`);

            const results = [];

            for (let i = 0; i < mediaItems.length; i++) {
                const item = mediaItems[i];

                try {
                    const itemMime = normalizeMime(item.mimetype, item.filename);

                    let itemFilename = item.filename;
                    if (!itemFilename) {
                        if (item.url) {
                            try { itemFilename = path.basename(new URL(item.url).pathname); } catch (_) {}
                        }
                        if (!itemFilename) {
                            itemFilename = ensureFilenameByMime(`media_${Date.now()}_${i}`, itemMime);
                        }
                    }

                    console.log(`[send/media] Item ${i}: ${itemMime}, ${itemFilename}`);

                    let media;
                    if (item.b64) {
                        media = new MessageMedia(itemMime, item.b64, itemFilename);
                    } else if (item.url) {
                        media = await loadMedia({ url: item.url, mimetype: itemMime, filename: itemFilename });
                    } else {
                        continue;
                    }

                    const mime = normalizeMime(media.mimetype || itemMime, itemFilename);
                    const isVideo = mime.startsWith('video/');
                    const isAudio = mime.startsWith('audio/');
                    const isVoice = isAudio && (mime.includes('ogg') || mime.includes('opus'));

                    const itemCaption = (i === 0) ? (item.caption || caption || '') : '';

                    let msg = null;
                    let method = 'standard';

                    const opts = {};
                    if (itemCaption) opts.caption = itemCaption;
                    if (isVoice) opts.sendAudioAsVoice = true;

                    if (isVideo) {
                        if (useDirectSend) {
                            try {
                                msg = await session.client.sendMessage(targetJid, media, opts);
                                method = 'client.sendMessage';
                            } catch (e) {
                                console.error(`[send/media] Direct send video failed:`, e?.message);
                                results.push({ ok: false, error: e?.message, index: i });
                                continue;
                            }
                        } else {
                            const r = await sendVideo(chat, media, itemCaption);
                            if (r.ok) {
                                msg = r.msg;
                                method = r.method;
                            } else {
                                results.push({ ok: false, error: r.error, index: i });
                                continue;
                            }
                        }
                    } else {
                        try {
                            if (useDirectSend) {
                                msg = await session.client.sendMessage(targetJid, media, opts);
                                method = 'client.sendMessage';
                            } else {
                                msg = await chat.sendMessage(media, opts);
                                method = 'chat.sendMessage';
                            }
                        } catch (e) {
                            console.error(`[send/media] Item ${i} failed:`, e?.message);
                            results.push({ ok: false, error: e?.message, index: i });
                            continue;
                        }
                    }

                    if (msg) {
                        const msgId = msg.id?._serialized || null;
                        console.log(`[send/media] Item ${i} OK: method=${method}`);

                        if (typeof markSentFromChatwoot === 'function') {
                            markSentFromChatwoot(msgId);
                        }

                        setTimeout(async () => {
                            try { await msg.downloadMedia(); } catch (_) {}
                        }, 600);

                        results.push({ ok: true, id: msgId, index: i, method });
                    }

                    if (i < mediaItems.length - 1) await sleep(rnd(800, 1500));
                } catch (e) {
                    console.error(`[send/media] Item ${i} exception:`, e?.message);
                    results.push({ ok: false, error: e?.message, index: i });
                }
            }

            const successCount = results.filter(r => r.ok).length;
            return {
                ok: successCount > 0,
                to: chat?.id?._serialized || targetJid,
                results,
                total: mediaItems.length,
                success: successCount
            };
        };

        // ★★★ 发送逻辑：优先电话，失败后回退到 LID ★★★
        let finalResult = null;

        // 第一优先：使用电话号码发送
        if (hasPhone) {
            console.log(`[send/media] Trying phone first: ${phoneJid}`);
            finalResult = await trySendMedia(phoneJid);

            if (finalResult.ok) {
                console.log(`[send/media] SUCCESS via phone: ${phoneJid}`);
                finalResult.method = 'phone';
            } else if (hasLid) {
                // 电话发送失败，回退到 LID
                console.log(`[send/media] Phone failed, falling back to LID: ${lidJid}`);
                finalResult = await trySendMedia(lidJid);
                if (finalResult.ok) {
                    console.log(`[send/media] SUCCESS via LID fallback: ${lidJid}`);
                    finalResult.method = 'lid_fallback';
                }
            }
        } else if (hasLid) {
            // 只有 LID
            console.log(`[send/media] Using LID: ${lidJid}`);
            finalResult = await trySendMedia(lidJid);
            if (finalResult.ok) {
                finalResult.method = 'lid';
            }
        }

        console.log(`[send/media] DONE: ${finalResult.success}/${finalResult.total}`);

        if (message_id) {
            sendMediaCache.set(message_id, { ts: Date.now(), processing: false, result: finalResult });
            console.log(`[send/media] Cached result for message_id=${message_id}`);
        }

        return ok(finalResult.ok ? 200 : 500, finalResult);
    } catch (e) {
        console.error(`[send/media] Error:`, e?.message);
        return res.status(500).json({ ok: false, error: e?.message });
    }
});

app.post('/mark-read/:sessionId/:chatId', async (req, res) => {
    try {
        const { sessionId, chatId } = req.params;
        const sess = sessions[sessionId];
        if (!sess || sess.status !== 'ready') return res.status(400).json({ ok:false, error:'Session not ready' });

        const chat = await sess.client.getChatById(chatId);
        await chat.markSeen();
        sess.chats = await listChatsLiveOrCache(sessionId);
        res.json({ ok:true });
    } catch (e) {
        res.status(500).json({ ok:false, error: e?.message || String(e) });
    }
});

const PORT = process.env.BRIDGE_PORT;
app.listen(PORT, '0.0.0.0', () => {
    console.log(`[manager] up on 0.0.0.0:${PORT}`);
});

// 优雅关闭处理
// ============================================================
process.on('SIGTERM', async () => {
    console.log('[SHUTDOWN] SIGTERM received, cleaning up...');
    await sessionManager.shutdown();
    process.exit(0);
});

process.on('SIGINT', async () => {
    console.log('[SHUTDOWN] SIGINT received, cleaning up...');
    await sessionManager.shutdown();
    process.exit(0);
});