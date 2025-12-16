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

const SESSIONS = (process.env.SESSIONS || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
function sleep(ms){ return new Promise(r=>setTimeout(r, ms)); }
function rnd(a,b){ return Math.floor(Math.random()*(b-a+1))+a; }

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
const toJid  = s => /@g\.us$|@c\.us$/.test(s) ? s : (DIGITS(s) ? `${DIGITS(s)}@c.us` : String(s||'').trim());

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
// 估算“人类打字用时”计划（多段 typing）
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
    // 标点/换行稍作停顿（加一些“犹豫”）
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
            // 优先保留：传入的 keepPage；若没有，则保留“最近创建的那一个”
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
const AUTO_CLICK_USE_HERE = String(process.env.AUTO_CLICK_USE_HERE || '1') === '1'; // 1=自动点“Use here”




// 简单的参数检查
if (!SESSIONS.length) {
    console.error('[BOOT] No SESSIONS provided in .env.chatwoot');
    process.exit(1);
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


// Store clients and their statuses
const sessions = {};
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

// Initialize each WhatsApp session
// 使用 AdsPower 启动并“连接已登录浏览器”，避免扫码
async function initSessionViaAdsPower(sessionId){
    // 注意：这里假设 SESSIONS 即为 AdsPower 的 user_id；如果不是，请自行建立映射
    const ws = await startProfile(sessionId);
    // 新增：配置 web 版本缓存策略
    const WWEB_CACHE = String(process.env.WWEB_CACHE || 'local').toLowerCase();
    let webVersionCache;
    if (WWEB_CACHE === 'none') webVersionCache = { type: 'none' };
    else if (WWEB_CACHE === 'memory') webVersionCache = { type: 'memory' };
    else webVersionCache = { type: 'local' };

    // 可选：启动即清理 .wwebjs_cache
    if (String(process.env.WWEB_CACHE_CLEAN || '0') === '1') {
        try { fs.rmSync(path.join(process.cwd(), '.wwebjs_cache'), { recursive: true, force: true }); } catch (_) {}
    }

    const client = new Client({
        authStrategy: new NoAuth(),         // 复用 AdsPower 已登录会话，不扫码
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
        webVersionCache,                    // ★ 关键：按 .env.chatwoot 控制缓存策略（none=不落盘）
        takeoverOnConflict: true,
        takeoverTimeoutMs: 1000
    });

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
// 等待真正连接到 WA 后再拉取，避免 getChats 早调用报错
    async function waitConnected(client, sessionId, tries = 12) {
        for (let i = 0; i < tries; i++) {
            const st = await client.getState().catch(() => null);
            if (st === 'CONNECTED') return true;
            await new Promise(r => setTimeout(r, 700));
        }
        console.log(`[${sessionId}] waitConnected timeout, continue anyway`);
        return false;
    }

    async function preloadChatsSafe(client, sessionId) {
        for (let i = 1; i <= 3; i++) {
            try {
                const chats = await client.getChats();         // 只能用 webjs API
                sessions[sessionId].chats = chats;             // 写入缓存，后续接口直接用
                console.log(`[${sessionId}] Preloaded ${chats.length} chats.`);
                return;
            } catch (e) {
                console.log(`[${sessionId}] preload retry(${i}): ${e?.message || e}`);
                await new Promise(r => setTimeout(r, 1200));
            }
        }
        // 最后再尝试一次，仅日志
        try {
            const chats = await client.getChats();
            sessions[sessionId].chats = chats;
            console.log(`[${sessionId}] Preloaded ${chats.length} chats.`);
        } catch (e) {
            console.log(`[${sessionId}] preload failed: ${e?.message || e}`);
        }
    }

// 统一的页面守护：自动点“Use here”+ 关掉多余 WA 标签
    const pageGuard = async () => {
        try {
            const page = client.pupPage;
            if (page) {
                await autoTakeover(page, sessionId);
                if (!AUTO_CLICK_USE_HERE || !page) return;
                await closeExtraWATabs(page.browser(), page);
            }
        } catch (_) {}
    };

// loading_screen 阶段：可能会弹“Use here”，先兜底处理一次
    client.on('loading_screen', pageGuard);

// ready：只处理一次 → 设置状态、兜底处理、等待 CONNECTED、再安全预加载
    client.once('ready', async () => {
        sessions[sessionId].status = 'ready';
        console.log(`[${sessionId}] Client is ready (AdsPower profile).`);
        await pageGuard();
        try {
            if (client.pupPage) {
                client.pupPage.setDefaultTimeout(PROTOCOL_TIMEOUT_MS);
                client.pupPage.setDefaultNavigationTimeout(PROTOCOL_TIMEOUT_MS);
            }
        } catch (_) {}
        await waitConnected(client, sessionId);
        await preloadChatsSafe(client, sessionId);
        // === 追加：后台定时刷新缓存，避免其它接口滞后 ===
        if (sessions[sessionId].refreshTimer) {
            clearInterval(sessions[sessionId].refreshTimer);
            delete sessions[sessionId].refreshTimer;
        }
        sessions[sessionId].refreshTimer = setInterval(async () => {
            try {
                sessions[sessionId].chats = await listChatsLiveOrCache(sessionId);
            } catch (_) {}
        }, 4000);

    });

// 认证失败、断线重连
    client.on('auth_failure', msg => {
        sessions[sessionId].status = 'auth_failure';
        console.error(`[${sessionId}] Auth failure: ${msg}`);
    });

    client.on('disconnected', async (reason) => {
        sessions[sessionId].status = 'disconnected';
        console.log(`[${sessionId}] Client disconnected: ${reason}`);
        // 自动重连：重新拿 AdsPower 的 ws 再 init
        try { await initSessionViaAdsPower(sessionId); }
        catch(e){ console.error(`[${sessionId}] Reconnect failed:`, e?.message || e); }
        if (sessions[sessionId]?.refreshTimer) {
            clearInterval(sessions[sessionId].refreshTimer);
            delete sessions[sessionId].refreshTimer;
        }
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
                    }, { 'x-api-token': COLLECTOR_TOKEN });
                } catch (_) {}
            }
        } catch (_) {}
    });

    // 我方消息同步
    // client.on('message_create', async (message) => {
    //     try {
    //         if (!message.fromMe) return;
    //
    //         const msgId = message.id?._serialized || '';
    //         if (isSentFromChatwoot(msgId)) return;
    //
    //         const toJid = message.to || '';
    //         if (/@g\.us$/i.test(toJid) || /@broadcast/i.test(toJid)) return;
    //
    //         let phone = '', phone_lid = '';
    //         if (/@c\.us$/i.test(toJid)) {
    //             phone = toJid.replace(/@.*/, '').replace(/\D/g, '');
    //         } else if (/@lid$/i.test(toJid)) {
    //             phone_lid = toJid.replace(/@.*/, '').replace(/\D/g, '');
    //         }
    //
    //         const chat = await message.getChat().catch(() => null);
    //         const contact = await message.getContact().catch(() => null);
    //
    //         console.log(`[${sessionId}] OUTGOING: ${msgId.substring(0, 25)}..., type=${message.type}`);
    //
    //         const payload = {
    //             sessionId, messageId: msgId,
    //             phone, phone_lid,
    //             name: contact?.pushname || contact?.name || chat?.name || '',
    //             text: message.body || '',
    //             type: message.type || 'chat',
    //             timestamp: message.timestamp ? (message.timestamp * 1000) : Date.now(),
    //             to: toJid,
    //             chatId: chat?.id?._serialized || toJid,
    //             fromMe: true, direction: 'outgoing'
    //         };
    //
    //         if (message.hasMedia) {
    //             try {
    //                 const media = await message.downloadMedia();
    //                 if (media?.data) {
    //                     payload.attachment = {
    //                         data_url: `data:${media.mimetype};base64,${media.data}`,
    //                         mime: media.mimetype, name: media.filename
    //                     };
    //                 }
    //             } catch (_) {}
    //         }
    //
    //         await postWithRetry(`${COLLECTOR_BASE}/ingest-outgoing`, payload, { 'x-api-token': COLLECTOR_TOKEN }).catch(() => {});
    //     } catch (e) {
    //         console.error(`[${sessionId}] message_create error:`, e?.message);
    //     }
    // });

    client.on('message', async (message) => {
        try {
            const sess = sessions[sessionId] || (sessions[sessionId] = {});

            const chat = await message.getChat();

            // 初始化缓存数组
            const arr = Array.isArray(sess.chats) ? sess.chats : (sess.chats = []);

            // 定位并更新/插入该会话
            const idSer = chat?.id?._serialized;
            const idx   = arr.findIndex(c => c?.id?._serialized === idSer);
            if (idx === -1) {
                arr.push(chat);
            } else {
                arr[idx] = chat;
            }

            // 轻量补齐一些常用字段，便于接口直接读取
            // （不改变 webjs 内部对象结构，仅在缓存上冗余记录）
            const rec = arr[arr.length - 1]; // 刚 push 的或刚更新的
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
                    // 这些类型我们认为是“有媒体”的（wweb.js 的 message.hasMedia 也可以先判断）
                    const MEDIA_TYPES = new Set(['image', 'video', 'audio', 'ptt', 'sticker', 'document']);
                    if (message.hasMedia || MEDIA_TYPES.has(message.type)) {
                        const media = await message.downloadMedia().catch(()=>null);
                        if (media && media.data) {
                            const saved = saveBase64ToFile(sessionId, chatIdSer, (message.id?._serialized || message.id), media);
                            // 记一份到最近消息上，方便接口直接拿 URL
                            rec.__lastMessage.media = {
                                mimetype: saved.mimetype,
                                fileUrl: saved.fileUrl,
                                bytes: saved.bytes
                            };
                        }
                    }
                } catch(e) {
                    console.log(`[${sessionId}] media auto-save fail:`, e?.message || e);
                }
            }
            rec.__name = chat.name || chat.pushname || chat.formattedTitle || '';

            // ===== 区分真实电话(@c.us)和隐私号(@lid) =====
            const fromJidRaw = message.from || '';
            let phone = '';      // 真实电话
            let phone_lid = '';  // 隐私号

            if (/@c\.us$/i.test(fromJidRaw)) {
                // 是真实电话
                phone = fromJidRaw.replace(/@.*/, '').replace(/\D/g, '');
            } else if (/@lid$/i.test(fromJidRaw)) {
                // 是隐私号
                phone_lid = fromJidRaw.replace(/@.*/, '').replace(/\D/g, '');
                // 尝试从 chat.id 获取真实电话
                if (chat?.id?._serialized && /@c\.us$/i.test(chat.id._serialized)) {
                    phone = chat.id._serialized.replace(/@.*/, '').replace(/\D/g, '');
                }
            } else {
                // 未知格式，尝试从 chat.id 获取
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

            const jidSer  = chat?.id?._serialized || '';
            const fromJid = message.from || '';
            const isGroup = !!chat.isGroup || /@g\.us$/i.test(jidSer) || /@g\.us$/i.test(fromJid);
            const isStatus = fromJid === 'status@broadcast'
                || /@broadcast/i.test(fromJid)
                || /@broadcast/i.test(jidSer)
                || String((chat.id && chat.id.user) || '').toLowerCase() === 'status';

            if (isGroup || isStatus) {
                console.log(`[${sessionId}] skip group/status message: ${message.id?._serialized || message.id}`);
                return; // 直接丢弃，不转发到 collector
            }
            //数据保障直线通达方法
            try {
                const payload = {
                    sessionId: sessionId,
                    phone: rec.__phone || '',
                    phone_lid: rec.__phone_lid || '',
                    name: rec.__name || '',
                    text: message.body || '',
                    type: message.type || 'chat',
                    messageId: (message.id && (message.id._serialized || message.id.id)) || '',
                    timestamp: message.timestamp ? (message.timestamp*1000) : Date.now(),
                    from: message.from || '',
                    server: (chat.id && chat.id.server) || '',
                    chatId: chat?.id?._serialized || ''
                };

                // 媒体处理（API + DOM提取备选）
                if (message.hasMedia === true && typeof message.downloadMedia === 'function') {
                    try {
                        // ===== 第一次尝试 API 下载 =====
                        let media = null;
                        try {
                            const mediaPromise = message.downloadMedia();
                            const timeoutPromise = new Promise((_, reject) =>
                                setTimeout(() => reject(new Error('timeout')), 5000)
                            );
                            media = await Promise.race([mediaPromise, timeoutPromise]);
                        } catch (e1) {
                            // API 失败
                        }

                        // ===== 如果 API 失败，从 DOM 提取 =====
                        if (!media?.data && (message.type === 'image' || message.type === 'sticker')) {
                            console.log(`[${sessionId}] API failed, extracting from DOM...`);

                            try {
                                const client = sessions[sessionId]?.client;
                                if (client?.pupPage) {
                                    const extracted = await client.pupPage.evaluate(async (msgId) => {
                                        const msgEl = document.querySelector(`[data-id="${msgId}"]`);
                                        if (!msgEl) return { error: 'msg_not_found' };

                                        // 检查下载按钮
                                        const downloadBtn = msgEl.querySelector('[data-icon="media-download"]')
                                            || msgEl.querySelector('span[data-icon="media-download"]')?.closest('button, div[role="button"]');

                                        if (downloadBtn) {
                                            downloadBtn.click();
                                            await new Promise(r => setTimeout(r, 5000));
                                        }

                                        // 提取图片
                                        const img = msgEl.querySelector('img[src^="blob:"]')
                                            || msgEl.querySelector('img[draggable="false"]');

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
                                    }, message.id._serialized);

                                    if (extracted?.data) {
                                        media = extracted;
                                        console.log(`[${sessionId}] DOM extraction OK`);
                                    }
                                }
                            } catch (_) {}
                        }

                        if (media && media.mimetype && media.data) {
                            payload.attachment = {
                                data_url: `data:${media.mimetype};base64,${media.data}`,
                                mime: media.mimetype,
                                name: media.filename || undefined
                            };
                            console.log(`[${sessionId}] media OK: ${media.mimetype}, ${media.data.length} bytes`);
                        } else {
                            console.log(`[${sessionId}] media unavailable`);
                        }
                    } catch (mediaErr) {
                        console.log(`[${sessionId}] media error: ${mediaErr?.message}`);
                    }
                }

                // ★ 推送前日志
                console.log(`[${sessionId}] >>> PUSH: phone=${payload.phone}, text="${(payload.text || '').slice(0, 30)}..."`);

                await postWithRetry(`${COLLECTOR_BASE}/ingest`, payload, { 'x-api-token': COLLECTOR_TOKEN });

                // ★ 推送成功日志
                console.log(`[${sessionId}] <<< PUSH OK`);

            } catch (e) {
                console.error(`[${sessionId}] <<< PUSH FAIL: ${e?.response?.status || ''} ${e?.message || e}`);
            }

            //（已在 ready 里有周期性刷新，这里再加一次短延迟刷新，让“已读/新来”更快反映到缓存）
            sess.__debounced = sess.__debounced || {};
            clearTimeout(sess.__debounced.msgUpdate);
            sess.__debounced.msgUpdate = setTimeout(async () => {
                try {
                    sess.chats = await listChatsLiveOrCache(sessionId);
                } catch (_) {}
            }, 1000);

        } catch (err) {
            console.error(`[${sessionId}] Error updating chat cache:`, err.message);
        }
    });

// 实时跟踪会话属性变化（unreadCount/lastMessage/timestamp 等）
    client.on('chat_update', (chat) => {
        try {
            const sess = sessions[sessionId] || (sessions[sessionId] = {});
            const arr  = Array.isArray(sess.chats) ? sess.chats : (sess.chats = []);
            const idS  = chat?.id?._serialized;
            const i    = arr.findIndex(c => c?.id?._serialized === idS);
            if (i === -1) arr.push(chat);
            else arr[i] = chat;
        } catch (_) {}
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
        } catch (_) {}
    }, 500);
    // 初始化后再兜底点一次（如果页面此刻正好停在弹框）
    setTimeout(() => {
        const page = sessions[sessionId]?.client?.pupPage;
        if (page) autoTakeover(page, sessionId).catch(()=>{});
    }, 1200);

}

// 启动所有会话（AdsPower）
(async ()=>{
    for (const sessionId of SESSIONS){
        try{
            await initSessionViaAdsPower(sessionId);
        }catch(e){
            console.error(`[${sessionId}] init failed:`, e?.message || e);
        }
    }
})();
// 让浏览器能直接访问已保存的媒体文件
// 例如 http://127.0.0.1:5010/files/k165wa1x/4475.../msgid.jpg
const app = express();
app.use('/files', express.static(MEDIA_DIR));
app.use(express.json());
app.use((req, res, next) => {
    const need = process.env.BRIDGE_TOKEN || process.env.MANAGER_TOKEN || '';
    if (!need) return next(); // 未设置则不校验

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
        phone: chat.id.user, // for personal chats, chat.id.user is the phone number (without '+')
    };
}

// Endpoint: list sessions
app.get('/sessions', (req, res) => {
    const info = {};
    for (const sessionId of SESSIONS) {
        const session = sessions[sessionId] || {};
        info[sessionId] = {
            status: session.status || 'unknown',
            chatsCached: (session.chats || []).length,
            via: 'adspower-ws' // 告知是通过 AdsPower DevTools 连接
        };
    }
    res.json(info);
});


// Endpoint: list chats for a session
app.get('/chats/:sessionId', async (req, res) => {
    const { sessionId } = req.params;
    const count = parseInt(req.query.count) || 15;
    const session = sessions[sessionId];
    if (!session) {
        return res.status(400).json({ error: 'Session not found.' });
    }
    if (session.status !== 'ready') {
        return res.status(400).json({ error: 'Session not ready.' });
    }
    try {
        const chats = session.chats.filter(c => !c.isGroup); // filter out groups
        const score = c => (c.timestamp || c.lastMessage?.timestamp || 0);
        const sortedChats = chats.sort((a, b) => score(b) - score(a));
        const limited = sortedChats.slice(0, count).map(formatChat);
        res.json(limited);
    } catch (err) {
        console.error(`[${sessionId}] get chats error:`, err.message);
        res.status(500).json({ error: 'Error getting chats.' });
    }
});

// 未读列表（实时）——每次调用都从 webjs 现状拉最近 N 个
app.get('/unread/:sessionId', async (req, res) => {
    try {
        const { sessionId } = req.params;
        const count = Math.max(1, parseInt(req.query.count || '15', 10));

        const sess = sessions[sessionId];
        const client = sess?.client;
        if (!client) return res.status(400).json({ ok:false, error:'session not found' });

        // 关键：实时拉取，而不是看启动时缓存
        const chats = await listChatsLiveOrCache(sessionId);                // 最新状态
        // getChats() 默认按最近活跃排序（最新在前），我们只看前 N
        const recent = chats.slice(0, count);

        // 过滤：非群组 + 有未读
        const unread = recent
            .filter(c => !c.isGroup)
            .filter(c => (c.unreadCount || 0) > 0)
            .map(c => ({
                id: c.id?._serialized,
                phone: c.id?.user || '',        // 私聊：这里就是手机号（@c.us 前缀部分）
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

// === 拉取会话消息（支持 includeMedia=1 保存并返回 URL；caption 兜底） ===
app.get('/messages/:sessionId/:chatId', async (req, res) => {
    const { sessionId, chatId } = req.params;
    const limit = parseInt(req.query.limit) || 50;
    const includeMedia = String(req.query.includeMedia || '0') === '1';

    const session = sessions[sessionId];
    if (!session) return res.status(400).json({ error: 'Session not found.' });
    if (session.status !== 'ready') return res.status(400).json({ error: 'Session not ready.' });

    try {
        const chat = await session.client.getChatById(chatId);
        const messages = await chat.fetchMessages({ limit });

        const out = [];
        for (const msg of messages) {
            const idSer = msg.id?._serialized || msg.id;
            // “说明文字/字幕”的兜底：优先 body，其次 _data.caption（某些版本可用）
            const caption = (msg.body && msg.body.trim().length > 0)
                ? msg.body
                : ((msg._data && msg._data.caption) ? String(msg._data.caption) : '');

            const item = {
                id: idSer,
                fromMe: !!msg.fromMe,
                type: msg.type,            // 'chat' | 'image' | 'video' | 'audio' | 'ptt' | 'sticker' | 'document' ...
                body: caption || (msg.type === 'chat' ? msg.body : ''), // 纯文本才保留 body，否则给 caption 兜底
                timestamp: msg.timestamp || Date.now()
            };

            if (includeMedia) {
                // 附件保存（无 caption 也会下载）
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
// ====== [routes] 拟人文本发送 插入开始 ======
/**
 * POST /send-text/:sessionId
 * body: { chatIdOrPhone: "4477...@c.us" 或 "4477...", text: "hello", wpm?:140 }
 * 拟人输出：先维持 typing 再一次性发送最终文本（不分多条，降低风控）
 */
app.post('/send-text/:sessionId', async (req,res)=>{
    try{
        const { sessionId } = req.params;
        const { chatIdOrPhone, text, wpm } = req.body || {};
        if (!chatIdOrPhone || !text) throw new Error('need chatIdOrPhone & text');

        const chat = await getChat(sessionId, chatIdOrPhone);

        // 限流 + 同会话冷却
        await takeSendToken();
        await ensureChatCooldown(sessionId, chat.id._serialized);

        const msg = await humanTypeThenSendText(chat, text, { wpm });
        res.json({ ok:true, id: msg.id?._serialized || null });
    }catch(e){
        res.status(400).json({ ok:false, error: e.message || String(e) });
    }
});
// ====== [routes] 拟人文本发送 插入结束 ======
// === 替换这个路由：发送媒体（图片/视频/音频/文档），支持本地 filePath，拟人化 + 可选“文档方式” ===
// === 发送媒体：图片/视频/音频/文档。支持本地 filePath，默认发“黄框大图”，并触发清晰化 ===
app.post('/send-media/:sessionId', async (req,res)=>{
    try{
        const { sessionId } = req.params;
        let {
            chatIdOrPhone,
            mediaType,                 // "image" | "video" | "audio" | "document"
            filePath, url, b64, mimetype, filename,
            caption,
            asVoice,                   // audio -> 语音(PTT)
            sendAsDocument,            // 可选：true 则走文档卡片（不会“糊”，但无大图预览）
            forceOwnDownload           // 可选：true 发送后拉一遍，促使本端从“糊图”变清晰
        } = req.body || {};

        if (!chatIdOrPhone) throw new Error('need chatIdOrPhone');
        if (!mediaType)      throw new Error('need mediaType');

        // 若误把本地盘符路径放在 url 中，loadMedia 会自动矫正
        const chat  = await getChat(sessionId, chatIdOrPhone);
        const media = await loadMedia({ filePath, url, b64, mimetype, filename });

        // —— 拟人：限速 + 同会话冷却 + 打字中
        await takeSendToken();
        await ensureChatCooldown(sessionId, chat.id._serialized);
        const plan = [ rnd(800, 2000), rnd(600, 1400) ];
        for (const seg of plan){ await chat.sendStateTyping(); await sleep(seg); }
        await chat.clearState();

        // —— 参数：默认就是“图片方式”发送（黄框大图预览）
        const opts = {};
        if (caption) opts.caption = String(caption);
        if (mediaType === 'audio' && asVoice) opts.sendAudioAsVoice = true;

        // 只有显式要求时才走“文档方式”（红框那种）
        if (sendAsDocument === true && ALLOW_DOC) {
            opts.sendMediaAsDocument = true;
        }

        const msg = await chat.sendMessage(media, opts);

        // —— 关键：为了让发送端尽快从“糊图”变清晰，默认触发一次自下载
        // 若想全局默认可改成：const FORCE = true;
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
    if (!session || session.status !== 'ready') {
        return res.status(400).json({ error: 'Session not ready.' });
    }
    try {
        // Determine chat or contact
        const normalized = to.includes('@') ? to : `${to}@c.us`;
        const chat = await session.client.getChatById(normalized).catch(() => null);

        // Type simulation
        const toChat = chat || await session.client.getNumberId(to).then(r => r._serialized).catch(() => null);


        if (!toChat) {
            return res.status(400).json({ error: 'Invalid chat or number.' });
        }
        const chatObj = chat || await session.client.getChatById(toChat);
        await chatObj.sendStateTyping();
        // Simulate typing delay
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

// ========== 发文本 ==========
app.post('/send/text', async (req, res) => {
    const ok  = (code, data) => res.status(code).json(data);
    const bad = (code, msg)  => ok(code, { ok: false, error: msg });

    try {
        const { sessionId, to, to_lid, text } = req.body || {};
        if (!sessionId || !text) return bad(400, 'missing sessionId/text');
        if (!to && !to_lid) return bad(400, 'missing to or to_lid');

        const session = sessions[sessionId];
        if (!session || !session.client) return bad(400, 'session not ready');

        let targetJid = null;

        if (to) {
            const { kind, digits, jid } = parseRecipient(to);
            if (kind === 'group') {
                targetJid = jid;
            } else if (kind === 'user') {
                const r = await session.client.getNumberId(digits).catch(() => null);
                targetJid = r?._serialized || null;
                if (!targetJid && digits.length >= 7) {
                    targetJid = `${digits}@c.us`;
                }
            }
        }

        if (!targetJid && to_lid) {
            targetJid = `${to_lid.replace(/\D/g, '')}@lid`;
            console.log(`[send/text] Using LID: ${targetJid}`);
        }

        if (!targetJid) return bad(400, 'recipient not found');

        // ===== 新增：转义 WhatsApp Markdown 特殊字符 =====
        const escapeWhatsAppMarkdown = (str) => {
            if (!str) return str;
            // 在特殊字符前插入零宽空格，防止 WhatsApp 将其识别为格式标记
            // * 粗体, _ 斜体, ~ 删除线, ``` 代码块, ` 行内代码
            return str.replace(/([*_~`])/g, '\u200B$1');
        };

        const safeText = escapeWhatsAppMarkdown(String(text));

        // 发送消息
        const msg = await session.client.sendMessage(targetJid, safeText);
        const msgId = msg?.id?._serialized || null;

        // 标记为从 Chatwoot 发送
        markSentFromChatwoot(msgId);

        return ok(200, { ok: true, to: targetJid, msgId });
    } catch (e) {
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

    // 时间参数（支持秒和毫秒）
    let afterTs = req.query.after ? parseInt(req.query.after) : null;
    let beforeTs = req.query.before ? parseInt(req.query.before) : null;

    // 自动识别毫秒并转换为秒
    if (afterTs && afterTs > 9999999999) afterTs = Math.floor(afterTs / 1000);
    if (beforeTs && beforeTs > 9999999999) beforeTs = Math.floor(beforeTs / 1000);

    // 默认：最近 12 小时
    const now = Math.floor(Date.now() / 1000);
    if (!afterTs) afterTs = now - 12 * 3600;
    if (!beforeTs) beforeTs = now;

    const session = sessions[sessionId];
    if (!session) return res.status(400).json({ ok: false, error: 'Session not found' });
    if (session.status !== 'ready') return res.status(400).json({ ok: false, error: 'Session not ready' });

    try {
        console.log(`[messages-sync] session=${sessionId}, chat=${chatId}`);
        console.log(`[messages-sync] includeMedia=${includeMedia}, includeBase64=${includeBase64}`);
        console.log(`[messages-sync] time: ${new Date(afterTs * 1000).toISOString()} ~ ${new Date(beforeTs * 1000).toISOString()}`);

        const chat = await session.client.getChatById(chatId);

        // 获取更多消息用于过滤（因为 WhatsApp 返回的是最近的消息）
        const fetchLimit = Math.min(limit * 5, 500);
        const rawMessages = await chat.fetchMessages({ limit: fetchLimit });

        console.log(`[messages-sync] Fetched ${rawMessages.length} raw messages from WhatsApp`);

        const out = [];
        let mediaCount = 0;
        let mediaErrorCount = 0;

        for (const msg of rawMessages) {
            const msgTs = msg.timestamp || 0;

            // 时间过滤
            if (msgTs < afterTs || msgTs > beforeTs) continue;
            if (out.length >= limit) break;

            const idSer = msg.id?._serialized || msg.id;

            // 获取消息内容
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
                ack: msg.ack
            };

            // 处理媒体
            if (includeMedia || includeBase64) {
                const MEDIA_TYPES = new Set(['image', 'video', 'audio', 'ptt', 'sticker', 'document']);
                if (msg.hasMedia || MEDIA_TYPES.has(msg.type)) {
                    try {
                        // ===== 第一次尝试用 API 下载 =====
                        let media = await msg.downloadMedia().catch(() => null);

                        // ===== 如果 API 失败，从 DOM 提取媒体 =====
                        if (!media?.data) {
                            console.log(`[messages-sync] API failed, extracting from DOM for ${idSer}...`);

                            try {
                                const extracted = await session.client.pupPage.evaluate(async (msgId, msgType) => {
                                    // 查找消息元素
                                    let msgEl = document.querySelector(`[data-id="${msgId}"]`);

                                    if (!msgEl) {
                                        // 滚动查找
                                        const container = document.querySelector('[data-testid="conversation-panel-messages"]')
                                            || document.querySelector('._ajyl');
                                        if (container) {
                                            container.scrollTop = 0;
                                            await new Promise(r => setTimeout(r, 500));
                                            msgEl = document.querySelector(`[data-id="${msgId}"]`);
                                        }
                                    }

                                    if (!msgEl) return { error: 'msg_not_found' };

                                    // 滚动到消息
                                    msgEl.scrollIntoView({ behavior: 'instant', block: 'center' });
                                    await new Promise(r => setTimeout(r, 300));

                                    // 检查是否有下载按钮（未下载状态）
                                    const downloadBtn = msgEl.querySelector('[data-icon="media-download"]')
                                        || msgEl.querySelector('[data-icon="audio-download"]')
                                        || msgEl.querySelector('span[data-icon="media-download"]')?.closest('button, div[role="button"]');

                                    if (downloadBtn) {
                                        // 点击下载
                                        downloadBtn.click();
                                        // 等待下载完成
                                        await new Promise(r => setTimeout(r, 6000));
                                    }

                                    // ===== 提取图片数据 =====
                                    if (msgType === 'image' || msgType === 'sticker') {
                                        // 查找已加载的图片
                                        const img = msgEl.querySelector('img[src^="blob:"]')
                                            || msgEl.querySelector('img[src*="base64"]')
                                            || msgEl.querySelector('[data-testid="media-canvas"] img')
                                            || msgEl.querySelector('img[draggable="false"]');

                                        if (img && img.src) {
                                            if (img.src.startsWith('blob:')) {
                                                // blob URL 转 base64
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
                                                // 已经是 base64
                                                const matches = img.src.match(/^data:(.+);base64,(.+)$/);
                                                if (matches) {
                                                    return { mimetype: matches[1], data: matches[2] };
                                                }
                                            }
                                        }

                                        // 尝试从 canvas 获取
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

                                    // ===== 视频/音频暂时跳过 DOM 提取 =====
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

                                    // 最后再尝试一次 API
                                    await new Promise(r => setTimeout(r, 2000));
                                    media = await msg.downloadMedia().catch(() => null);
                                }
                            } catch (extractErr) {
                                console.log(`[messages-sync] Extract error: ${extractErr?.message}`);
                            }
                        }

                        if (media?.data) {
                            mediaCount++;

                            // 获取文件扩展名
                            const ext = mimeToExt(media.mimetype) || '';
                            const filename = media.filename || `${idSer}${ext}`;

                            item.media = {
                                mimetype: media.mimetype,
                                filename: filename,
                                bytes: Buffer.from(media.data, 'base64').length,
                                // ===== 关键：直接返回 data_url =====
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

        // 按时间排序（旧到新）
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
    if (session.status !== 'ready') return res.status(400).json({ ok: false, error: 'Session not ready' });

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

        // 【去重检查】如果有 message_id
        if (message_id) {
            const cached = sendMediaCache.get(message_id);
            if (cached) {
                // 如果已经有结果，直接返回
                if (cached.result) {
                    console.log(`[send/media] DEDUP: message_id=${message_id} already completed, returning cached result`);
                    return ok(200, cached.result);
                }
                // 如果正在处理中，等待完成（最多等30秒）
                if (cached.processing) {
                    console.log(`[send/media] DEDUP: message_id=${message_id} is processing, waiting...`);
                    for (let i = 0; i < 60; i++) {
                        await new Promise(r => setTimeout(r, 500));
                        const updated = sendMediaCache.get(message_id);
                        if (updated?.result) {
                            console.log(`[send/media] DEDUP: message_id=${message_id} completed while waiting`);
                            return ok(200, updated.result);
                        }
                    }
                    // 等待超时，返回处理中状态
                    console.log(`[send/media] DEDUP: message_id=${message_id} wait timeout`);
                    return ok(200, { ok: true, pending: true, message: 'Request is being processed' });
                }
            }
            // 标记为"处理中"
            sendMediaCache.set(message_id, { ts: Date.now(), processing: true, result: null });
        }

        if (!sessionId) return bad(400, 'missing sessionId');
        if (!to && !to_lid) return bad(400, 'missing to or to_lid');

        const session = sessions[sessionId];
        if (!session || !session.client) return bad(400, 'session not ready');

        // ===== 关键：确定 chatIdOrPhone =====
        let chatIdOrPhone = null;

        if (to) {
            const { kind, digits, jid } = parseRecipient(to);
            if (kind === 'group') {
                chatIdOrPhone = jid;
            } else if (kind === 'user' && digits) {
                // 尝试获取真实 JID
                const r = await session.client.getNumberId(digits).catch(() => null);
                chatIdOrPhone = r?._serialized || (digits.length >= 7 ? `${digits}@c.us` : null);
            }
        }

        // 如果没有真实电话，使用隐私号
        if (!chatIdOrPhone && to_lid) {
            chatIdOrPhone = `${to_lid.replace(/\D/g, '')}@lid`;
            console.log(`[send/media] Using LID: ${chatIdOrPhone}`);
        }

        if (!chatIdOrPhone) return bad(400, 'recipient not found');

        // ===== 关键修复：获取 Chat 对象（和成功版本一样）=====
        let chat = null;
        try {
            chat = await session.client.getChatById(chatIdOrPhone);
            console.log(`[send/media] Got chat: ${chat?.id?._serialized}`);
        } catch (e) {
            console.log(`[send/media] getChatById failed: ${e?.message}`);
            // 尝试从缓存获取
            const cached = (session.chats || []).find(c => c?.id?._serialized === chatIdOrPhone);
            if (cached) {
                chat = cached;
                console.log(`[send/media] Using cached chat`);
            }
        }

        if (!chat) {
            return bad(400, `Cannot get chat for ${chatIdOrPhone}`);
        }

        // 收集媒体
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

        console.log(`[send/media] Chat: ${chat.id._serialized}, items: ${mediaItems.length}`);

        const results = [];

        for (let i = 0; i < mediaItems.length; i++) {
            const item = mediaItems[i];

            try {
                const itemMime = normalizeMime(item.mimetype, item.filename);

                // 确保文件名
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

                // 加载媒体
                let media;
                if (item.b64) {
                    media = new MessageMedia(itemMime, item.b64, itemFilename);
                } else if (item.url) {
                    media = await loadMedia({ url: item.url, mimetype: itemMime, filename: itemFilename });
                } else {
                    continue;
                }

                // 判断类型
                const mime = normalizeMime(media.mimetype || itemMime, itemFilename);
                const isVideo = mime.startsWith('video/');
                const isAudio = mime.startsWith('audio/');
                const isVoice = isAudio && (mime.includes('ogg') || mime.includes('opus'));

                const itemCaption = (i === 0) ? (item.caption || caption || '') : '';

                let msg = null;
                let method = 'standard';

                // ===== 关键修复：统一使用 chat.sendMessage =====
                const opts = {};
                if (itemCaption) opts.caption = itemCaption;
                if (isVoice) opts.sendAudioAsVoice = true;

                if (isVideo) {
                    // 视频：使用修复后的 sendVideo 函数
                    const r = await sendVideo(chat, media, itemCaption);
                    if (r.ok) {
                        msg = r.msg;
                        method = r.method;
                    } else {
                        results.push({ ok: false, error: r.error, index: i });
                        continue;
                    }
                } else {
                    // 非视频：直接用 chat.sendMessage
                    try {
                        msg = await chat.sendMessage(media, opts);
                        method = 'chat.sendMessage';
                    } catch (e) {
                        console.error(`[send/media] Item ${i} failed:`, e?.message);
                        results.push({ ok: false, error: e?.message, index: i });
                        continue;
                    }
                }

                if (msg) {
                    const msgId = msg.id?._serialized || null;
                    console.log(`[send/media] Item ${i} OK: method=${method}`);

                    // 标记为从 Chatwoot 发送
                    if (typeof markSentFromChatwoot === 'function') {
                        markSentFromChatwoot(msgId);
                    }

                    // 触发清晰化
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
        console.log(`[send/media] DONE: ${successCount}/${mediaItems.length}`);

        const finalResult = {
            ok: successCount > 0,
            to: chat.id._serialized,
            results,
            total: mediaItems.length,
            success: successCount
        };

        // 【去重缓存】保存成功结果，防止重试时重复发送
        if (message_id) {
            sendMediaCache.set(message_id, { ts: Date.now(), processing: false, result: finalResult });
            console.log(`[send/media] Cached result for message_id=${message_id}`);
        }

        return ok(200, finalResult);
    } catch (e) {
        console.error('[send/media] ERROR:', e);
        // 【修复】失败时清除处理中标记，允许重试
        const message_id = req.body?.message_id;
        if (message_id) {
            sendMediaCache.delete(message_id);
        }
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
        // 立即刷新缓存
        sess.chats = await listChatsLiveOrCache(sessionId);
        res.json({ ok:true });
    } catch (e) {
        res.status(500).json({ ok:false, error: e?.message || String(e) });
    }
});
const PORT = process.env.BRIDGE_PORT; // 统一用 BRIDGE_PORT
app.listen(PORT, '0.0.0.0', () => {
    console.log(`[manager] up on 0.0.0.0:${PORT}`);
});