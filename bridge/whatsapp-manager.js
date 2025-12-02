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

    // 4) 仅当确实是 http(s) 才走 fromUrl
    if (url && /^https?:\/\//i.test(url)) {
        // 注意：fromUrl 内部会自己读并构造 MessageMedia
        return await MessageMedia.fromUrl(url, { unsafeMime: true, filename: filename || undefined });
    }

    throw new Error('no media source');
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
            // ===== REPLACE: 规范化号码，避免 @lid 导致的“假号码” =====
            let phoneJid = message.from || ''; // 往往是 447...@c.us
            if (!/@c\.us$/i.test(phoneJid)) {
                if (chat?.id?._serialized && /@c\.us$/i.test(chat.id._serialized)) {
                    phoneJid = chat.id._serialized;
                } else {
                    try {
                        const c = await message.getContact();
                        if (c?.id?._serialized && /@c\.us$/i.test(c.id._serialized)) {
                            phoneJid = c.id._serialized;
                        }
                    } catch (e) { /* ignore */ }
                }
            }

            rec.__jid    = phoneJid || (chat?.id?._serialized || '');
            rec.__server = chat.id?.server || '';     // 原样保留
            // 只取 c.us 的纯数字；若最终没拿到 c.us，就退回 chat.id.user（但这会触发下游兜底）
            rec.__phone  = phoneJid ? phoneJid.replace(/@.*/,'').replace(/\D/g,'')
                : (chat.id?.user || '');

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
                    name: rec.__name || '',
                    text: message.body || '',
                    type: message.type || 'chat',
                    messageId: (message.id && (message.id._serialized || message.id.id)) || '',
                    timestamp: message.timestamp ? (message.timestamp*1000) : Date.now(),
                    // 新增便于 collector 判断/排障（非必须）
                    from: message.from || '',
                    server: (chat.id && chat.id.server) || '',
                    chatId: chat?.id?._serialized || ''
                };

                // 可选：媒体占位
                if (message.hasMedia === true && typeof message.downloadMedia === 'function') {
                    try {
                        const media = await message.downloadMedia();
                        if (media && media.mimetype && media.data) {
                            payload.attachment = {
                                data_url: `data:${media.mimetype};base64,${media.data}`,
                                mime: media.mimetype,
                                name: media.filename || undefined
                            };
                        }
                    } catch (_) {}
                }

                await postWithRetry(`${COLLECTOR_BASE}/ingest`, payload, { 'x-api-token': COLLECTOR_TOKEN });
            } catch (e) {
                console.log(`[${sessionId}] forward to collector failed:`, e?.response?.data || e?.message || e);
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
        return res.status(404).json({ error: 'Session not found.' });
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
    if (!session) return res.status(404).json({ error: 'Session not found.' });
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
            return res.status(404).json({ error: 'Invalid chat or number.' });
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
// === REPLACE: /send/text ===
app.post('/send/text', async (req, res) => {
    const ok  = (code, data) => res.status(code).json(data);
    const bad = (code, msg)  => ok(code, { ok: false, error: msg });

    try {
        const { sessionId, to, text } = req.body || {};
        if (!sessionId || !text || to == null) return bad(400, 'missing sessionId/to/text');

        // ❗ 修正：不要用 SESSIONS.get(...)，而是用 sessions[sessionId]
        const session = sessions[sessionId];
        if (!session || !session.client) return bad(400, 'session not ready');

        const { kind, digits, jid } = parseRecipient(to);

        let targetJid = null;
        if (kind === 'group') {
            targetJid = jid;                             // 群聊：直接用传入的群 JID
        } else if (kind === 'user') {
            // 个人：用纯数字跑 getNumberId，拿到 WA 认可的最终 JID（可能是 @lid）
            const r = await session.client.getNumberId(digits).catch(() => null);
            targetJid = r && r._serialized ? r._serialized : null;
        } else {
            return bad(400, 'invalid recipient');
        }

        if (!targetJid) return bad(404, 'recipient not found');

        await session.client.sendMessage(targetJid, String(text));
        return ok(200, { ok: true, to: targetJid });
    } catch (e) {
        return res.status(500).json({ ok: false, error: e?.message || String(e) });
    }
});


// === REPLACE: /send/media ===
app.post('/send/media', async (req, res) => {
    const ok  = (code, data) => res.status(code).json(data);
    const bad = (code, msg)  => ok(code, { ok: false, error: msg });

    try {
        const { sessionId, to, type, url, b64, mimetype, filename, caption } = req.body || {};
        if (!sessionId || to == null) return bad(400, 'missing sessionId/to');

        // ❗ 修正：不要用 SESSIONS.get(...)，而是用 sessions[sessionId]
        const session = sessions[sessionId];
        if (!session || !session.client) return bad(400, 'session not ready');

        const { kind, digits, jid } = parseRecipient(to);

        let targetJid = null;
        if (kind === 'group') {
            targetJid = jid;
        } else if (kind === 'user') {
            const r = await session.client.getNumberId(digits).catch(() => null);
            targetJid = r && r._serialized ? r._serialized : null;
        } else {
            return bad(400, 'invalid recipient');
        }
        if (!targetJid) return bad(404, 'recipient not found');

        // 构建媒体对象
        let media;
        if (b64) {
            media = new MessageMedia(mimetype || 'application/octet-stream', b64, filename || undefined);
        } else if (url) {
            media = await MessageMedia.fromUrl(url, { unsafeMime: true });
        } else {
            return bad(400, 'missing url/b64');
        }

        await session.client.sendMessage(targetJid, media, { caption: caption || '' });
        return ok(200, { ok: true, to: targetJid });
    } catch (e) {
        return res.status(500).json({ ok: false, error: e?.message || String(e) });
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