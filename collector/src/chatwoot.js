const axios = require('axios');
const FormData = require('form-data');
const path = require('path');
const { CFG } = require('./config');

// ★★★ 新增：联系人内存缓存（解决 Chatwoot 搜索 API 索引延迟问题） ★★★
const contactCache = new Map();  // key: digits -> value: { contact, timestamp }
const conversationCache = new Map();  // key: contact_id:inbox_id -> value: { conversation, timestamp }
const CACHE_TTL = 5 * 60 * 1000; // 5 分钟过期

// ★★★ 新增：锁机制（防止并发创建多个联系人/会话） ★★★
const contactLocks = new Map();      // key: digits -> value: Promise
const conversationLocks = new Map();  // key: contact_id:inbox_id -> value: Promise

async function withContactLock(digits, fn) {
  if (!digits) return fn();

  // 等待之前的操作完成
  while (contactLocks.has(digits)) {
    await contactLocks.get(digits).catch(() => {});
  }

  // 创建新的 Promise 并设置锁
  let resolve;
  const lockPromise = new Promise(r => { resolve = r; });
  contactLocks.set(digits, lockPromise);

  try {
    return await fn();
  } finally {
    contactLocks.delete(digits);
    resolve();
  }
}

async function withConversationLock(contact_id, inbox_id, fn) {
  const key = `${contact_id}:${inbox_id}`;

  // 等待之前的操作完成
  while (conversationLocks.has(key)) {
    await conversationLocks.get(key).catch(() => {});
  }

  // 创建新的 Promise 并设置锁
  let resolve;
  const lockPromise = new Promise(r => { resolve = r; });
  conversationLocks.set(key, lockPromise);

  try {
    return await fn();
  } finally {
    conversationLocks.delete(key);
    resolve();
  }
}

// ★★★ 修改：缓存 key 改为 digits:sessionId，区分不同 WhatsApp 账号 ★★★
function cacheContact(digits, sessionId, contact) {
  if (!digits || !contact?.id) return;
  const key = sessionId ? `${digits}:${sessionId}` : digits;
  contactCache.set(key, { contact, timestamp: Date.now() });
  console.log(`[contactCache] Cached contact ${contact.id} for key ${key}`);
}

function getCachedContact(digits, sessionId) {
  if (!digits) return null;
  const key = sessionId ? `${digits}:${sessionId}` : digits;
  const cached = contactCache.get(key);
  if (!cached) return null;
  if (Date.now() - cached.timestamp > CACHE_TTL) {
    contactCache.delete(key);
    return null;
  }
  console.log(`[contactCache] Hit! contact ${cached.contact.id} for key ${key}`);
  return cached.contact;
}

// ★★★ 新增：清除联系人缓存 ★★★
function clearContactCache(digits, sessionId) {
  if (!digits) return;
  const key = sessionId ? `${digits}:${sessionId}` : digits;
  contactCache.delete(key);
  console.log(`[contactCache] Cleared cache for key ${key}`);
}

// ★★★ 新增：会话缓存 ★★★
function cacheConversation(contact_id, inbox_id, conversation) {
  if (!contact_id || !inbox_id || !conversation?.id) return;
  const key = `${contact_id}:${inbox_id}`;
  conversationCache.set(key, { conversation, timestamp: Date.now() });
  console.log(`[conversationCache] Cached conversation ${conversation.id} for contact ${contact_id} inbox ${inbox_id}`);
}

function getCachedConversation(contact_id, inbox_id) {
  if (!contact_id || !inbox_id) return null;
  const key = `${contact_id}:${inbox_id}`;
  const cached = conversationCache.get(key);
  if (!cached) return null;
  if (Date.now() - cached.timestamp > CACHE_TTL) {
    conversationCache.delete(key);
    return null;
  }
  console.log(`[conversationCache] Hit! conversation ${cached.conversation.id} for contact ${contact_id} inbox ${inbox_id}`);
  return cached.conversation;
}

// ★★★ 新增：清除会话缓存 ★★★
function clearConversationCache(contact_id, inbox_id) {
  if (!contact_id || !inbox_id) return;
  const key = `${contact_id}:${inbox_id}`;
  conversationCache.delete(key);
  console.log(`[conversationCache] Cleared cache for contact ${contact_id} inbox ${inbox_id}`);
}

// ★★★ 修改：清除所有与联系人相关的缓存（需要 sessionId） ★★★
function clearAllCacheForDigits(digits, sessionId) {
  if (!digits) return;
  const key = sessionId ? `${digits}:${sessionId}` : digits;
  const cached = contactCache.get(key);
  if (cached?.contact?.id) {
    // 清除该联系人的所有会话缓存
    for (const cacheKey of conversationCache.keys()) {
      if (cacheKey.startsWith(`${cached.contact.id}:`)) {
        conversationCache.delete(cacheKey);
        console.log(`[conversationCache] Cleared related cache: ${cacheKey}`);
      }
    }
  }
  contactCache.delete(key);
  console.log(`[cache] Cleared all cache for key ${key}`);
}

// 尝试导入 SessionManager（可选依赖）
let sessionManager = null;
try {
  const sm = require('./session-manager');
  sessionManager = sm.sessionManager;
  console.log('[chatwoot] SessionManager loaded for dynamic name mapping');
} catch (e) {
  console.log('[chatwoot] SessionManager not available, using fallback names');
}

// 后备的静态名称映射（当 SessionManager 不可用时使用）
const FALLBACK_SESSION_NAMES = {
};

// 从环境变量加载额外的后备映射
try {
  const fallbackEnv = process.env.FALLBACK_SESSION_NAMES || '';
  if (fallbackEnv) {
    fallbackEnv.split(',').forEach(pair => {
      const [id, name] = pair.split(':').map(s => s.trim());
      if (id && name) FALLBACK_SESSION_NAMES[id] = name;
    });
  }
} catch (_) {}

const CW_BASE = (CFG.chatwoot.baseURL || '').replace(/\/$/, '');
const CW_TOKEN = CFG.chatwoot.token;
const DEFAULT_ACCOUNT_ID = CFG.chatwoot.accountId;

if (!CW_BASE || !CW_TOKEN || !DEFAULT_ACCOUNT_ID) {
  console.warn('[chatwoot] 请检查 CHATWOOT_BASE_URL / CHATWOOT_API_TOKEN / CHATWOOT_ACCOUNT_ID');
}

// ---------- 小工具 ----------

const EXT2MIME = {
  // images
  jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png', gif: 'image/gif',
  webp: 'image/webp', avif: 'image/avif', heic: 'image/heic', heif: 'image/heif',
  // video
  mp4: 'video/mp4', webm: 'video/webm',
  // audio
  mp3: 'audio/mpeg', wav: 'audio/wav', ogg: 'audio/ogg', oga: 'audio/ogg', opus: 'audio/ogg', m4a: 'audio/mp4', aac: 'audio/aac', amr: 'audio/amr',
  // docs
  pdf: 'application/pdf'
};
// ---- 简单魔数嗅探（优先于 headerType / 扩展名）----
function sniffMime(buffer) {
  if (!buffer || buffer.length < 12) return null;
  // PNG
  if (buffer[0] === 0x89 && buffer.toString('ascii',1,4) === 'PNG') return 'image/png';
  // JPEG
  if (buffer[0] === 0xFF && buffer[1] === 0xD8) return 'image/jpeg';
  // GIF
  if (buffer.toString('ascii',0,3) === 'GIF') return 'image/gif';
  // WEBP / RIFF
  if (buffer.toString('ascii',0,4) === 'RIFF') {
    const fourcc = buffer.toString('ascii',8,12);
    if (fourcc === 'WEBP') return 'image/webp';
    if (fourcc === 'WAVE') return 'audio/wav';
  }
  // OGG (Opus/Voice note)
  if (buffer.toString('ascii',0,4) === 'OggS') return 'audio/ogg';
  // MP3 (ID3) 或帧同步
  if (buffer.toString('ascii',0,3) === 'ID3') return 'audio/mpeg';
  if (buffer[0] === 0xFF && (buffer[1] & 0xE0) === 0xE0) return 'audio/mpeg';
  // MP4 / M4A（ftyp）
  if (buffer.length >= 12 && buffer.toString('ascii',4,8) === 'ftyp') return 'video/mp4';
  return null;
}

const cleanMime = (s) => String(s || '').split(';')[0].trim() || '';

const mimeFromExt = (ext) => EXT2MIME[String(ext || '').toLowerCase()] || null;
// 反查：mime -> 合适的扩展名
const MIME2EXT = Object.entries(EXT2MIME).reduce((acc, [ext, mime]) => {
  const m = cleanMime(mime);
  if (!acc[m]) acc[m] = ext; // 取第一个匹配的扩展名
  return acc;
}, {});

const extFromMime = (mime) => MIME2EXT[cleanMime(mime)] || null;

// 强制让文件扩展名与 MIME 一致（Edge/必应更严格）
function forceFileExt(filename, mime) {
  const wantExt = extFromMime(mime);
  if (!wantExt) return filename || `upload.bin`;
  const base = path.basename(filename || 'upload', path.extname(filename || ''));
  return `${base}.${wantExt}`;
}
// 兼容 data:<mime>[;param...];base64,<data>（如 audio/ogg; codecs=opus;base64,...）
function parseDataUrlLoose(dataUrl) {
  if (!dataUrl || typeof dataUrl !== 'string') return null;
  const i = dataUrl.indexOf(',');
  if (i <= 5 || !/;base64/i.test(dataUrl.slice(0, i))) return null;

  const header = dataUrl.slice(5, i); // 去掉 'data:'
  const meta = header.split(';').map(s => s.trim());
  const mime = cleanMime(meta[0] || 'application/octet-stream');
  const b64  = dataUrl.slice(i + 1);
  const buf  = Buffer.from(b64, 'base64');

  // 关键：按 MIME 纠正扩展名（尤其 OGG）
  const filename = forceFileExt('upload', mime);
  return { buffer: buf, filename, contentType: mime };
}

async function request(method, urlPath, data, extraHeaders = {}) {
  const url = `${CW_BASE}${urlPath}`;
  const headers = {
    'api_access_token': CW_TOKEN,  // 修复：使用 CW_TOKEN 而不是 CFG.chatwoot.apiAccessToken
    ...extraHeaders
  };

  if (!(data instanceof require('form-data'))) {
    headers['Content-Type'] = 'application/json';
  }

  try {
    const response = await axios({
      method,
      url,
      headers,
      data: data instanceof require('form-data') ? data : JSON.stringify(data),
      timeout: 30000
    });

    return response.data;
  } catch (e) {
    // ★★★ 修复：正确抛出 Chatwoot API 错误 ★★★
    const cwError = e?.response?.data;
    if (cwError) {
      const err = new Error(cwError.error || cwError.message || JSON.stringify(cwError));
      err._cw = cwError;
      err.response = e.response;
      throw err;
    }
    throw e;
  }
}


async function fetchAsBuffer(url) {
  const res = await axios.get(url, {
    responseType: 'arraybuffer',
    maxBodyLength: Infinity,
    maxContentLength: Infinity,
    validateStatus: s => s >= 200 && s < 400, // 允许重定向
  });

  const buffer = Buffer.from(res.data);

  // 先取 URL 推断的文件名
  let filename = 'upload.bin';
  try {
    const p = new URL(url).pathname || '';
    const base = path.basename(p);
    if (base && base !== '/') filename = base;
  } catch {}

  // 三重判定：优先魔数嗅探，其次扩展名，最后响应头
  const headerType = cleanMime(res.headers?.['content-type']);
  const extType    = mimeFromExt(path.extname(filename).slice(1));
  const sniffType  = sniffMime(buffer);

  // 某些网关会返回 text/html / octet-stream，优先用更可信的类型
  let contentType = sniffType || extType || headerType || 'application/octet-stream';

  // 文件名与 MIME 对齐（Edge/必应严格校验）
  filename = forceFileExt(filename, contentType);

  return { buffer, filename, contentType };
}


// 根据 sessionId 和原始 name/phone 生成联系人显示名
function buildDisplayName(sessionId, rawName, phone_e164, identifier, sessionName = null) {
  const rhs = (rawName && rawName.trim()) || phone_e164 || identifier || 'WA User';

  // ★ 优先使用传入的 sessionName（来自 bridge payload）
  if (sessionName && sessionName !== sessionId) {
    return `${sessionName}>${rhs}`;
  }

  // 其次从 SessionManager 获取动态名称
  if (sessionManager) {
    const dynamicName = sessionManager.getSessionName(sessionId);
    if (dynamicName) {
      return `${dynamicName}>${rhs}`;
    }
  }

  // 再次使用后备静态映射
  if (FALLBACK_SESSION_NAMES[sessionId]) {
    return `${FALLBACK_SESSION_NAMES[sessionId]}>${rhs}`;
  }

  // 最后使用 sessionId 本身
  return `${sessionId || 'unknown'}>${rhs}`;
}
function normalizeE164(rawPhone, rawName) {
  const pick = (s) => {
    if (!s) return null;
    const mPlus = (s.match(/\+[\d\s\-]{6,}/) || [])[0];
    if (mPlus) return '+' + mPlus.replace(/[^\d]/g, '');
    const mDigits = (s.match(/[\d\s\-]{6,}/) || [])[0];
    if (mDigits) return '+' + mDigits.replace(/[^\d]/g, '');
    return null;
  };
  const fromName = pick(rawName);
  if (fromName) return fromName;
  return pick(rawPhone);
}

/**
 * ★★★ 新增：检查字符串是否看起来像 WhatsApp 电话号码 ★★★
 * WhatsApp 电话号码格式规则：
 * - E.164 国际格式：+国家代码 + 区号 + 本地号码
 * - 纯数字：7-15 位
 * - 可能包含空格、横杠、括号等分隔符
 *
 * @param {string} str - 要检查的字符串
 * @returns {{ isPhone: boolean, digits: string, e164: string|null }}
 */
function extractPhoneFromName(str) {
  if (!str || typeof str !== 'string') {
    return { isPhone: false, digits: '', e164: null };
  }

  const trimmed = str.trim();

  // 1. 检查是否主要由数字和电话分隔符组成
  // 允许的字符：数字、+、-、空格、括号、点
  const phoneCharsOnly = /^[\d\s\-\+\(\)\.]+$/;

  // 2. 提取所有数字
  const digits = trimmed.replace(/[^\d]/g, '');

  // 3. 检查条件
  // - 字符串主要是电话字符
  // - 纯数字长度在 7-15 位之间（国际电话号码标准）
  // - 原始字符串中数字占比超过 50%（避免误判普通文本）
  const isPhoneFormat = phoneCharsOnly.test(trimmed);
  const validLength = digits.length >= 7 && digits.length <= 15;
  const digitRatio = digits.length / trimmed.replace(/\s/g, '').length;

  if (isPhoneFormat && validLength && digitRatio >= 0.5) {
    // 构建 E.164 格式
    let e164 = null;
    if (trimmed.startsWith('+')) {
      e164 = '+' + digits;
    } else if (digits.length >= 10) {
      // 假设是国际号码，加上 + 前缀
      e164 = '+' + digits;
    }

    return { isPhone: true, digits, e164 };
  }

  return { isPhone: false, digits: '', e164: null };
}

// ---------- 通用请求 ----------
async function cwRequest(method, pathOrUrl, { data, params, headers } = {}) {
  const url = /^https?:\/\//i.test(pathOrUrl) ? pathOrUrl : `${CW_BASE}${pathOrUrl}`;
  try {
    const res = await axios({
      method,
      url,
      data,
      params,
      headers: {
        'api_access_token': CW_TOKEN,
        ...(headers || {})
      },
      validateStatus: s => s >= 200 && s < 300
    });
    return res.data;
  } catch (e) {
    const body = e.response?.data;
    const err = new Error(body?.error || body?.message || e.message || 'Chatwoot request failed');
    err._cw = body;
    err.status = e.response?.status;
    throw err;
  }
}

// ---------- 账户 / 收件箱 ----------
async function listAccounts() {
  return cwRequest('get', `/api/v1/accounts`);
}
async function listInboxes(account_id = DEFAULT_ACCOUNT_ID) {
  return cwRequest('get', `/api/v1/accounts/${account_id}/inboxes`);
}
async function getInboxIdByIdentifier(account_id = DEFAULT_ACCOUNT_ID, identifierOrName) {
  if (!identifierOrName) throw new Error('缺少 inbox identifier');
  const list = await listInboxes(account_id);
  const arr = (list && list.payload) || list || [];
  const hit = arr.find(x =>
      String(x.identifier || '').toLowerCase() === String(identifierOrName).toLowerCase() ||
      String(x.name || '').toLowerCase() === String(identifierOrName).toLowerCase()
  );
  if (!hit) throw new Error(`找不到 Inbox（identifier/name=${identifierOrName}）`);
  return hit.id;
}

async function searchContact({ account_id = DEFAULT_ACCOUNT_ID, identifier /*, phone_e164*/ }) {
  if (!identifier) return null;
  const q = encodeURIComponent(identifier.slice(0, 80));
  const res = await cwRequest('get', `/api/v1/accounts/${account_id}/contacts/search?q=${q}`);
  const list = Array.isArray(res?.payload) ? res.payload : (res || []);
  return list.find(c => (c.identifier || '') === identifier) || null;
}

async function createContact({ account_id = DEFAULT_ACCOUNT_ID, name, identifier, phone_e164, custom_attributes, withPhone = true }) {
  const data = {
    name: name || (phone_e164 || identifier || 'WA User'),
    identifier,
    ...(withPhone && phone_e164 ? { phone_number: phone_e164 } : {}),
    ...(custom_attributes ? { custom_attributes } : {})
  };
  const result = await cwRequest('post', `/api/v1/accounts/${account_id}/contacts`, { data });
  // ★★★ 关键修复：解析 API 返回的 { payload: { contact: { ... } } } 结构 ★★★
  if (result?.payload?.contact) {
    return result.payload.contact;
  }
  return result;
}

async function updateContact({ account_id = DEFAULT_ACCOUNT_ID, contact_id, patch }) {
  const result = await cwRequest('patch', `/api/v1/accounts/${account_id}/contacts/${contact_id}`, { data: patch });
  // ★★★ 关键修复：解析 API 返回的结构 ★★★
  if (result?.payload?.contact) {
    return result.payload.contact;
  }
  return result;
}

async function ensureContact({ account_id = DEFAULT_ACCOUNT_ID, rawPhone, rawPhone_lid, rawName, sessionId, sessionName, messageId }) {
  // 从messageId提取@lid前的纯数字
  const getLidDigits = (mid) => {
    if (!mid) return '';
    const match = mid.match(/(\d+)@lid/);
    return match ? match[1] : '';
  };

  // ★★★ 新增：当没有电话号码时，检查联系人名称是否看起来像电话号码 ★★★
  // WhatsApp 在没有保存联系人时会显示电话号码作为名称
  let extractedPhoneFromName = null;
  if (!rawPhone && rawPhone_lid && rawName) {
    const phoneCheck = extractPhoneFromName(rawName);
    if (phoneCheck.isPhone && phoneCheck.digits) {
      console.log(`[ensureContact] Name "${rawName}" looks like phone number, extracted: ${phoneCheck.digits}`);
      extractedPhoneFromName = phoneCheck.digits;
    }
  }

  // ★★★ 判断是 phone 还是 lid ★★★
  // 如果从名称中提取到电话号码，优先使用它
  const effectivePhone = rawPhone || extractedPhoneFromName;
  const isLid = !effectivePhone && (rawPhone_lid || getLidDigits(messageId));
  const finalPhone = effectivePhone || rawPhone_lid || getLidDigits(messageId);
  const digits = (String(finalPhone || '').match(/\d+/g) || []).join('');

  // ★★★ 修改 identifier 格式，区分 phone 和 lid ★★★
  // 新格式: wa:sessionId:phone:123456 或 wa:sessionId:lid:123456
  // 如果从名称中提取到电话，使用 phone 类型
  const identifierType = isLid ? 'lid' : 'phone';
  const identifier = `wa:${sessionId || 'default'}:${identifierType}:${digits}`;

  // ★★★ 电话号码规范化：优先使用提取的电话，其次从名称提取 ★★★
  let phone_e164 = normalizeE164(effectivePhone, rawName);
  // 如果从名称提取到电话号码，确保使用它
  if (extractedPhoneFromName && !phone_e164) {
    phone_e164 = '+' + extractedPhoneFromName;
  }

  // ★★★ 缓存 key 改为 digits:sessionId，区分不同 WhatsApp 账号 ★★★
  let contact = getCachedContact(digits, sessionId);
  if (contact) {
    console.log(`[ensureContact] Found in cache: id=${contact.id}, name=${contact.name}`);
    // 缓存命中，但还是更新一下信息
    const wantName = buildDisplayName(sessionId, rawName, phone_e164, identifier, sessionName);
    const wantAttrs = { session_id: sessionId || null };

    // 检查是否需要更新名称
    if (contact.name !== wantName) {
      const originalContact = contact;
      try {
        const updated = await updateContact({ account_id, contact_id: contact.id, patch: { name: wantName } });
        if (updated && updated.id) {
          contact = updated;
          cacheContact(digits, sessionId, contact);  // 更新缓存
        } else {
          contact = originalContact;  // 恢复原来的 contact
        }
      } catch (_) {
        contact = originalContact;  // 恢复原来的 contact
      }
    }
    return contact;
  }

  // ★★★ 使用锁防止并发创建多个联系人（锁 key 也要包含 sessionId）★★★
  const lockKey = sessionId ? `${digits}:${sessionId}` : digits;
  return withContactLock(lockKey, async () => {
    // 再次检查缓存（可能在等待锁期间被其他请求创建了）
    let contact = getCachedContact(digits, sessionId);
    if (contact) {
      console.log(`[ensureContact] Found in cache after lock: id=${contact.id}, name=${contact.name}`);
      return contact;
    }

    // ★★★ 原有搜索逻辑 ★★★

    // 1. 按新格式 identifier 搜索
    contact = await searchContact({ account_id, identifier });
    if (contact) {
      console.log(`[ensureContact] Found by identifier: id=${contact.id}, name=${contact.name}`);
    }

    // 1.5 ★★★ 兼容旧格式 identifier 搜索 ★★★
    if (!contact && digits && sessionId) {
      const oldIdentifier = `wa:${sessionId}:${digits}`;
      contact = await searchContact({ account_id, identifier: oldIdentifier });
      if (contact) {
        console.log(`[ensureContact] Found by old identifier: id=${contact.id}, name=${contact.name}`);
      }
    }

    // 2. 如果找不到，按电话号码精确搜索
    // ★★★ 修复：LID 不是真实电话号码，不应该用 phone_e164 搜索 ★★★
    if (!contact && phone_e164 && !isLid) {
      contact = await findContactByPhone({ account_id, phone_e164 });
      if (contact) {
        // ★★★ 检查找到的联系人是否属于当前 session ★★★
        const contactSessionId = contact.identifier?.split(':')[1];
        if (sessionId && contactSessionId && contactSessionId !== sessionId) {
          console.log(`[ensureContact] Found by phone_e164 but different session: ${contactSessionId} vs ${sessionId}, skipping`);
          contact = null;
        } else {
          console.log(`[ensureContact] Found by phone_e164: id=${contact.id}, name=${contact.name}`);
        }
      }
    }

    // 3. 如果还找不到，按纯数字搜索（更广泛）
    if (!contact && digits) {
      console.log(`[ensureContact] Searching by digits: ${digits}`);
      const searchResult = await findContactByDigits({ account_id, digits });
      if (searchResult) {
        // ★★★ 检查找到的联系人是否属于当前 session ★★★
        const contactSessionId = searchResult.identifier?.split(':')[1];
        if (sessionId && contactSessionId && contactSessionId !== sessionId) {
          console.log(`[ensureContact] Found by digits but different session: ${contactSessionId} vs ${sessionId}, skipping`);
          // 不使用这个联系人
        } else {
          console.log(`[ensureContact] Found by digits: id=${searchResult.id}, name=${searchResult.name}`);
          contact = searchResult;
        }
      }
    }

    const wantName = buildDisplayName(sessionId, rawName, phone_e164, identifier, sessionName);
    const wantAttrs = { session_id: sessionId || null };

    if (!contact) {
      console.log(`[ensureContact] No existing contact found, creating new: ${wantName}`);
      try {
        contact = await createContact({
          account_id,
          name: wantName,
          identifier,
          phone_e164,
          custom_attributes: wantAttrs,
          withPhone: true
        });
      } catch (e) {
        const msg = (e._cw?.error || e._cw?.message || e.message || '').toLowerCase();
        if (msg.includes('identifier') && msg.includes('taken')) {
          await new Promise(r => setTimeout(r, 500));
          contact = await searchContact({ account_id, identifier });
          if (!contact) throw e;
        } else if (msg.includes('phone number') && msg.includes('taken')) {
          contact = await createContact({
            account_id,
            name: wantName,
            identifier,
            phone_e164: undefined,
            custom_attributes: wantAttrs,
            withPhone: false
          });
        } else {
          throw e;
        }
      }
    } else {
      // 更新联系人信息（更新 identifier 确保统一）
      const patch = {};
      const pure = (s) => (s || '').replace(/[^\+\d]/g, '');

      // ★★★ 关键：如果找到的联系人 identifier 不同，更新它 ★★★
      if (contact.identifier !== identifier) {
        console.log(`[ensureContact] Updating identifier: ${contact.identifier} -> ${identifier}`);
        patch.identifier = identifier;
      }

      if (contact.name !== wantName) patch.name = wantName;
      if (phone_e164) {
        const owner = await findContactByPhone({ account_id, phone_e164 });
        if (!owner || owner.id === contact.id) {
          if (pure(contact.phone_number) !== pure(phone_e164)) patch.phone_number = phone_e164;
        }
      }
      if ((contact.custom_attributes?.session_id || null) !== (sessionId || null)) {
        patch.custom_attributes = { ...(contact.custom_attributes || {}), session_id: sessionId || null };
      }
      if (Object.keys(patch).length) {
        // ★★★ 关键修复：保存原来的 contact，防止 updateContact 返回 undefined ★★★
        const originalContact = contact;
        try {
          const updated = await updateContact({ account_id, contact_id: contact.id, patch });
          // 只有返回有效的联系人对象时才更新
          if (updated && updated.id) {
            contact = updated;
            console.log(`[ensureContact] Updated contact: id=${contact.id}`);
          } else {
            console.warn(`[ensureContact] Update returned invalid data, keeping original contact: id=${originalContact.id}`);
            contact = originalContact;
          }
        } catch (e) {
          console.warn(`[ensureContact] Update failed:`, e.message);
          contact = originalContact;  // 恢复原来的 contact
        }
      }
    }

    // ★★★ 新增：将联系人添加到缓存（包含 sessionId）★★★
    if (contact && digits) {
      cacheContact(digits, sessionId, contact);
    }

    return contact;
  });
}


async function ensureContactAndInbox({ account_id = DEFAULT_ACCOUNT_ID, inbox_id, phone, name, sessionId }) {
  if (!inbox_id) throw new Error(`ensureContactAndInbox 缺少 inbox_id`);
  const contact = await ensureContact({ account_id, rawPhone: phone, rawName: name, sessionId });
  return { account_id, inbox_id, contact_id: contact.id };
}
// 按 E.164 手机号查是否已被别的联系人占用
async function findContactByPhone({ account_id = DEFAULT_ACCOUNT_ID, phone_e164 }) {
  if (!phone_e164) return null;
  const q = encodeURIComponent(phone_e164.slice(0, 80));
  const res = await cwRequest('get', `/api/v1/accounts/${account_id}/contacts/search?q=${q}`);
  const list = Array.isArray(res?.payload) ? res.payload : (res || []);
  const pure = (s) => (s || '').replace(/[^\+\d]/g, '');
  return list.find(c => pure(c.phone_number) === pure(phone_e164)) || null;
}

// ★★★ 新增：按纯数字搜索联系人（更广泛的搜索） ★★★
// 可以找到电话号码在 name、phone_number、identifier 中的联系人
async function findContactByDigits({ account_id = DEFAULT_ACCOUNT_ID, digits }) {
  if (!digits || digits.length < 6) return null;

  // 搜索时只用后 10 位数字（避免国家代码差异）
  const searchDigits = digits.length > 10 ? digits.slice(-10) : digits;
  const q = encodeURIComponent(searchDigits);

  try {
    const res = await cwRequest('get', `/api/v1/accounts/${account_id}/contacts/search?q=${q}`);
    const list = Array.isArray(res?.payload) ? res.payload : (res || []);

    if (list.length === 0) return null;

    // 在搜索结果中找最匹配的
    const pure = (s) => (s || '').replace(/\D/g, '');

    for (const c of list) {
      // 检查 phone_number
      if (c.phone_number && pure(c.phone_number).includes(searchDigits)) {
        return c;
      }
      // 检查 name 中是否包含这些数字
      if (c.name && pure(c.name).includes(searchDigits)) {
        return c;
      }
      // 检查 identifier
      if (c.identifier && pure(c.identifier).includes(searchDigits)) {
        return c;
      }
    }

    return null;
  } catch (e) {
    console.warn(`[findContactByDigits] Search failed:`, e.message);
    return null;
  }
}

// ---------- 会话 ----------
async function getContactConversations({ account_id = DEFAULT_ACCOUNT_ID, contact_id }) {
  return cwRequest('get', `/api/v1/accounts/${account_id}/contacts/${contact_id}/conversations`);
}

async function createConversation({ account_id = DEFAULT_ACCOUNT_ID, inbox_id, contact_id, message }) {
  const data = { inbox_id, contact_id };
  if (message) data.message = message;
  return cwRequest('post', `/api/v1/accounts/${account_id}/conversations`, { data });
}

async function ensureConversation({ account_id = DEFAULT_ACCOUNT_ID, contact_id, inbox_id }) {
  // ★★★ 新增：先查会话缓存 ★★★
  const cachedConv = getCachedConversation(contact_id, inbox_id);
  if (cachedConv) {
    console.log(`[ensureConversation] Found in cache: ${cachedConv.id}`);
    cachedConv._isNew = false;
    return cachedConv;
  }

  // ★★★ 使用锁防止并发创建多个会话 ★★★
  return withConversationLock(contact_id, inbox_id, async () => {
    // 再次检查缓存（可能在等待锁期间被其他请求创建了）
    const cachedConv2 = getCachedConversation(contact_id, inbox_id);
    if (cachedConv2) {
      console.log(`[ensureConversation] Found in cache after lock: ${cachedConv2.id}`);
      cachedConv2._isNew = false;
      return cachedConv2;
    }

    const list = await getContactConversations({ account_id, contact_id });
    const items = Array.isArray(list?.payload) ? list.payload : (list || []);

    // 1. 优先复用未解决的会话
    const openConv = items.find(c => Number(c.inbox_id) === Number(inbox_id) && c.status !== 'resolved');
    if (openConv) {
      console.log(`[ensureConversation] Reusing open conversation: ${openConv.id}`);
      openConv._isNew = false;
      cacheConversation(contact_id, inbox_id, openConv);  // 缓存
      return openConv;
    }

    // 2. 复用任何该 inbox 下的会话
    const anyConv = items.find(c => Number(c.inbox_id) === Number(inbox_id));
    if (anyConv) {
      console.log(`[ensureConversation] Reusing existing conversation (was ${anyConv.status}): ${anyConv.id}`);
      try {
        await cwRequest('post', `/api/v1/accounts/${account_id}/conversations/${anyConv.id}/toggle_status`, {
          data: { status: 'open' }
        });
      } catch (e) {
        console.warn(`[ensureConversation] Failed to reopen:`, e.message);
      }
      anyConv._isNew = false;
      cacheConversation(contact_id, inbox_id, anyConv);  // 缓存
      return anyConv;
    }

    // 3. 创建新会话
    console.log(`[ensureConversation] Creating new conversation for contact ${contact_id} in inbox ${inbox_id}`);
    const newConv = await createConversation({ account_id, inbox_id, contact_id });
    newConv._isNew = true;
    cacheConversation(contact_id, inbox_id, newConv);  // 缓存
    return newConv;
  });
}


async function getConversationDetails(account_id = DEFAULT_ACCOUNT_ID, conversation_id) {
  return cwRequest('get', `/api/v1/accounts/${account_id}/conversations/${conversation_id}`);
}

async function createMessageMultipart({ account_id = DEFAULT_ACCOUNT_ID, conversation_id, content, attachments = [] }) {
  const url = `${CW_BASE}/api/v1/accounts/${account_id}/conversations/${conversation_id}/messages`;

  // 先把所有附件转成 {buffer, filename, contentType}，避免重试时二次下载
  const files = [];
  for (const att of attachments || []) {
    if (!att) continue;

    if (att.data_url) {
      const parsed = parseDataUrlLoose(att.data_url);
      if (parsed) {
        // 统一文件名与 MIME（优先使用 data_url 的 MIME）
        const ct = cleanMime(att.file_type || parsed.contentType);
        const fn = forceFileExt(att.filename || parsed.filename, ct);
        files.push({ buffer: parsed.buffer, filename: fn, contentType: ct });
        continue;
      }
    }

    if (att.file_url) {
      const file = await fetchAsBuffer(att.file_url);
      const ct = cleanMime(att.file_type || file.contentType);
      const fn = forceFileExt(att.filename || file.filename, ct);
      files.push({ buffer: file.buffer, filename: fn, contentType: ct });
      continue;
    }

    if (att.buffer) {
      const ct = cleanMime(att.file_type || 'application/octet-stream');
      const fn = forceFileExt(att.filename || 'upload', ct);
      files.push({ buffer: att.buffer, filename: fn, contentType: ct });
    }
  }

  const buildForm = (msgTypeVal) => {
    const form = new FormData();
    if (content !== undefined && content !== null) form.append('content', String(content));
    form.append('message_type', msgTypeVal);  // 新版优先 'incoming'，必要时回退 1
    form.append('private', 'false');
    for (const f of files) {
      form.append('attachments[]', f.buffer, { filename: f.filename, contentType: f.contentType });
    }
    return form;
  };

  const tryPost = async (msgTypeVal) => {
    const form = buildForm(msgTypeVal);
    const headers = { ...form.getHeaders(), api_access_token: CW_TOKEN };
    const res = await axios.post(url, form, {
      headers,
      maxBodyLength: Infinity,
      maxContentLength: Infinity,
      validateStatus: s => s >= 200 && s < 300
    });
    return res.data;
  };

  // 首选字符串 'incoming'
  try {
    return await tryPost('incoming');
  } catch (e) {
    const body = e.response?.data;
    const msg  = (body?.error || body?.message || e.message || '').toString().toLowerCase();
    const invalidType = (e.response?.status === 422) && (msg.includes('message_type') || msg.includes('not a valid'));
    if (!invalidType) throw e;
  }

  // 回退为数值 1（兼容旧版）
  return await tryPost(1);
}



async function createIncomingMessage({ account_id = DEFAULT_ACCOUNT_ID, conversation_id, content, text, attachments }) {
  const bodyContent = (content ?? text ?? '');
  const hasAtt = Array.isArray(attachments) && attachments.length > 0;

  if (hasAtt) {
    return createMessageMultipart({ account_id, conversation_id, content: bodyContent, attachments });
  }

  return cwRequest('post',
      `/api/v1/accounts/${account_id}/conversations/${conversation_id}/messages`,
      { data: { content: String(bodyContent), message_type: 'incoming', private: false } }
  );
}
async function createContactNote({ account_id, contact_id, content }) {
  return cwRequest(
      'post',
      `/api/v1/accounts/${account_id}/contacts/${contact_id}/notes`,
      { data: { content: String(content || '') } }
  );
}

async function updateContactNote({ account_id, contact_id, note_id, content }) {
  return cwRequest(
      'put',
      `/api/v1/accounts/${account_id}/contacts/${contact_id}/notes/${note_id}`,
      { data: { content: String(content || '') } }
  );
}

/**
 * 获取会话消息列表
 */
async function getConversationMessages({ account_id = DEFAULT_ACCOUNT_ID, conversation_id, after, before }) {
  let allMessages = [];
  let beforeId = null;
  let hasMore = true;
  let pageCount = 0;
  const maxPages = 100;

  const afterDate = after ? new Date(after) : null;
  const beforeDate = before ? new Date(before) : null;

  console.log(`[getConversationMessages] Start: conv=${conversation_id}`);
  console.log(`[getConversationMessages] Range: ${afterDate?.toISOString() || 'none'} ~ ${beforeDate?.toISOString() || 'none'}`);

  while (hasMore && pageCount < maxPages) {
    try {
      let url = `/api/v1/accounts/${account_id}/conversations/${conversation_id}/messages`;
      if (beforeId) {
        url += `?before=${beforeId}`;
      }

      const result = await cwRequest('get', url);
      const messages = result?.payload || result || [];

      if (!Array.isArray(messages) || messages.length === 0) {
        hasMore = false;
        break;
      }

      // 打印第一页的时间范围，帮助调试
      if (pageCount === 0 && messages.length > 0) {
        const firstTime = new Date(messages[0].created_at).toISOString();
        const lastTime = new Date(messages[messages.length - 1].created_at).toISOString();
        console.log(`[getConversationMessages] Page 1 messages: ${lastTime} ~ ${firstTime}`);
      }

      allMessages.push(...messages);
      pageCount++;

      console.log(`[getConversationMessages] Page ${pageCount}: got ${messages.length}, total ${allMessages.length}`);

      if (messages.length < 15) {
        hasMore = false;
      } else {
        const lastMsg = messages[messages.length - 1];
        beforeId = lastMsg.id;

        if (after) {
          const lastTs = new Date(lastMsg.created_at).getTime();
          if (lastTs < after) {
            console.log(`[getConversationMessages] Reached boundary: msg time ${new Date(lastTs).toISOString()} < after`);
            hasMore = false;
          }
        }
      }

      if (hasMore) await new Promise(r => setTimeout(r, 50));
    } catch (e) {
      console.error(`[getConversationMessages] Error:`, e?.message);
      hasMore = false;
    }
  }

  console.log(`[getConversationMessages] Total: ${allMessages.length} in ${pageCount} pages`);

  // 时间过滤
  if (after || before) {
    const total = allMessages.length;
    let afterBefore = 0;  // 在 before 之后的（太新）
    let beforeAfter = 0;  // 在 after 之前的（太旧）

    allMessages = allMessages.filter(m => {
      const ts = new Date(m.created_at).getTime();
      if (before && ts > before) { afterBefore++; return false; }
      if (after && ts < after) { beforeAfter++; return false; }
      return true;
    });

    console.log(`[getConversationMessages] Filter: ${allMessages.length} in range, ${afterBefore} too new, ${beforeAfter} too old`);
  }

  return allMessages;
}



/**
 * 创建 outgoing 消息（客服/我方发送的消息）
 */
async function createOutgoingMessage({
                                       account_id = DEFAULT_ACCOUNT_ID,
                                       conversation_id,
                                       content,
                                       attachments = [],
                                       source_id
                                     }) {
  const hasAttachments = Array.isArray(attachments) && attachments.length > 0;

  if (hasAttachments) {
    // 有附件：使用 FormData
    const url = `${CW_BASE}/api/v1/accounts/${account_id}/conversations/${conversation_id}/messages`;

    const files = [];
    for (const att of attachments) {
      if (!att) continue;

      if (att.data_url) {
        const parsed = parseDataUrlLoose(att.data_url);
        if (parsed) {
          const ct = cleanMime(att.file_type || parsed.contentType);
          const fn = forceFileExt(att.filename || parsed.filename, ct);
          files.push({ buffer: parsed.buffer, filename: fn, contentType: ct });
        }
      } else if (att.file_url) {
        try {
          const file = await fetchAsBuffer(att.file_url);
          const ct = cleanMime(att.file_type || file.contentType);
          const fn = forceFileExt(att.filename || file.filename, ct);
          files.push({ buffer: file.buffer, filename: fn, contentType: ct });
        } catch (e) {
          console.error('[createOutgoingMessage] Fetch attachment failed:', e?.message);
        }
      }
    }

    const form = new FormData();
    form.append('content', String(content || ''));
    form.append('message_type', 'outgoing');
    form.append('private', 'false');

    for (const f of files) {
      form.append('attachments[]', f.buffer, { filename: f.filename, contentType: f.contentType });
    }

    const headers = { ...form.getHeaders(), api_access_token: CW_TOKEN };
    const res = await axios.post(url, form, {
      headers,
      maxBodyLength: Infinity,
      maxContentLength: Infinity,
      validateStatus: s => s >= 200 && s < 300
    });

    return res.data;
  }

  // 无附件：JSON 请求
  return cwRequest('post', `/api/v1/accounts/${account_id}/conversations/${conversation_id}/messages`, {
    data: {
      content: String(content || ''),
      message_type: 'outgoing',
      private: false
    }
  });
}


/**
 * 删除消息（用于 replace 模式）
 */
async function deleteMessage({ account_id = DEFAULT_ACCOUNT_ID, conversation_id, message_id }) {
  try {
    const result = await cwRequest('delete', `/api/v1/accounts/${account_id}/conversations/${conversation_id}/messages/${message_id}`);
    console.log(`[deleteMessage] Deleted message ${message_id}`);
    return result;
  } catch (e) {
    console.error(`[deleteMessage] Failed to delete ${message_id}:`, e?.message);
    throw e;
  }
}
/**
 * ========== chatwoot.js 新增函数 ==========
 * 将以下代码添加到 chatwoot.js 文件末尾（在 module.exports 之前）
 */

// ========== 批量查询消息 ==========
/**
 * 批量查询指定时间范围内的消息
 * @param {Object} options
 * @param {number} options.account_id - 账户 ID
 * @param {number} options.conversation_id - 会话 ID
 * @param {string} options.after - 开始时间 ISO 格式
 * @param {string} options.before - 结束时间 ISO 格式
 * @returns {Promise<Array>} 消息数组 [{id, content, created_at, created_at_unix, message_type, sender_type, source_id}...]
 */
async function batchQueryMessages({
                                    account_id = DEFAULT_ACCOUNT_ID,
                                    conversation_id,
                                    after,
                                    before
                                  }) {
  console.log(`[batchQueryMessages] Start: conv=${conversation_id}, after=${after}, before=${before}`);

  try {
    const result = await cwRequest('post', `/api/v1/accounts/${account_id}/conversations/${conversation_id}/messages/batch_query`, {
      data: { after, before }
    });

    const messages = result?.messages || [];
    console.log(`[batchQueryMessages] Got ${messages.length} messages`);

    return messages;
  } catch (e) {
    console.error(`[batchQueryMessages] Error:`, e?.message);

    // 如果新 API 不存在，降级到旧方法
    if (e?.response?.status === 404 || e?.message?.includes('404')) {
      console.log(`[batchQueryMessages] Fallback to getConversationMessages`);
      return await getConversationMessages({
        account_id,
        conversation_id,
        after: after ? new Date(after).getTime() : null,
        before: before ? new Date(before).getTime() : null
      });
    }

    throw e;
  }
}

// ========== 批量删除消息 ==========
/**
 * 批量删除消息（支持 ID 列表或时间范围）
 * @param {Object} options
 * @param {number} options.account_id - 账户 ID
 * @param {number} options.conversation_id - 会话 ID
 * @param {Array<number>} options.message_ids - 消息 ID 数组（可选）
 * @param {string} options.after - 开始时间 ISO 格式（可选）
 * @param {string} options.before - 结束时间 ISO 格式（可选）
 * @param {boolean} options.hard_delete - 是否硬删除（默认软删除）
 * @returns {Promise<Object>} { ok, deleted, failed, errors, total_found }
 */
async function batchDeleteMessages({
                                     account_id = DEFAULT_ACCOUNT_ID,
                                     conversation_id,
                                     message_ids = null,
                                     after = null,
                                     before = null,
                                     hard_delete = false
                                   }) {
  console.log(`[batchDeleteMessages] Start: conv=${conversation_id}, ids=${message_ids?.length || 0}, after=${after}, before=${before}`);

  try {
    const result = await cwRequest('post', `/api/v1/accounts/${account_id}/conversations/${conversation_id}/messages/batch_destroy`, {
      data: {
        message_ids,
        after,
        before,
        hard_delete
      }
    });

    console.log(`[batchDeleteMessages] Result: deleted=${result?.deleted}, failed=${result?.failed}`);
    return result;
  } catch (e) {
    console.error(`[batchDeleteMessages] Error:`, e?.message);

    // 如果新 API 不存在，降级到逐条删除
    if (e?.response?.status === 404 || e?.message?.includes('404')) {
      console.log(`[batchDeleteMessages] Fallback to single delete`);
      return await fallbackBatchDelete({
        account_id,
        conversation_id,
        message_ids,
        after,
        before
      });
    }

    throw e;
  }
}

/**
 * 降级批量删除（逐条删除）
 */
async function fallbackBatchDelete({
                                     account_id,
                                     conversation_id,
                                     message_ids,
                                     after,
                                     before
                                   }) {
  let messages = [];

  // 如果提供了 ID 列表
  if (message_ids && message_ids.length > 0) {
    messages = message_ids.map(id => ({ id }));
  }
  // 否则按时间范围查询
  else if (after || before) {
    const allMessages = await getConversationMessages({
      account_id,
      conversation_id,
      after: after ? new Date(after).getTime() : null,
      before: before ? new Date(before).getTime() : null
    });
    messages = allMessages;
  }

  let deleted = 0;
  let failed = 0;
  const errors = [];

  for (const msg of messages) {
    try {
      await deleteMessage({
        account_id,
        conversation_id,
        message_id: msg.id
      });
      deleted++;
    } catch (e) {
      failed++;
      errors.push({ id: msg.id, error: e?.message });
    }
    // 小延迟避免请求过快
    await new Promise(r => setTimeout(r, 30));
  }

  return {
    ok: true,
    deleted,
    failed,
    errors,
    total_found: messages.length
  };
}


// ========== 消息对齐辅助函数 ==========
/**
 * 在消息数组中查找匹配的消息（按内容和时间容差）
 * @param {Array} messages - 消息数组
 * @param {string} content - 要查找的内容
 * @param {number} targetTimestamp - 目标时间戳（秒）
 * @param {number} toleranceSeconds - 时间容差（秒），默认 60
 * @returns {Object|null} 找到的消息或 null
 */
function findMessageByContentAndTime(messages, content, targetTimestamp, toleranceSeconds = 60) {
  if (!messages || messages.length === 0) return null;
  if (!content) return null;

  const normalizedContent = String(content).trim().toLowerCase();
  const minTs = targetTimestamp - toleranceSeconds;
  const maxTs = targetTimestamp + toleranceSeconds;

  for (const msg of messages) {
    const msgContent = String(msg.content || msg.body || '').trim().toLowerCase();
    const msgTs = msg.created_at_unix || msg.timestamp || Math.floor(new Date(msg.created_at).getTime() / 1000);

    // 时间在容差范围内
    if (msgTs >= minTs && msgTs <= maxTs) {
      // 内容匹配（支持部分匹配）
      if (msgContent === normalizedContent ||
          msgContent.includes(normalizedContent) ||
          normalizedContent.includes(msgContent)) {
        return msg;
      }
    }
  }

  return null;
}

/**
 * 计算两组消息的时间偏移量
 * @param {Array} cwMessages - Chatwoot 消息
 * @param {Array} waMessages - WhatsApp 消息
 * @returns {Object} { offset: 秒数, confidence: 置信度 }
 */
function calculateTimeOffset(cwMessages, waMessages) {
  if (!cwMessages?.length || !waMessages?.length) {
    return { offset: 0, confidence: 0 };
  }

  const offsets = [];

  // 尝试匹配前 10 条消息
  const sampleSize = Math.min(10, waMessages.length);
  for (let i = 0; i < sampleSize; i++) {
    const waMsg = waMessages[i];
    const waTs = waMsg.timestamp;
    const waContent = waMsg.body || '';

    if (!waContent) continue;

    // 在 CW 消息中查找匹配
    const matchedCw = findMessageByContentAndTime(cwMessages, waContent, waTs, 120);
    if (matchedCw) {
      const cwTs = matchedCw.created_at_unix || Math.floor(new Date(matchedCw.created_at).getTime() / 1000);
      offsets.push(cwTs - waTs);
    }
  }

  if (offsets.length === 0) {
    return { offset: 0, confidence: 0 };
  }

  // 取中位数作为偏移量
  offsets.sort((a, b) => a - b);
  const medianOffset = offsets[Math.floor(offsets.length / 2)];
  const confidence = offsets.length / sampleSize;

  return {
    offset: medianOffset,
    confidence,
    samples: offsets.length
  };
}

/**
 * 批量创建消息
 * @param {Object} options
 * @param {number} options.account_id - 账户 ID
 * @param {number} options.conversation_id - 会话 ID
 * @param {Array} options.messages - 消息数组
 * @returns {Promise<Object>}
 */
async function batchCreateMessages({
                                     account_id = DEFAULT_ACCOUNT_ID,
                                     conversation_id,
                                     messages
                                   }) {
  if (!messages || messages.length === 0) {
    return { ok: true, created: 0, failed: 0, skipped: 0, created_ids: [] };
  }

  console.log(`[batchCreateMessages] Start: conv=${conversation_id}, count=${messages.length}`);

  try {
    // 尝试使用批量 API
    const result = await request('post',
        `/api/v1/accounts/${account_id}/conversations/${conversation_id}/messages/batch_create`,
        { messages }
    );

    console.log(`[batchCreateMessages] Success: created=${result?.created}, skipped=${result?.skipped}, failed=${result?.failed}`);
    return result;

  } catch (e) {
    const status = e?.response?.status || e?.status;

    // 如果批量 API 不存在（404），降级到逐条创建
    if (status === 404) {
      console.log(`[batchCreateMessages] API not found (404), fallback to single create`);
      return await fallbackBatchCreate({
        account_id,
        conversation_id,
        messages
      });
    }

    console.error(`[batchCreateMessages] Error: ${e?.message}`);
    throw e;
  }
}

/**
 * 降级批量创建（逐条）
 */
async function fallbackBatchCreate({
                                     account_id = DEFAULT_ACCOUNT_ID,
                                     conversation_id,
                                     messages
                                   }) {
  let created = 0;
  let failed = 0;
  let skipped = 0;
  const errors = [];
  const created_ids = [];

  for (const msg of messages) {
    try {
      const isIncoming = msg.message_type === 0 || msg.message_type === 'incoming';

      let result;
      if (isIncoming) {
        result = await createIncomingMessage({
          account_id,
          conversation_id,
          content: msg.content || '',
          source_id: msg.source_id,
          private: msg.private || false,
          content_attributes: msg.content_attributes || {},
          attachments: msg.attachments || []
        });
      } else {
        result = await createOutgoingMessage({
          account_id,
          conversation_id,
          content: msg.content || '',
          source_id: msg.source_id,
          private: msg.private || false,
          content_attributes: msg.content_attributes || {},
          attachments: msg.attachments || []
        });
      }

      if (result?.id) {
        created++;
        created_ids.push(result.id);
      }
    } catch (e) {
      const errMsg = e?.message || '';

      // 重复消息不算失败
      if (errMsg.includes('duplicate') ||
          errMsg.includes('same_second') ||
          errMsg.includes('record_invalid') ||
          errMsg.includes('Duplicate message')) {
        skipped++;
      } else {
        failed++;
        errors.push({ source_id: msg.source_id, error: errMsg });
      }
    }

    // 小延迟
    await new Promise(r => setTimeout(r, 50));
  }

  return {
    ok: true,
    created,
    failed,
    skipped,
    created_ids,
    failed_details: errors
  };
}





/**
 * 分批同步消息（推荐用于大量消息）
 * @param {Object} options
 * @param {number} options.account_id
 * @param {number} options.conversation_id
 * @param {Array} options.messages - 所有要同步的消息
 * @param {number} options.batchSize - 每批数量，默认 50
 * @param {number} options.delayMs - 批次间延迟，默认 200ms
 * @param {Function} options.onProgress - 进度回调 (current, total)
 */
async function syncMessagesInBatches({
                                       account_id = DEFAULT_ACCOUNT_ID,
                                       conversation_id,
                                       messages,
                                       batchSize = 50,
                                       delayMs = 200,
                                       onProgress = null
                                     }) {
  if (!messages || messages.length === 0) {
    return { ok: true, total: 0, created: 0, failed: 0, skipped: 0 };
  }

  const total = messages.length;
  let totalCreated = 0;
  let totalFailed = 0;
  let totalSkipped = 0;
  const allErrors = [];

  console.log(`[syncMessagesInBatches] Start: conv=${conversation_id}, total=${total}, batchSize=${batchSize}`);

  for (let i = 0; i < total; i += batchSize) {
    const batch = messages.slice(i, i + batchSize);
    const batchNum = Math.floor(i / batchSize) + 1;
    const totalBatches = Math.ceil(total / batchSize);

    console.log(`[syncMessagesInBatches] Processing batch ${batchNum}/${totalBatches}`);

    try {
      const result = await batchCreateMessages({
        account_id,
        conversation_id,
        messages: batch
      });

      totalCreated += result.created || 0;
      totalFailed += result.failed || 0;
      totalSkipped += result.skipped || 0;

      if (result.failed_details) {
        allErrors.push(...result.failed_details);
      }
    } catch (e) {
      console.error(`[syncMessagesInBatches] Batch ${batchNum} failed: ${e?.message}`);
      totalFailed += batch.length;
      allErrors.push({ batch: batchNum, error: e?.message });
    }

    // 进度回调
    if (onProgress) {
      onProgress(Math.min(i + batchSize, total), total);
    }

    // 批次间延迟
    if (i + batchSize < total) {
      await new Promise(r => setTimeout(r, delayMs));
    }
  }

  console.log(`[syncMessagesInBatches] Complete: created=${totalCreated}, skipped=${totalSkipped}, failed=${totalFailed}`);

  return {
    ok: true,
    total,
    created: totalCreated,
    failed: totalFailed,
    skipped: totalSkipped,
    errors: allErrors
  };
}


// ============================================================
// 消息去重检查函数（用于多端同步）
// ============================================================

/**
 * 检查消息是否已存在（通过 source_id / wa_message_id）
 * @param {Object} options
 * @param {string} options.account_id
 * @param {string} options.conversation_id
 * @param {string} options.source_id - WhatsApp 消息ID
 * @param {number} options.limit - 检查最近多少条消息，默认50
 * @returns {Promise<boolean>}
 */
async function checkMessageExists({ account_id, conversation_id, source_id, limit = 50 }) {
  if (!source_id || !conversation_id) return false;

  try {
    const res = await cwRequest('get',
        `/api/v1/accounts/${account_id}/conversations/${conversation_id}/messages`
    );

    const messages = res?.payload || res || [];

    for (const m of messages.slice(0, limit)) {
      if (m.source_id === source_id) return true;
      if (m.content_attributes?.wa_message_id === source_id) return true;
    }

    return false;
  } catch (e) {
    console.warn('[checkMessageExists] Error:', e.message);
    return false;
  }
}

/**
 * 通过 source_id 查找消息详情
 * @param {Object} options
 * @returns {Promise<Object|null>}
 */
async function findMessageBySourceId({ account_id, conversation_id, source_id }) {
  if (!source_id || !conversation_id) return null;

  try {
    const res = await cwRequest('get',
        `/api/v1/accounts/${account_id}/conversations/${conversation_id}/messages`
    );

    const messages = res?.payload || res || [];

    return messages.find(m =>
        m.source_id === source_id ||
        m.content_attributes?.wa_message_id === source_id
    ) || null;
  } catch (e) {
    return null;
  }
}

/**
 * 批量检查多个消息是否存在
 * @param {Object} options
 * @returns {Promise<Set<string>>} 已存在的消息ID集合
 */
async function checkMessagesExist({ account_id, conversation_id, source_ids }) {
  if (!source_ids?.length || !conversation_id) return new Set();

  const existingIds = new Set();

  try {
    const res = await cwRequest('get',
        `/api/v1/accounts/${account_id}/conversations/${conversation_id}/messages`
    );

    const messages = res?.payload || res || [];
    const existingSourceIds = new Set();

    for (const m of messages) {
      if (m.source_id) existingSourceIds.add(m.source_id);
      if (m.content_attributes?.wa_message_id) {
        existingSourceIds.add(m.content_attributes.wa_message_id);
      }
    }

    for (const sourceId of source_ids) {
      if (existingSourceIds.has(sourceId)) {
        existingIds.add(sourceId);
      }
    }
  } catch (e) {
    console.warn('[checkMessagesExist] Error:', e.message);
  }

  return existingIds;
}

module.exports = {
  listAccounts,
  listInboxes,
  batchCreateMessages,
  syncMessagesInBatches,
  getInboxIdByIdentifier,
  createContactNote,
  updateContactNote,
  ensureContact,
  ensureConversation,
  getConversationDetails,
  request,
  createIncomingMessage,
  batchQueryMessages,
  batchDeleteMessages,
  findMessageByContentAndTime,
  calculateTimeOffset,
  // ★★★ 新增：缓存清除函数 ★★★
  clearContactCache,
  clearConversationCache,
  clearAllCacheForDigits,
  getConversationMessages,
  createOutgoingMessage,
  deleteMessage,
  // 新增：去重检查函数
  checkMessageExists,
  findMessageBySourceId,
  checkMessagesExist,
};