// collector/src/chatwoot.js
const axios = require('axios');
const FormData = require('form-data');
const path = require('path');
const { CFG } = require('./config');

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
function buildDisplayName(sessionId, rawName, phone_e164, identifier) {
  const rhs = (rawName && rawName.trim()) || phone_e164 || identifier || 'WA User';
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
  return cwRequest('post', `/api/v1/accounts/${account_id}/contacts`, { data });
}

async function updateContact({ account_id = DEFAULT_ACCOUNT_ID, contact_id, patch }) {
  return cwRequest('patch', `/api/v1/accounts/${account_id}/contacts/${contact_id}`, { data: patch });
}

async function ensureContact({ account_id = DEFAULT_ACCOUNT_ID, rawPhone, rawName, sessionId }) {
  // 归属主键：必须 phone + sessionId 同时相同（你现行规则）
  const digits = (String(rawPhone || '').match(/\d+/g) || []).join('');
  const identifier = `wa:${sessionId || 'default'}:${digits}`;
  const phone_e164 = normalizeE164(rawPhone, rawName); // 用于联系人 phone_number

  // 先按 identifier 精确找
  let contact = await searchContact({ account_id, identifier });

  // 按你的显示名格式：sessionId>name（保留）
  const wantName = buildDisplayName(sessionId, rawName, phone_e164, identifier);

  // 仅保存 sessionId 到自定义属性（其它信息不放这儿）
  const wantAttrs = { session_id: sessionId || null };

  if (!contact) {
    // 先尝试“带手机号”创建；若手机号已被别的联系人占用，则降级为不带手机号创建
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
      const dupPhone = msg.includes('phone number') && msg.includes('taken');
      if (!dupPhone) throw e;
      // 重试：不写 phone_number，避免 422
      contact = await createContact({
        account_id,
        name: wantName,
        identifier,
        phone_e164: undefined,
        custom_attributes: wantAttrs,
        withPhone: false
      });
    }
  } else {
    // 需要时做 patch：name / phone_number（仅当不会与别人冲突）/ custom_attributes
    const patch = {};
    const pure = (s) => (s || '').replace(/[^\+\d]/g, '');

    if (contact.name !== wantName) patch.name = wantName;

    if (phone_e164) {
      const owner = await findContactByPhone({ account_id, phone_e164 });
      if (!owner || owner.id === contact.id) {  // 没被别人占
        if (pure(contact.phone_number) !== pure(phone_e164)) patch.phone_number = phone_e164;
      }
    }

    if ((contact.custom_attributes?.session_id || null) !== (sessionId || null)) {
      patch.custom_attributes = { ...(contact.custom_attributes || {}), session_id: sessionId || null };
    }

    if (Object.keys(patch).length) {
      try { contact = await updateContact({ account_id, contact_id: contact.id, patch }); } catch {}
    }
  }

  return contact;
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

// ---------- 会话 ----------
async function getContactConversations({ account_id = DEFAULT_ACCOUNT_ID, contact_id }) {
  return cwRequest('get', `/api/v1/accounts/${account_id}/contacts/${contact_id}/conversations`);
}

async function createConversation({ account_id = DEFAULT_ACCOUNT_ID, inbox_id, contact_id, message }) {
  const data = { inbox_id, contact_id };
  // 关键：不再传 source_id，避免 “source_id should be unique”
  if (message) data.message = message;
  return cwRequest('post', `/api/v1/accounts/${account_id}/conversations`, { data });
}

async function ensureConversation({ account_id = DEFAULT_ACCOUNT_ID, contact_id, inbox_id }) {
  const list = await getContactConversations({ account_id, contact_id });
  const items = Array.isArray(list?.payload) ? list.payload : (list || []);
  // 优先复用该 inbox 下“未解决”的会话
  const open = items.find(c => Number(c.inbox_id) === Number(inbox_id) && c.status !== 'resolved');
  if (open) return open;
  // 找不到就创建
  return createConversation({ account_id, inbox_id, contact_id });
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



// ---------- 导出 ----------
module.exports = {
  // 基础
  listAccounts,
  listInboxes,
  getInboxIdByIdentifier,
  createContactNote,
  updateContactNote,
  // 联系人
  ensureContact,

  // 会话
  ensureConversation,
  getConversationDetails,

  // 消息
  createIncomingMessage,
};
