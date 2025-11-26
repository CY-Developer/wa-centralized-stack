// sender/src/webhook.js  —— 全量替换为本版本
const express = require('express');
const bodyParser = require('body-parser');
const morgan = require('morgan');

const USE_QUEUE = process.env.SENDER_USE_QUEUE !== '0'; // 默认用队列；设置 SENDER_USE_QUEUE=0 可直发
const REDIS_URL = process.env.REDIS_URL || '';

let addJobOrDeliver;
if (USE_QUEUE && REDIS_URL) {
  // 用 BullMQ
  const { Queue } = require('bullmq');
  const sendQueue = new Queue('cw-send', {
    connection: { url: REDIS_URL }
  });

  addJobOrDeliver = async (job) => {
    await sendQueue.add('send', job, {
      removeOnComplete: true,
      removeOnFail: false,
      attempts: 3,
      backoff: { type: 'exponential', delay: 2000 }
    });
  };
} else {
  // 直发（本地/无 redis 时）
  const { deliver } = require('./worker');
  addJobOrDeliver = async (job) => {
    await deliver(job);
  };
}

function pickFirstAttachment(payload = {}) {
  const a = Array.isArray(payload.attachments) ? payload.attachments : [];
  if (a.length === 0) return null;
  // Chatwoot 通常提供 data_url（CDN 可访问），file_type（MIME），file_name
  const att = a[0];
  return {
    url: att.data_url || att.download_url || null,
    mime: att.file_type || null,
    name: att.file_name || null
  };
}

function inferTypeByMime(mime = '') {
  const m = (mime || '').toLowerCase();
  if (m.startsWith('image/')) return 'image';
  if (m.startsWith('video/')) return 'video';
  if (m.startsWith('audio/')) return 'audio';
  // 贴纸/文档等：按 document 走
  return 'document';
}

// 多种来源上拿“收件人手机号”（ensureContact/ensureConversation 时务必把手机号放在 contact.identifier 或 custom_attributes.phone）
function resolveToPhone(payload = {}) {
  // 常见位置（API Channel）
  // 1) conversation.contact_inbox.source_id
  let v =
      payload?.conversation?.contact_inbox?.source_id ||
      payload?.conversation?.meta?.sender?.identifier ||
      payload?.conversation?.contact?.phone_number ||
      payload?.sender?.phone_number ||
      payload?.custom_attributes?.phone ||
      '';

  if (typeof v === 'string') v = v.replace(/[^\d]/g, '');
  return v || null;
}

function resolveSessionId(payload = {}) {
  // 在 ensureConversation 时把 sessionId 存在 conversation.custom_attributes.sessionId（或 inbox 名称映射）
  return (
      payload?.conversation?.custom_attributes?.sessionId ||
      process.env.DEFAULT_SESSION_ID || // 兜底
      null
  );
}

function isOutgoingToCustomer(payload = {}) {
  // Chatwoot webhook: message_type === 'outgoing' 表示坐席发给客户
  // 也可判断 'conversation' 的 inbox/source 是否属于我们
  return payload?.message_type === 'outgoing';
}

function buildJobFromWebhook(payload = {}) {
  const toPhone = resolveToPhone(payload);
  const sessionId = resolveSessionId(payload);
  if (!toPhone || !sessionId) {
    throw new Error('cannot resolve toPhone/sessionId from webhook payload');
  }

  const text = payload?.content || '';
  const att = pickFirstAttachment(payload);

  if (!att) {
    // 纯文本
    return {
      sessionId,
      to: toPhone,
      type: 'text',
      text
    };
  }

  // 有附件：按 MIME 推导
  const mediaType = inferTypeByMime(att.mime);
  return {
    sessionId,
    to: toPhone,
    type: mediaType,             // image|video|audio|document
    text,                        // 走 caption
    fileUrl: att.url
  };
}

function startServer() {
  const PORT = process.env.SENDER_WEBHOOK_PORT || 7002;
  const app = express();
  app.use(bodyParser.json({ limit: '10mb' }));
  app.use(morgan('tiny'));

  // Chatwoot Webhook 接收
  app.post('/webhook/chatwoot', async (req, res) => {
    try {
      const payload = req.body || {};
      if (!isOutgoingToCustomer(payload)) {
        // 不是“坐席→客户”的消息，忽略（例如 incoming）
        return res.json({ ok: true, skipped: true });
      }

      const job = buildJobFromWebhook(payload);
      await addJobOrDeliver(job);
      res.json({ ok: true, queued: !!(USE_QUEUE && REDIS_URL), job });
    } catch (e) {
      console.error('[webhook] error:', e?.message || e);
      res.status(400).json({ ok: false, error: e?.message || String(e) });
    }
  });

  // 健康检查
  app.get('/health', (_req, res) => res.json({
    ok: true,
    queue: !!(USE_QUEUE && REDIS_URL)
  }));

  app.listen(PORT, () => console.log(`[sender-webhook] up on :${PORT}`));
}

module.exports = { startServer };
