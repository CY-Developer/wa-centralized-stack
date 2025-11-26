// sender/src/worker.js —— 用 HTTP 调本地 manager（不再操纵 WhatsApp）
const axios = require('axios');

const MANAGER_BASE = process.env.MANAGER_BASE || 'http://127.0.0.1:5010';

/**
 * job: {
 *   sessionId: 'k165wa1x',
 *   to: '447123456789',
 *   type: 'text'|'image'|'video'|'audio'|'document',
 *   fileUrl?: 'http://.../xxx.jpg'  // 或 file:// 路径（建议先由 manager 支持本地路径）
 * }
 */
async function deliver(job){
  const { sessionId, to, type } = job;
  if (!sessionId || !to || !type) throw new Error('bad job');

  if (type === 'text'){
    await axios.post(`${MANAGER_BASE}/send`, {
      sessionId, phone: to, text: job.text || ''
    }, { timeout: 15000 });
    return;
  }

  // 统一媒体转发（要求 manager 已提供 /send-media）
  await axios.post(`${MANAGER_BASE}/send-media`, {
    sessionId,
    phone: to,
    type,                  // image|video|audio|document
    source: job.fileUrl,   // 可为 http(s) 或 file://
    caption: job.text || ''
  }, { timeout: 30000 });
}

module.exports = { deliver };
