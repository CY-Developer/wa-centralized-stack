const crypto = require('crypto');

function sleep(ms) {
  return new Promise(res => setTimeout(res, ms));
}

function jitter(minMs, maxMs) {
  const span = Math.max(0, maxMs - minMs);
  return minMs + Math.floor(Math.random() * (span + 1));
}

function normalizePhone(raw, defaultDialCode = '+65') {
  if (!raw) return null;
  let s = String(raw).replace(/\D/g, '');
  if (s.startsWith('0') && defaultDialCode) {
    return defaultDialCode.replace(/\D/g,'') + s.slice(1);
  }
  return s;
}

function uuid() {
  return crypto.randomBytes(16).toString('hex');
}

module.exports = { sleep, jitter, normalizePhone, uuid };
