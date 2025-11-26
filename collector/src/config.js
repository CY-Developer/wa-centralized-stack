require('../../common/src/env');
const { env } = require('../../common/src/env');
const { logger } = require('@wa/common/src/logger');

function loadProfiles() {
  try {
    const arr = JSON.parse(process.env.PROFILES || '[]');
    if (!Array.isArray(arr)) throw new Error('PROFILES must be array');
    return arr;
  } catch (e) {
    logger.error({ err: e }, 'Failed to parse PROFILES JSON');
    return [];
  }
}

const CFG = {
  profiles: loadProfiles(),
  chatwoot: {
    baseURL: process.env.CHATWOOT_BASE_URL,
    token: process.env.CHATWOOT_API_TOKEN,
    accountId: process.env.CHATWOOT_ACCOUNT_ID,
    inboxId: process.env.CHATWOOT_INBOX_ID || null,                 // 可选：直接给 ID
    inboxIdentifier: process.env.CHATWOOT_INBOX_IDENTIFIER || null, // 或者给 identifier
    MEDIA_DIR: process.env.MEDIA_DIR || 'data/media',
    PUBLIC_BASE_URL: process.env.PUBLIC_BASE_URL || 'http://127.0.0.1:7001',
    WA_BRIDGE_URL: process.env.WA_BRIDGE_URL || process.env.MANAGER_BASE || '',
  }
};

module.exports = { CFG };
