require('../../common/src/env');
const { env } = require('../../common/src/env');
const { logger } = require('@wa/common/src/logger');

function loadProfiles() {
  try {
    const arr = JSON.parse(process.env.PROFILES || '[]');
    return Array.isArray(arr) ? arr : [];
  } catch (e) {
    logger.error({ err: e }, 'Invalid PROFILES JSON');
    return [];
  }
}

const CFG = {
  port: Number(process.env.SENDER_PORT || 3001),
  redisUrl: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
  profiles: loadProfiles(),
  limits: {
    minTime: Number(process.env.RATE_MIN_TIME_MS || 3000),
    perMinute: Number(process.env.RATE_MAX_PER_MINUTE || 18),
    typingMin: Number(process.env.TYPING_MIN_MS || 60),
    typingMax: Number(process.env.TYPING_MAX_MS || 180),
    gapMin: Number(process.env.MESSAGE_GAP_MIN_MS || 2000),
    gapMax: Number(process.env.MESSAGE_GAP_MAX_MS || 5000),
    readDelay: Number(process.env.READ_RECEIPT_DELAY_MS || 800)
  }
};

module.exports = { CFG };
