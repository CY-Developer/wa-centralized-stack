require('../../common/src/env'); // 仍然读取 .env
const express = require('express');
const bodyParser = require('body-parser');
const morgan = require('morgan');
const IORedis = require('ioredis');
const fs = require('fs');
const path = require('path');
const axios = require('axios');

const {CFG} = require('./config');
const cw = require('./chatwoot'); // <-- 修正后有 getInboxIdByIdentifier 等

// ====== 基础配置 ======
const MEDIA_DIR = path.resolve(CFG.chatwoot.MEDIA_DIR || 'data/media');

const CHATWOOT_ACCOUNT_ID = CFG.chatwoot.accountId || '';
const CHATWOOT_INBOX_ID = CFG.chatwoot.inboxId || null;
const CHATWOOT_INBOX_IDENTIFIER = CFG.chatwoot.inboxIdentifier || null;

// ★★★ 新增：Session -> Inbox 映射 ★★★
// 格式: CHATWOOT_INBOX_MAP=sessionId1:inboxId1,sessionId2:inboxId2
// 例如: CHATWOOT_INBOX_MAP=k17se6o0:3,k16f9wid:4
const CHATWOOT_INBOX_MAP_RAW = process.env.CHATWOOT_INBOX_MAP || '';
const CHATWOOT_INBOX_MAP = new Map();
if (CHATWOOT_INBOX_MAP_RAW) {
    CHATWOOT_INBOX_MAP_RAW.split(',').forEach(pair => {
        const [sessionId, inboxId] = pair.trim().split(':');
        if (sessionId && inboxId && /^\d+$/.test(inboxId)) {
            CHATWOOT_INBOX_MAP.set(sessionId.trim(), Number(inboxId));
        }
    });
    if (CHATWOOT_INBOX_MAP.size > 0) {
        console.log('[CONFIG] Session->Inbox mapping:', Object.fromEntries(CHATWOOT_INBOX_MAP));
    }
}

// ★★★ 支持多种环境变量名称 ★★★
const CHATWOOT_TOKEN = process.env.CHATWOOT_API_ACCESS_TOKEN || process.env.CHATWOOT_API_TOKEN || CFG.chatwoot.token || '';

const WA_BRIDGE_URL = CFG.chatwoot.WA_BRIDGE_URL || '';
const WA_BRIDGE_TOKEN = process.env.WA_BRIDGE_TOKEN || '';
const WA_DEFAULT_SESSION = process.env.SESSIONS || '';

const PORT = Number(process.env.COLLECTOR_PORT || 7001);
const INGEST_TOKEN = process.env.COLLECTOR_INGEST_TOKEN || process.env.API_TOKEN || '';

const REDIS_URL = process.env.REDIS_URL || '';
const redis = REDIS_URL ? new IORedis(REDIS_URL, {maxRetriesPerRequest: null}) : null;

function ensureDir(p) {
    if (!fs.existsSync(p)) fs.mkdirSync(p, {recursive: true});
}
// ===== 新增：LID 检测函数 =====
function extractPhoneFromName(name) {
    if (!name) return null;
    const match = String(name).match(/\+?[\d\s\-]{7,}/);
    if (match) {
        const digits = match[0].replace(/[^\d]/g, '');
        if (digits.length >= 7 && digits.length <= 15) return digits;
    }
    return null;
}

function isLidFormat(jidOrPhone) {
    const s = String(jidOrPhone || '');
    if (/@lid$/i.test(s)) return true;
    const digits = s.replace(/\D/g, '');
    if (/^6789\d{10,}/.test(digits)) return true;
    return false;
}
ensureDir(MEDIA_DIR);
const COLLECTOR_LOG = path.resolve(process.env.COLLECTOR_LOG || path.join(__dirname, 'collector.log'));
const collectorStream = fs.createWriteStream(COLLECTOR_LOG, {flags: 'a'});


function logToCollector(tag, data) {
    try {
        const line = `[${new Date().toISOString()}] ${tag} ${data ? JSON.stringify(data) : ''}\n`;
        collectorStream.write(line);
    } catch (e) {
        console.warn('logToCollector failed:', e.message);
    }
    // 同步打一份到控制台，方便本地开发
    console.log(tag, data || '');
}
const syncingConversations = new Map();

// ★★★ V5.3新增：Redis消息ID映射功能 ★★★
// 解决Chatwoot不支持update_source_id API的问题
// 正向映射: cw:msgmap:{conversation_id}:{wa_message_id} -> {cw_message_id} (入站引用查找)
// 反向映射: cw:msgmap:rev:{conversation_id}:{cw_message_id} -> {wa_message_id} (出站引用查找)
// 过期时间: 30天（足够长时间支持引用历史消息）
const MESSAGE_MAP_TTL = 30 * 24 * 3600; // 30天

/**
 * 保存消息ID映射到Redis（正向+反向）
 * @param {number} conversation_id - Chatwoot会话ID
 * @param {string} waMessageId - WhatsApp消息ID
 * @param {number} cwMessageId - Chatwoot消息ID
 */
/**
 * ★★★ V5.3.7新增：提取消息ID后缀（唯一标识部分） ★★★
 * WhatsApp消息ID格式: true/false_xxx@xxx_SUFFIX
 * 同一条消息在发送方和接收方的后缀相同，但前缀不同
 * 例如：
 * - 发送方: true_17932042137754@lid_3EB0A8ED271F02A0B3C23B
 * - 接收方: true_85256018957@c.us_3EB0A8ED271F02A0B3C23B
 * 后缀都是: 3EB0A8ED271F02A0B3C23B
 */
function extractMessageIdSuffix(messageId) {
    if (!messageId) return null;
    // 格式: true_xxx@lid_3EB0A8ED271F02A0B3C23B
    // 或: false_xxx@c.us_3EB0A8ED271F02A0B3C23B
    // 提取最后一个下划线后的部分（后缀）
    const lastUnderscoreIndex = messageId.lastIndexOf('_');
    if (lastUnderscoreIndex > 0 && lastUnderscoreIndex < messageId.length - 1) {
        return messageId.substring(lastUnderscoreIndex + 1);
    }
    return null;
}

/**
 * ★★★ V5.3.7改进：保存消息ID映射（包含后缀索引） ★★★
 */
async function saveMessageMapping(conversation_id, waMessageId, cwMessageId) {
    if (!redis || !conversation_id || !waMessageId || !cwMessageId) return false;
    try {
        const pipeline = redis.pipeline();
        // 1. 正向映射: wa -> cw
        pipeline.set(`cw:msgmap:${conversation_id}:${waMessageId}`, String(cwMessageId), 'EX', MESSAGE_MAP_TTL);
        // 2. 反向映射: cw -> wa
        pipeline.set(`cw:msgmap:rev:${conversation_id}:${cwMessageId}`, waMessageId, 'EX', MESSAGE_MAP_TTL);

        // 3. ★★★ V5.3.7新增：后缀索引 ★★★
        // 同一条消息在不同端ID前缀不同，但后缀相同
        const suffix = extractMessageIdSuffix(waMessageId);
        if (suffix && suffix.length >= 10) {
            pipeline.set(`cw:msgmap:suffix:${conversation_id}:${suffix}`, String(cwMessageId), 'EX', MESSAGE_MAP_TTL);
            // 4. ★★★ V5.3.13新增：全局后缀索引（用于撤回查找）★★★
            pipeline.set(`cw:msgmap:suffix_global:${suffix}`, `${conversation_id}:${cwMessageId}`, 'EX', MESSAGE_MAP_TTL);
        }

        await pipeline.exec();
        console.log(`[MSG_MAP] Saved: wa=${waMessageId.substring(0, 35)} <-> cw=${cwMessageId}${suffix ? `, suffix=${suffix.substring(0, 15)}` : ''}`);
        return true;
    } catch (e) {
        console.warn(`[MSG_MAP] Save failed: ${e.message}`);
        return false;
    }
}

/**
 * 从Redis查询Chatwoot消息ID（正向）
 * @param {number} conversation_id - Chatwoot会话ID
 * @param {string} waMessageId - WhatsApp消息ID
 * @returns {number|null} - Chatwoot消息ID或null
 */
async function getMessageMapping(conversation_id, waMessageId) {
    if (!redis || !conversation_id || !waMessageId) return null;
    try {
        const key = `cw:msgmap:${conversation_id}:${waMessageId}`;
        const cwMessageId = await redis.get(key);
        if (cwMessageId) {
            console.log(`[MSG_MAP] Found in Redis: wa=${waMessageId.substring(0, 35)} -> cw=${cwMessageId}`);
            return Number(cwMessageId);
        }
        return null;
    } catch (e) {
        console.warn(`[MSG_MAP] Get failed: ${e.message}`);
        return null;
    }
}

/**
 * ★★★ V5.3.7改进：通过source_id在Chatwoot中查找消息ID ★★★
 * 支持后缀匹配，解决发送方/接收方ID不同的问题
 * @param {number} conversation_id - Chatwoot会话ID
 * @param {string} waMessageId - WhatsApp消息ID (即source_id)
 * @returns {number|null} - Chatwoot消息ID或null
 */
async function findCwMessageIdBySourceId(conversation_id, waMessageId) {
    // ★★★ V5.3.11修复：禁用Chatwoot API查询 ★★★
    // 原因：API分页查询整个会话（1000+条消息）需要60-165秒，
    // 导致消息重复发送问题。
    // 现在只依赖Redis映射，如果Redis没有就返回null（使用文本格式引用）
    console.log(`[MSG_MAP] API lookup disabled (V5.3.11): wa=${waMessageId?.substring(0, 30)}`);
    return null;
}

/**
 * ★★★ V5.3.7改进：综合查找Chatwoot消息ID ★★★
 * 查找顺序：
 * 1. Redis精确匹配（最快）
 * 2. Redis后缀索引（快速，解决ID格式不同问题）
 * 3. Chatwoot API查找（最慢，最后兜底）
 *
 * @param {number} conversation_id - Chatwoot会话ID
 * @param {string} waMessageId - WhatsApp消息ID
 * @returns {number|null} - Chatwoot消息ID或null
 */
async function findCwMessageId(conversation_id, waMessageId) {
    if (!conversation_id || !waMessageId) return null;

    // 1. 先查Redis（精确匹配）
    const fromRedis = await getMessageMapping(conversation_id, waMessageId);
    if (fromRedis) return fromRedis;

    // 2. ★★★ V5.3.7核心改进：使用后缀索引查找 ★★★
    // 同一条消息在不同端ID不同，但后缀相同
    if (redis) {
        const targetSuffix = extractMessageIdSuffix(waMessageId);
        if (targetSuffix && targetSuffix.length >= 10) {
            try {
                // 直接通过后缀索引查找
                const suffixKey = `cw:msgmap:suffix:${conversation_id}:${targetSuffix}`;
                const cwId = await redis.get(suffixKey);
                if (cwId) {
                    console.log(`[MSG_MAP] Found by suffix index: wa=${waMessageId.substring(0, 30)} -> cw=${cwId}, suffix=${targetSuffix.substring(0, 15)}`);
                    // 保存精确映射以加速下次查找
                    await saveMessageMapping(conversation_id, waMessageId, Number(cwId)).catch(() => {});
                    return Number(cwId);
                }
            } catch (e) {
                console.warn(`[MSG_MAP] Suffix lookup failed: ${e.message}`);
            }
        }
    }

    // 3. Redis没有，通过Chatwoot API查找
    return await findCwMessageIdBySourceId(conversation_id, waMessageId);
}

/**
 * 从Redis查询WhatsApp消息ID（反向）
 * @param {number} conversation_id - Chatwoot会话ID
 * @param {number} cwMessageId - Chatwoot消息ID
 * @returns {string|null} - WhatsApp消息ID或null
 */
async function getWaMessageIdByCwId(conversation_id, cwMessageId) {
    if (!redis || !conversation_id || !cwMessageId) return null;
    try {
        const key = `cw:msgmap:rev:${conversation_id}:${cwMessageId}`;
        const waMessageId = await redis.get(key);
        if (waMessageId) {
            console.log(`[MSG_MAP] Found: cw=${cwMessageId} -> wa=${waMessageId.substring(0, 35)}`);
            return waMessageId;
        }
        return null;
    } catch (e) {
        console.warn(`[MSG_MAP] Reverse get failed: ${e.message}`);
        return null;
    }
}

/**
 * ★★★ V5.3.7改进：批量保存消息ID映射（包含后缀索引） ★★★
 * @param {number} conversation_id - Chatwoot会话ID
 * @param {Array} mappings - [{waMessageId, cwMessageId}, ...]
 */
async function batchSaveMessageMappings(conversation_id, mappings) {
    if (!redis || !conversation_id || !mappings?.length) return 0;
    try {
        const pipeline = redis.pipeline();
        let suffixCount = 0;

        for (const { waMessageId, cwMessageId } of mappings) {
            if (waMessageId && cwMessageId) {
                // 1. 正向映射
                pipeline.set(`cw:msgmap:${conversation_id}:${waMessageId}`, String(cwMessageId), 'EX', MESSAGE_MAP_TTL);
                // 2. 反向映射
                pipeline.set(`cw:msgmap:rev:${conversation_id}:${cwMessageId}`, waMessageId, 'EX', MESSAGE_MAP_TTL);

                // 3. ★★★ V5.3.7新增：后缀索引 ★★★
                const suffix = extractMessageIdSuffix(waMessageId);
                if (suffix && suffix.length >= 10) {
                    pipeline.set(`cw:msgmap:suffix:${conversation_id}:${suffix}`, String(cwMessageId), 'EX', MESSAGE_MAP_TTL);
                    // 4. ★★★ V5.3.13新增：全局后缀索引（用于撤回查找）★★★
                    pipeline.set(`cw:msgmap:suffix_global:${suffix}`, `${conversation_id}:${cwMessageId}`, 'EX', MESSAGE_MAP_TTL);
                    suffixCount++;
                }
            }
        }
        await pipeline.exec();
        console.log(`[MSG_MAP] Batch saved ${mappings.length} mappings (${suffixCount} suffixes) for conv=${conversation_id}`);
        return mappings.length;
    } catch (e) {
        console.warn(`[MSG_MAP] Batch save failed: ${e.message}`);
        return 0;
    }
}

// 核心策略：同步进行中时，完全阻止所有发送操作，不依赖内容匹配
const syncLockManager = {

    // 获取锁 - 如果已有锁在进行中，返回失败
    acquireLock(conversation_id) {
        const existing = syncingConversations.get(conversation_id);

        if (existing) {
            const elapsed = Date.now() - existing.startTime;

            // 如果锁未释放且未超时，拒绝新的同步
            if (!existing.released && elapsed < 5 * 60 * 1000) {
                logToCollector('[SYNC_LOCK] Rejected - lock active', {
                    conversation_id,
                    lockId: existing.lockId,
                    elapsed: Math.round(elapsed / 1000),
                    completed: existing.completed
                });
                return {
                    success: false,
                    reason: 'sync_in_progress',
                    elapsed: Math.round(elapsed / 1000),
                    completed: existing.completed
                };
            }

            // 超时或已释放，强制清理
            logToCollector('[SYNC_LOCK] Force cleaning stale lock', {
                conversation_id,
                elapsed: Math.round(elapsed / 1000)
            });
        }

        // 生成唯一的锁 ID
        const lockId = `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

        const newLock = {
            lockId,
            startTime: Date.now(),
            completed: false,
            released: false,
            messageCount: 0
        };

        syncingConversations.set(conversation_id, newLock);

        logToCollector('[SYNC_LOCK] Acquired', {
            conversation_id,
            lockId
        });

        return { success: true, lock: newLock };
    },

    // 设置消息数量（用于日志）
    setMessageCount(conversation_id, count) {
        const lock = syncingConversations.get(conversation_id);
        if (lock) {
            lock.messageCount = count;
        }
    },

    // 检查是否应该阻止发送 - 核心方法
    // 只要有活跃的锁（未释放），就阻止发送
    shouldBlockSend(conversation_id) {
        const lock = syncingConversations.get(conversation_id);

        if (!lock) {
            return { block: false, reason: 'no_lock' };
        }

        if (lock.released) {
            return { block: false, reason: 'lock_released' };
        }

        // 有活跃的锁，阻止发送
        const elapsed = Date.now() - lock.startTime;
        return {
            block: true,
            reason: lock.completed ? 'sync_completed_waiting' : 'sync_in_progress',
            elapsed: Math.round(elapsed / 1000),
            lockId: lock.lockId
        };
    },

    // 标记完成并安排释放
    // ★★★ V5.3.2修复：缩短同步锁等待时间（从20秒改为3秒）★★★
    markComplete(conversation_id, delayMs = 3000) {
        const lock = syncingConversations.get(conversation_id);
        if (!lock) return;

        lock.completed = true;
        lock.completedAt = Date.now();

        const lockId = lock.lockId;

        logToCollector('[SYNC_LOCK] Marked complete', {
            conversation_id,
            lockId,
            messageCount: lock.messageCount,
            willReleaseIn: delayMs
        });

        // 延迟释放（缩短为3秒，之前是20秒）
        setTimeout(() => {
            const currentLock = syncingConversations.get(conversation_id);
            if (currentLock && currentLock.lockId === lockId && !currentLock.released) {
                currentLock.released = true;
                logToCollector('[SYNC_LOCK] Released', { conversation_id, lockId });

                // 再过3秒后完全删除（之前是10秒）
                setTimeout(() => {
                    const stillThere = syncingConversations.get(conversation_id);
                    if (stillThere && stillThere.lockId === lockId) {
                        syncingConversations.delete(conversation_id);
                        logToCollector('[SYNC_LOCK] Deleted', { conversation_id, lockId });
                    }
                }, 3000);
            }
        }, delayMs);
    },

    // 强制释放锁
    forceRelease(conversation_id) {
        const lock = syncingConversations.get(conversation_id);
        if (lock) {
            lock.released = true;
            syncingConversations.delete(conversation_id);
            logToCollector('[SYNC_LOCK] Force released', { conversation_id, lockId: lock.lockId });
        }
    },

    // 获取锁状态（调试用）
    getStatus(conversation_id) {
        const lock = syncingConversations.get(conversation_id);
        if (!lock) return null;
        return {
            lockId: lock.lockId,
            startTime: lock.startTime,
            elapsed: Date.now() - lock.startTime,
            completed: lock.completed,
            released: lock.released,
            messageCount: lock.messageCount
        };
    },

    // 获取所有活跃锁（调试用）
    getAllLocks() {
        const result = {};
        for (const [convId, lock] of syncingConversations) {
            result[convId] = this.getStatus(convId);
        }
        return result;
    }
};

global.logToCollector = logToCollector; // 需要的话其他文件也能用

// 进程启动时就落一条，告诉你日志实际写到哪
logToCollector('[BOOT] collector up', {log: COLLECTOR_LOG, pid: process.pid});
console.log('[BOOT] collector up, log file =', COLLECTOR_LOG);
// == 小工具 ==
const CW_INTERNAL_BASE = process.env.CW_INTERNAL_BASE || 'http://chatwoot-web:3000';

function rewriteCwDataUrl(u) {
    if (!u) return u;
    try {
        const src = new URL(u);

        // 重写来自 Chatwoot 的 localhost:3000
        if (src.hostname === 'localhost' && (src.port === '' || src.port === '3000')) {
            const base = new URL(CW_INTERNAL_BASE);
            src.protocol = base.protocol;
            src.hostname = base.hostname;
            src.port = base.port;
            return src.toString();
        }

        // ===== 新增：内网 HTTPS -> HTTP 转换 =====
        const internalPatterns = [
            /^192\.168\./, /^10\./, /^172\.(1[6-9]|2[0-9]|3[0-1])\./,
            /^127\./, /localhost/i
        ];
        const isInternal = internalPatterns.some(p => p.test(src.hostname));
        if (isInternal && src.protocol === 'https:') {
            src.protocol = 'http:';
            console.log('[URL_REWRITE] https->http:', u, '->', src.toString());
            return src.toString();
        }

        return u;
    } catch {
        return u;
    }
}
// 从 Redis 中解析发送目标（优先联系人的键，再回退会话键，最后退到手机号）
// === REPLACE: resolveWaTarget (统一从 Redis 解析目标) ===
async function resolveWaTarget({redis, conversation_id, sender, fallback, WA_DEFAULT_SESSION}) {
    const getJSON = async (k) => {
        try {
            const v = await redis.get(k);
            return v ? JSON.parse(v) : null;
        } catch (_) {
            return null;
        }
    };

    // 1) 优先用会话映射
    let m = null;
    if (conversation_id) m = await getJSON(`cw:mapping:conv:${conversation_id}`);
    // 2) 再退到联系人映射
    if (!m && sender?.id) m = await getJSON(`cw:mapping:contact:${sender.id}`);

    // 3) 再退到 Chatwoot 的 identifier
    // ★★★ 支持新格式: wa:sessionId:phone:123456 或 wa:sessionId:lid:123456 ★★★
    // ★★★ 也支持旧格式: wa:sessionId:123456 ★★★
    const idParts = String(sender?.identifier || '').split(':');
    let idSess = null;
    let idType = null;  // 'phone' 或 'lid'
    let idDigits = null;

    if (idParts[0] === 'wa') {
        if (idParts.length === 4) {
            // 新格式: wa:sessionId:phone:123456 或 wa:sessionId:lid:123456
            idSess = idParts[1] || null;
            idType = idParts[2] || null;  // 'phone' 或 'lid'
            idDigits = idParts[3] || null;
        } else if (idParts.length === 3) {
            // 旧格式: wa:sessionId:123456
            idSess = idParts[1] || null;
            idDigits = idParts[2] || null;
            idType = 'phone';  // 旧格式默认当作 phone
        }
    }

    // 4) 选 sessionId
    const sessionId = m?.sessionId
        || sender?.additional_attributes?.session_id
        || idSess
        || WA_DEFAULT_SESSION;

    // 5) 分别获取 phone 和 phone_lid
    let to = '';      // 真实电话
    let to_lid = '';  // 隐私号

    // 从 Redis 映射中获取
    if (m?.phone) {
        to = String(m.phone).replace(/\D/g, '');
    }
    if (m?.phone_lid) {
        to_lid = String(m.phone_lid).replace(/\D/g, '');
    }

    // 如果 Redis 没有，尝试从 identifier 或 fallback 获取
    if (!to && !to_lid) {
        const digits = idDigits || String(fallback || '').replace(/\D/g, '');
        if (digits) {
            // ★★★ 根据 identifier 类型判断是电话还是 LID ★★★
            if (idType === 'lid') {
                to_lid = digits;
            } else {
                to = digits;
            }
        }
    }

    // 6) 群聊特殊处理
    const chatIdRaw = m?.chatId || '';
    if (/@g\.us$/i.test(chatIdRaw)) {
        to = chatIdRaw;
        to_lid = '';
    }

    // 7) 记录日志
    logToCollector('[CW_TARGET]', {
        contact_id: sender?.id || null,
        conversation_id,
        sessionId,
        to: to || 'none',
        to_lid: to_lid || 'none',
        source: m ? 'redis' : (idDigits ? 'identifier' : 'fallback')
    });

    return { sessionId, to, to_lid };
}


function toE164(phone) {
    if (!phone) return '';
    let s = String(phone).trim().replace(/[^\d+]/g, '');
    if (!s) return '';
    if (!s.startsWith('+')) s = '+' + s;
    return s;
}

const http = require('http');
const https = require('https');
const keepAliveHttp = new http.Agent({keepAlive: true, maxSockets: 100});
const keepAliveHttps = new https.Agent({keepAlive: true, maxSockets: 100});

function normalizeWaTo(input) {
    if (!input) return null;
    let t = String(input).trim();

    // 群聊：直接放行
    if (/^[^@]+@g\.us$/i.test(t)) return t;

    // 个人号：已是标准 chatId
    if (/^\d+@c\.us$/i.test(t)) return t;

    // 形如 "digits@lid" —— 一律退回到 c.us
    if (/^\d+@lid$/i.test(t)) {
        return t.replace(/@lid$/i, '@c.us');
    }

    // 其它任何含有 @lid 的花样（例如某些实现会给出 lid_*）——尽力取出数字回退到 c.us
    if (/@lid/i.test(t)) {
        const digits = t.replace(/\D/g, '');
        return digits ? `${digits}@c.us` : null;
    }

    // 只给了手机号：去非数字补 c.us
    const digits = t.replace(/\D/g, '');
    return digits ? `${digits}@c.us` : null;
}

async function postWithRetry(url, data, headers = {}, tries = 3, timeout = 120000) {
    const cfg = {
        method: 'post',
        url,
        data,
        headers: {'Content-Type': 'application/json', ...headers},
        timeout,
        httpAgent: keepAliveHttp,
        httpsAgent: keepAliveHttps,
        maxBodyLength: 30 * 1024 * 1024,
    };

    let backoff = 500; // 初始退避 500ms
    for (let i = 1; i <= tries; i++) {
        try {
            const r = await axios(cfg);
            return r.data;
        } catch (err) {
            const errCode = err.code || '';
            const isConnectionError = ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE', 'ECONNABORTED', 'ENOTFOUND'].includes(errCode);

            logToCollector('[BRIDGE_POST_ERROR]', {
                url,
                try: i,
                maxTries: tries,
                status: err.response?.status,
                code: errCode,
                errno: err.errno,
                address: err.address,
                port: err.port,
                willRetry: i < tries && isConnectionError
            });

            // 最后一轮直接抛出
            if (i === tries) throw err;

            // 对于连接错误，使用更长的退避时间
            if (isConnectionError) {
                const waitTime = errCode === 'ECONNREFUSED' ? Math.min(5000, backoff * 2) : Math.min(3000, backoff);
                console.log(`[BRIDGE_RETRY] Connection error (${errCode}), waiting ${waitTime}ms before retry ${i+1}/${tries}`);
                await new Promise(r => setTimeout(r, waitTime));
                backoff = Math.min(8000, backoff * 2);
            } else if (!err.response?.status) {
                // 其他网络错误
                await new Promise(r => setTimeout(r, backoff));
                backoff = Math.min(4000, backoff * 2);
            } else {
                // HTTP 错误，短暂等待后重试
                await new Promise(r => setTimeout(r, 300));
            }
        }
    }
}

async function markMessageAsFailed(account_id, conversation_id, message_id, errorReason) {
    try {
        logToCollector('[CW_MSG_FAILED]', {
            account_id,
            conversation_id,
            message_id,
            error: errorReason
        });

        if (conversation_id && account_id) {
            try {
                await cw.createOutgoingMessage({
                    account_id,
                    conversation_id,
                    content: `⚠️ 消息发送失败: ${errorReason}`,
                    private: true,
                    content_attributes: {
                        message_type: 'activity',
                        is_error: true
                    }
                });
            } catch (e) {
                console.error('[markMessageAsFailed] Failed to create error note:', e.message);
            }
        }

        return true;
    } catch (e) {
        console.error('[markMessageAsFailed] Error:', e.message);
        return false;
    }
}


function arrayify(x) {
    if (!x) return [];
    return Array.isArray(x) ? x : [x];
}

// 统一把 webhook 的 attachment/media 转成 [{file_url|data_url, file_type, filename}]
function normalizeAttachments({attachment, media, messageId}) {
    const out = [];

    // A：attachment 可以是对象或数组；支持 data_url / file_url / b64 / mime / filename / caption
    for (const a of arrayify(attachment)) {
        if (!a) continue;
        const o = {};
        if (a.data_url) o.data_url = a.data_url;
        else if (a.file_url || a.url) o.file_url = a.file_url || a.url;
        else if (a.b64 && a.mime) {
            const m = String(a.mime || '').split(';')[0]; // 去掉 ;codecs=opus 等参数
            o.data_url = `data:${m};base64,${a.b64}`;
        }
        if (a.mime) o.file_type = a.mime;
        if (a.filename) o.filename = a.filename;
        // 允许透传 caption（供上层决定是否并入 content）
        if (a.caption) o._caption = a.caption;

        out.push(o);
    }

    // B：media 可以是对象或数组；支持 url/b64/mime/name/caption
    for (const m of arrayify(media)) {
        if (!m) continue;
        const o = {};
        if (m.url) o.file_url = m.url;
        else if (m.b64 && m.mime) {
            const mm = String(m.mime || '').split(';')[0];
            o.data_url = `data:${mm};base64,${m.b64}`;
        }
        if (m.mime) o.file_type = m.mime;
        if (m.name) o.filename = m.name;
        if (m.caption) o._caption = m.caption;
        out.push(o);
    }

    // 去掉空项（既无 data_url 也无 file_url）
    return out.filter(x => x.data_url || x.file_url);
}

let CACHED_INBOX_ID = CHATWOOT_INBOX_ID || null;
const CACHED_SESSION_INBOX = new Map();  // session -> inbox 缓存

// ★★★ 修改：支持 sessionId 参数 ★★★
async function resolveInboxId(sessionId = null) {
    // 1. 如果有 session -> inbox 映射，优先使用
    if (sessionId && CHATWOOT_INBOX_MAP.size > 0) {
        // 检查缓存
        if (CACHED_SESSION_INBOX.has(sessionId)) {
            return CACHED_SESSION_INBOX.get(sessionId);
        }

        const mappedInboxId = CHATWOOT_INBOX_MAP.get(sessionId);
        if (mappedInboxId) {
            // 验证 inbox 存在
            const list = await cw.listInboxes(CHATWOOT_ACCOUNT_ID);
            const arr = (list && list.payload) || list || [];
            const hit = arr.find(x => Number(x.id) === mappedInboxId);
            if (hit) {
                CACHED_SESSION_INBOX.set(sessionId, mappedInboxId);
                console.log(`[resolveInboxId] Session ${sessionId} -> Inbox ${mappedInboxId}`);
                return mappedInboxId;
            } else {
                console.warn(`[resolveInboxId] Mapped inbox ${mappedInboxId} not found for session ${sessionId}, falling back`);
            }
        }
    }

    // 2. 没有映射或映射失败，使用默认逻辑
    if (CACHED_INBOX_ID) return CACHED_INBOX_ID;

    if (CHATWOOT_INBOX_ID && /^\d+$/.test(String(CHATWOOT_INBOX_ID))) {
        // 校验一下这个 id 是否真的在该 account 下
        const list = await cw.listInboxes(CHATWOOT_ACCOUNT_ID);
        const arr = (list && list.payload) || list || [];
        const hit = arr.find(x => Number(x.id) === Number(CHATWOOT_INBOX_ID));
        if (!hit) {
            const candidates = arr.map(x => `${x.id}:${x.name || '-'}`).join(', ');
            throw new Error(`CHATWOOT_INBOX_ID=${CHATWOOT_INBOX_ID} 不属于 account=${CHATWOOT_ACCOUNT_ID}。可用 inbox: ${candidates}`);
        }
        CACHED_INBOX_ID = Number(CHATWOOT_INBOX_ID);
        return CACHED_INBOX_ID;
    }

    if (CHATWOOT_INBOX_IDENTIFIER) {
        CACHED_INBOX_ID = await cw.getInboxIdByIdentifier(CHATWOOT_ACCOUNT_ID, CHATWOOT_INBOX_IDENTIFIER);
        return CACHED_INBOX_ID;
    }

    throw new Error('未配置 CHATWOOT_INBOX_ID（数字）或 CHATWOOT_INBOX_IDENTIFIER（名称）。');
}

/**
 * 新建联系人后同步历史消息
 *
 * 逻辑：查消息数 > 1 → 调用 sync-messages 全套流程（12小时）
 * sync-messages 已有完整去重逻辑，不需要额外处理
 */
async function syncNewContactHistory({ sessionId, sessionName, chatId, phone, phone_lid, contactName, conversation_id }) {
    const HOURS = 12;

    try {
        // 1. 查询 WhatsApp 上的消息数量
        let count = 0;
        try {
            const resp = await axios.post(
                `${WA_BRIDGE_URL}/messages-count`,
                { sessionId, chatId, hours: HOURS },
                { headers: WA_BRIDGE_TOKEN ? { 'x-api-token': WA_BRIDGE_TOKEN } : {}, timeout: 30000 }
            );
            count = resp.data?.count || 0;
        } catch (e) {
            // 备选方案
            try {
                const afterTs = Math.floor((Date.now() - HOURS * 3600 * 1000) / 1000);
                const resp = await axios.get(
                    `${WA_BRIDGE_URL}/messages-sync/${sessionId}/${encodeURIComponent(chatId)}?limit=20&after=${afterTs}`,
                    { headers: WA_BRIDGE_TOKEN ? { 'x-api-token': WA_BRIDGE_TOKEN } : {}, timeout: 60000 }
                );
                count = (resp.data?.messages || resp.data || []).length;
            } catch { return; }
        }

        console.log(`[NewContactSync] ${chatId}: ${count} messages`);

        // 2. 超过1条 → 调用 sync-messages
        if (count > 1) {
            const now = new Date();
            const after = new Date(now.getTime() - HOURS * 3600 * 1000);

            const resp = await axios.post(
                `http://127.0.0.1:${PORT}/sync-messages`,
                {
                    conversation_id, chatId, sessionId, sessionName, phone, phone_lid, contactName,
                    after: after.toISOString(),
                    before: now.toISOString(),
                    direction: 'both',
                    batchSize: 50,
                    useBatchCreate: true
                },
                { headers: INGEST_TOKEN ? { 'x-api-token': INGEST_TOKEN } : {}, timeout: 300000 }
            );

            console.log(`[NewContactSync] ${chatId}: synced ${resp.data?.summary?.synced || resp.data?.synced || 0} messages`);
        }
    } catch (e) {
        console.error(`[NewContactSync] Error:`, e.message);
    }
}

// ===== App 初始化 =====
const app = express();
const cors = require('cors');

// 使用 cors 包处理跨域
app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'x-api-token', 'X-Api-Token', 'Authorization', 'x-chatwoot-webhook-token'],
    credentials: false
}));
app.use((req, res, next) => {
    console.log(`[CORS] ${req.method} ${req.path} from ${req.headers.origin || 'no-origin'}`);
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type, x-api-token, X-Api-Token, Authorization');
    res.header('Access-Control-Max-Age', '86400');
    if (req.method === 'OPTIONS') {
        console.log('[CORS] Responding to OPTIONS preflight');
        return res.status(200).end();
    }
    next();
});

app.use('/media', express.static(MEDIA_DIR));
app.use(bodyParser.json({limit: '25mb'}));
app.use(morgan('dev'));
// 统一鉴权：头 x-api-token 或 query ?token=
app.use((req, res, next) => {
    if (req.method === 'OPTIONS') {
        return next();
    }
    // 鉴权（支持 header: x-api-token 或 query: ?token=）
    const t = (
        req.headers['x-api-token'] ||
        req.headers['x-chatwoot-webhook-token'] ||
        req.query.token ||
        ''
    ).toString();
    if (INGEST_TOKEN && t !== INGEST_TOKEN) {
        return res.status(401).json({ok: false, error: 'bad token'});
    }
    next();
});
// 健康检查
app.get('/health', (_req, res) => res.json({ok: true}));

/**
 * 入站采集 → Chatwoot
 * 支持两种附件格式：
 *  A) { type:'image', attachment: { data_url|file_url|b64, mime?, filename? } }
 *  B) { media: { type: 'image'|'video'|'audio'|'document', url?|b64?, mime?, name?, caption? } }
 */
app.get('/debug/accounts', async (_req, res) => {
    try {
        const data = await cw.listAccounts();
        const payload = data?.payload || data || [];
        res.json({
            ok: true,
            env: {CHATWOOT_ACCOUNT_ID, CHATWOOT_INBOX_ID, CHATWOOT_BASE_URL: CFG.chatwoot.baseURL},
            accounts: payload.map(a => ({id: a.id, name: a.name, role: a.role})),
        });
    } catch (e) {
        res.status(500).json({ok: false, error: e.message, cw: e._cw || e.response?.data});
    }
});

app.get('/debug/inboxes', async (_req, res) => {
    try {
        const data = await cw.listInboxes(CHATWOOT_ACCOUNT_ID);
        const payload = data?.payload || data || [];
        res.json({
            ok: true,
            account_id: CHATWOOT_ACCOUNT_ID,
            inboxes: payload.map(x => ({id: x.id, name: x.name, channel_type: x.channel_type})),
        });
    } catch (e) {
        res.status(500).json({ok: false, error: e.message, cw: e._cw || e.response?.data});
    }
});

// ★★★ V5 新增：处理消息撤回事件 ★★★
async function handleMessageRevoke(req, res) {
    try {
        const {
            sessionId,
            sessionName,
            phone,
            phone_lid,
            messageId,
            revokedBy,
            originalBody,
            originalType
        } = req.body || {};

        console.log(`[REVOKE] Processing: messageId=${messageId?.substring(0, 40)}..., revokedBy=${revokedBy}`);

        if (!messageId) {
            return res.json({ ok: true, skipped: 'no messageId' });
        }

        // ★★★ V5.3.13增强：多种方式查找映射 ★★★
        let cwMessageId = null;
        let conversationId = null;
        const suffix = extractMessageIdSuffix(messageId);

        if (redis && suffix && suffix.length >= 10) {
            // 方法1: 查找全局后缀映射（新消息）
            const globalMapping = await redis.get(`cw:msgmap:suffix_global:${suffix}`);
            if (globalMapping) {
                const [convId, cwId] = globalMapping.split(':');
                conversationId = Number(convId);
                cwMessageId = Number(cwId);
                console.log(`[REVOKE] Found via global suffix: conv=${conversationId}, cw=${cwMessageId}`);
            }

            // 方法2: 如果全局没找到，通过phone/phone_lid找conversation，再查会话级映射
            if (!cwMessageId && (phone || phone_lid)) {
                try {
                    // 查找conversation映射
                    const identifier = phone || phone_lid;
                    const convKeys = await redis.keys(`cw:mapping:conv:*`);

                    for (const key of convKeys) {
                        const mapping = await redis.get(key);
                        if (mapping) {
                            const parsed = JSON.parse(mapping);
                            // 检查phone或phone_lid是否匹配
                            if (parsed.phone === identifier || parsed.phone_lid === identifier ||
                                parsed.phone === phone || parsed.phone_lid === phone_lid) {
                                conversationId = parsed.conversation_id;

                                // 找到conversation后，查会话级后缀映射
                                const suffixKey = `cw:msgmap:suffix:${conversationId}:${suffix}`;
                                const cwId = await redis.get(suffixKey);
                                if (cwId) {
                                    cwMessageId = Number(cwId);
                                    console.log(`[REVOKE] Found via conversation suffix: conv=${conversationId}, cw=${cwMessageId}`);
                                    break;
                                }
                            }
                        }
                    }
                } catch (e) {
                    console.warn(`[REVOKE] Fallback lookup failed: ${e.message}`);
                }
            }
        }

        // 3. 如果找到了映射，调用Chatwoot API删除
        if (cwMessageId && conversationId) {
            try {
                logToCollector('[REVOKE] Calling Chatwoot delete API', {
                    conversation_id: conversationId,
                    cw_message_id: cwMessageId,
                    wa_message_id: messageId?.substring(0, 30)
                });

                // 调用Chatwoot删除API（添加skip_wa_delete避免循环）
                const deleteUrl = `${CFG.chatwoot.baseURL}/api/v1/accounts/${CHATWOOT_ACCOUNT_ID}/conversations/${conversationId}/messages/${cwMessageId}?skip_wa_delete=true`;

                const deleteResult = await axios.delete(deleteUrl, {
                    headers: {
                        'api_access_token': CHATWOOT_TOKEN,
                        'Content-Type': 'application/json'
                    },
                    timeout: 10000
                }).catch(e => ({ status: e.response?.status, error: e.message }));

                if (deleteResult.status === 200 || deleteResult.status === 204) {
                    logToCollector('[REVOKE] Chatwoot delete success', {
                        conversation_id: conversationId,
                        cw_message_id: cwMessageId
                    });
                } else {
                    logToCollector('[REVOKE] Chatwoot delete failed', {
                        status: deleteResult.status,
                        error: deleteResult.error
                    });
                }
            } catch (e) {
                console.error(`[REVOKE] Failed to delete Chatwoot message: ${e.message}`);
                logToCollector('[REVOKE] Chatwoot delete error', { error: e.message });
            }
        } else {
            // 没有找到映射，记录日志
            logToCollector('[REVOKE] Message revoked (no mapping)', {
                wa_message_id: messageId?.substring(0, 30),
                phone: phone || phone_lid,
                originalType,
                revokedBy
            });
        }

        return res.json({
            ok: true,
            revoked: true,
            wa_message_id: messageId,
            cw_message_id: cwMessageId || null,
            conversation_id: conversationId || null
        });

    } catch (e) {
        console.error('[REVOKE] Error:', e.message);
        return res.status(500).json({ ok: false, error: e.message });
    }
}

app.post('/ingest', async (req, res) => {
    // ★★★ 在 try 块外定义，让 catch 块也能访问 ★★★
    const {
        sessionId,
        sessionName,
        phone: rawPhone,
        phone_lid: rawPhoneLid,
        name,
        text,
        type,
        messageId,
        timestamp,
        attachment,
        media,
        // ★★★ V5 新增：引用消息信息 ★★★
        quotedMsg
    } = req.body || {};

    // ★★★ V5 新增：处理消息撤回事件 ★★★
    if (type === 'message_revoke') {
        return handleMessageRevoke(req, res);
    }

    // ★★★ 关键修复：从 messageId 提取 phone/phone_lid ★★★
    let phone = rawPhone;
    let phone_lid = rawPhoneLid;

    // 如果 phone 为空，尝试从 messageId 提取
    if (!phone && messageId) {
        // 检查是否是 LID 格式: false_27153387237439@lid_xxx
        const lidMatch = messageId.match(/(\d+)@lid/);
        if (lidMatch) {
            phone_lid = lidMatch[1];
            console.log(`[INGEST] Extracted phone_lid from messageId: ${phone_lid}`);
        } else {
            // 检查是否是普通格式: false_85270360156@c.us_xxx
            const phoneMatch = messageId.match(/(\d+)@c\.us/);
            if (phoneMatch) {
                phone = phoneMatch[1];
                console.log(`[INGEST] Extracted phone from messageId: ${phone}`);
            }
        }
    }

    let retryAttempted = false;
    let contact, conv, conversation_id, inbox_id;

    try {
        const isGroup = /@g\.us$/i.test(String(messageId || '')) || /@g\.us$/i.test(String(req.body?.from || '')) || String(req.body?.server || '').toLowerCase() === 'g.us';
        const isStatus = String(phone || '').toLowerCase() === 'status' || /@broadcast/i.test(String(messageId || '')) || /@broadcast/i.test(String(req.body?.from || ''));
        if (isGroup || isStatus) {
            return res.json({ok: true, skipped: 'ignored group/status message'});
        }

        // ★★★ 入站消息 Redis 去重 ★★★
        if (redis && messageId) {
            const redisKey = `wa:synced:incoming:${messageId}`;
            const exists = await redis.get(redisKey);
            if (exists) {
                console.log('[INGEST] Skip (Redis dedup):', messageId?.substring(0, 30));
                return res.json({ ok: true, skipped: 'duplicate_redis' });
            }
        }

        console.log('Received webhook:', {
            sessionId, sessionName, phone, phone_lid, name, text, type, messageId,
            hasAttachment: !!attachment, hasMedia: !!media,
            hasQuote: !!quotedMsg,
            quotedMsgId: quotedMsg?.id?.substring(0, 30) || null,
            quotedMsgBody: quotedMsg?.body?.substring(0, 30) || null,  // ★ V5新增：打印引用内容
            quotedMsgKeys: quotedMsg ? Object.keys(quotedMsg) : null   // ★ V5新增：打印引用对象的所有字段
        });

        // ★★★ 新增：LID 转电话号码三层兜底逻辑 ★★★
        // 层级1: 已有电话号码 (@c.us) - 直接使用
        // 层级2: 只有 LID (@lid) - 调用 Bridge API getContactLidAndPhone
        // 层级3: API 也找不到 - 检查联系人名称是否看起来像电话号码
        if (!phone && phone_lid && sessionId) {
            console.log(`[INGEST] No phone, trying to resolve LID: ${phone_lid}`);

            // 层级2: 调用 Bridge API 解析 LID
            try {
                const resolveUrl = `${WA_BRIDGE_URL}/resolve-lid/${sessionId}/${phone_lid}`;
                const resolveResp = await axios.get(resolveUrl, {
                    headers: WA_BRIDGE_TOKEN ? { 'x-api-token': WA_BRIDGE_TOKEN } : {},
                    timeout: 10000
                }).catch(() => null);

                if (resolveResp?.data?.ok && resolveResp.data.phone) {
                    phone = resolveResp.data.phone;
                    console.log(`[INGEST] LID resolved to phone via API: ${phone_lid} -> ${phone}`);
                } else {
                    console.log(`[INGEST] LID API returned no phone for: ${phone_lid}`);
                }
            } catch (e) {
                console.log(`[INGEST] LID resolve API error: ${e?.message}`);
            }

            // 层级3: 检查联系人名称是否看起来像电话号码
            if (!phone && name) {
                const phoneFromName = extractPhoneFromName(name);
                if (phoneFromName) {
                    phone = phoneFromName;
                    console.log(`[INGEST] Extracted phone from contact name: "${name}" -> ${phone}`);
                }
            }
        }

        inbox_id = await resolveInboxId(sessionId);  // ★ 传入 sessionId
        const phoneE164 = toE164(phone || phone_lid);

        // 1) 联系人（稳定复用：identifier=wa:<原始phone>，并尽量提取 E.164）
        contact = await cw.ensureContact({
            account_id: CHATWOOT_ACCOUNT_ID,
            rawPhone: phone,
            rawPhone_lid: phone_lid,
            rawName: name,
            sessionId,
            sessionName,  // ★ 传递动态名称
            messageId     // ★ 传递 messageId 以便提取 digits
        });

// 2) 会话（按联系人 + inbox 复用；不再传 source_id）
        conv = await cw.ensureConversation({
            account_id: CHATWOOT_ACCOUNT_ID,
            inbox_id,
            contact_id: contact.id
        });
        conversation_id = conv.id || conv;

        // ★★★ 关键修复：新联系人先同步历史，不创建触发消息 ★★★
        if (conv._isNew && sessionId) {
            const calcChatId = phone ? `${String(phone).replace(/\D/g, '')}@c.us`
                : phone_lid ? `${String(phone_lid).replace(/\D/g, '')}@lid`
                    : null;

            if (calcChatId) {
                console.log(`[INGEST] New conversation -> sync history directly`, { conversation_id, chatId: calcChatId });

                const HOURS = 12;
                const now = new Date();
                const after = new Date(now.getTime() - HOURS * 3600 * 1000);

                let syncedCount = 0;
                let triggerMessageSynced = false;

                try {
                    const syncResp = await axios.post(
                        `http://127.0.0.1:${PORT}/sync-messages`,
                        {
                            conversation_id,
                            chatId: calcChatId,
                            sessionId,
                            sessionName: sessionName || sessionId,
                            phone,
                            phone_lid,
                            contactName: name,
                            after: after.toISOString(),
                            before: now.toISOString(),
                            direction: 'both',
                            batchSize: 50,
                            useBatchCreate: true
                        },
                        { headers: INGEST_TOKEN ? { 'x-api-token': INGEST_TOKEN } : {}, timeout: 300000 }
                    );

                    syncedCount = syncResp.data?.summary?.synced || syncResp.data?.synced || 0;
                    console.log(`[INGEST] History synced for new contact: ${syncedCount} messages`);

                    // ★★★ V5修复：检查触发消息是否已同步 ★★★
                    // 通过 messageId 检查当前消息是否在同步的消息中
                    if (messageId && redis) {
                        const syncedKey = `wa:synced:incoming:${messageId}`;
                        const exists = await redis.get(syncedKey);
                        triggerMessageSynced = !!exists;
                        console.log(`[INGEST] Trigger message synced check: ${triggerMessageSynced ? 'YES' : 'NO'}`);
                    }
                } catch (syncErr) {
                    console.error(`[INGEST] History sync failed, will create single message:`, syncErr.message);
                    // 同步失败，继续创建单条消息
                    triggerMessageSynced = false;
                }

                // ★★★ V5修复：如果触发消息已同步，直接返回；否则继续创建 ★★★
                if (triggerMessageSynced) {
                    console.log(`[INGEST] Trigger message already synced, returning`);
                    return res.json({ ok: true, conversation_id, synced: syncedCount, newConversation: true });
                }

                // 继续创建触发消息（媒体消息可能没有被正确同步）
                console.log(`[INGEST] Trigger message NOT synced, creating it now...`);
            }
        }

// 3) 组装文本与附件（支持多附件、语音/视频等）
        let content = (text || '').toString();

// ★★★ V5.1修复：改进媒体补救逻辑 ★★★
// 先尝试归一化现有的 attachment/media
        let initialAttachments = normalizeAttachments({attachment, media, messageId});

// 如果是媒体类型消息但没有有效附件，尝试从 Bridge 补充获取
        const MEDIA_TYPES = ['image', 'video', 'audio', 'ptt', 'sticker', 'document'];
        if (MEDIA_TYPES.includes(type) && initialAttachments.length === 0 && sessionId && messageId) {
            console.log(`[INGEST] Media message (${type}) but no valid attachment, trying to fetch from Bridge`);
            console.log(`[INGEST] Original attachment object: ${attachment ? 'present' : 'null'}, media object: ${media ? 'present' : 'null'}`);

            try {
                // 调用 Bridge API 尝试重新获取媒体
                const fetchMediaUrl = `${WA_BRIDGE_URL}/fetch-media`;
                const mediaResp = await axios.post(fetchMediaUrl, {
                    sessionId,
                    messageId
                }, {
                    headers: WA_BRIDGE_TOKEN ? { 'x-api-token': WA_BRIDGE_TOKEN } : {},
                    timeout: 30000  // 入站消息给更长时间
                }).catch(err => {
                    console.log(`[INGEST] fetch-media request failed: ${err?.message}`);
                    return null;
                });

                if (mediaResp?.data?.ok && mediaResp.data.media) {
                    const m = mediaResp.data.media;
                    const enhancedAttachment = {
                        data_url: `data:${m.mimetype};base64,${m.data}`,
                        mime: m.mimetype,
                        filename: m.filename || `${type}_${Date.now()}.${m.mimetype?.split('/')[1] || 'bin'}`
                    };
                    initialAttachments = normalizeAttachments({attachment: enhancedAttachment, messageId});
                    console.log(`[INGEST] ✓ Media fetched successfully: ${m.mimetype}, size=${m.data?.length || 0}, attachments=${initialAttachments.length}`);
                } else {
                    console.log(`[INGEST] ✗ Media fetch failed or returned no data`);
                }
            } catch (e) {
                console.log(`[INGEST] ✗ Media fetch error: ${e?.message}`);
            }
        }

        const attachments = initialAttachments;

// 若外部把文字写在附件 caption 里，也并到 content（仅当原 content 为空）
        if (!content && attachments.length > 0) {
            const cap = attachments.find(a => a._caption)?.['_caption'];
            if (cap) content = cap;
        }
// 清理内部字段
        attachments.forEach(a => {
            delete a._caption;
        });

// 4) 发 incoming 消息到 Chatwoot
        // ★★★ V5.3.5修复：使用综合查找函数（Redis + source_id） ★★★
        let in_reply_to = null;
        if (quotedMsg && quotedMsg.id && conversation_id) {
            in_reply_to = await findCwMessageId(conversation_id, quotedMsg.id);
            if (in_reply_to) {
                console.log(`[INGEST] Found native reply: wa=${quotedMsg.id.substring(0, 30)} -> cw=${in_reply_to}`);
            } else {
                console.log(`[INGEST] No CW message ID found for quote, will use text fallback`);
            }
        }

        const created = await cw.createIncomingMessage({
            account_id: CHATWOOT_ACCOUNT_ID,
            conversation_id,
            // 关键：传 content（或 text，chatwoot.js 里已兼容）
            content,
            attachments,
            text,         // 兼容保留
            // ★★★ V5 新增：传递引用消息和WA消息ID ★★★
            quotedMsg,
            wa_message_id: messageId,
            // ★★★ V5.3.12修复：传递source_id确保Chatwoot保存WhatsApp消息ID ★★★
            source_id: messageId,
            // ★★★ V5.3.3新增：原生引用 ★★★
            in_reply_to
        });

        // 5) 映射保存（可选） + 同步到 Chatwoot 联系人备注
        if (redis) {
            const now = new Date().toISOString();

            // 计算 chatId（优先用真实电话，其次用隐私号）
            const calcChatId = (() => {
                if (phone) {
                    const digits = String(phone).replace(/\D/g, '');
                    if (digits) return `${digits}@c.us`;
                }
                if (phone_lid) {
                    const digits = String(phone_lid).replace(/\D/g, '');
                    if (digits) return `${digits}@lid`;
                }
                return normalizeWaTo(String(req.body?.chatId || '')) || null;
            })();

            const payload = {
                sessionId,
                phone: String(phone || ''),            // 真实电话
                phone_lid: String(phone_lid || ''),    // 隐私号
                phone_e164: String(phoneE164 || ''),
                name: String(name || ''),
                chatId: calcChatId,
                updated_at: now
            };
            // 1) 以“联系人ID”为键（核心）
            await redis.set(
                `cw:mapping:contact:${contact.id}`,
                JSON.stringify(payload)
            );

            // 2) 兼容：以“会话ID”为键（保留）
            await redis.set(
                `cw:mapping:conv:${conversation_id}`,
                JSON.stringify(payload)
            );

            // 3) 同步到 Chatwoot 右侧【联系人备注】
            const noteContent =
                `sessionId: ${payload.sessionId}\n` +
                `phone: ${payload.phone}\n` +
                `phone_lid: ${payload.phone_lid}\n` +
                `phone_e164: ${payload.phone_e164}\n` +
                `chatId: ${payload.chatId}\n` +
                `updated_at: ${payload.updated_at}`;

            const noteKey = `cw:note:contact:${contact.id}`;
            let noteId = await redis.get(noteKey);

            try {
                if (noteId) {
                    // 更新已有备注
                    await cw.updateContactNote({
                        account_id: CHATWOOT_ACCOUNT_ID,
                        contact_id: contact.id,
                        note_id: noteId,
                        content: noteContent
                    });
                    logToCollector && logToCollector('[CW_NOTE] updated', { contact_id: contact.id, note_id: noteId });
                } else {
                    // 创建新备注
                    const note = await cw.createContactNote({
                        account_id: CHATWOOT_ACCOUNT_ID,
                        contact_id: contact.id,
                        content: noteContent
                    });
                    // 兼容不同返回结构，尽力拿到 id
                    noteId = String(note?.id || note?.note?.id || note?.payload?.id || '');
                    if (noteId) await redis.set(noteKey, noteId);
                    logToCollector && logToCollector('[CW_NOTE] created', { contact_id: contact.id, note_id: noteId || null });
                }
            } catch (e) {
                // 备注失败不影响主流程
                logToCollector && logToCollector('[CW_NOTE] error', {
                    contact_id: contact.id,
                    err: e?.response?.data || e?.message || String(e)
                });
            }
        }


        // ★★★ 标记入站消息已同步 ★★★
        if (redis && messageId) {
            await redis.set(`wa:synced:incoming:${messageId}`, '1', 'EX', 7 * 24 * 3600).catch(() => {});
        }

        // ★★★ V5.3.6修复：保存消息ID映射（关键！支持后续引用查找） ★★★
        const createdId = created?.id || created?.message?.id;
        if (createdId && messageId && conversation_id) {
            await saveMessageMapping(conversation_id, messageId, createdId);
            console.log(`[INGEST] Message mapping saved: wa=${messageId.substring(0, 30)} -> cw=${createdId}`);
        }

        // 注意：新联系人的历史同步已在上方处理（直接返回），此处不再需要异步调用

        res.json({ok: true, conversation_id, message_id: created.id || created.message?.id || null});
    } catch (e) {
        const errMsg = e?.response?.data?.error || e?.response?.data?.message || e?.message || String(e);
        const errData = e?.response?.data;

        // 如果是"已存在"错误（并发或重复），返回200避免重试
        if (/identifier.*already.*taken|already.*exists|duplicate/i.test(errMsg) ||
            /identifier.*already.*taken|already.*exists|duplicate/i.test(JSON.stringify(errData || {}))) {
            console.log('[INGEST] Already exists (concurrent/duplicate), skipping:', errMsg);
            return res.json({ ok: true, skipped: 'duplicate', message: 'Already exists' });
        }

        // ★★★ 新增：404 时清除缓存并重试 ★★★
        const is404 = e?.response?.status === 404 || /not.*found|Resource could not be found/i.test(errMsg);
        if (is404 && !retryAttempted) {
            console.log('[INGEST] 404 detected, clearing cache and retrying');

            // 清除缓存（包含 sessionId）
            const digits = (String(phone || phone_lid || '').match(/\d+/g) || []).join('');
            cw.clearAllCacheForDigits(digits, sessionId);

            retryAttempted = true;

            try {
                // 重新获取联系人和会话
                contact = await cw.ensureContact({
                    account_id: CHATWOOT_ACCOUNT_ID,
                    rawPhone: phone,
                    rawPhone_lid: phone_lid,
                    rawName: name,
                    sessionId,
                    sessionName
                });

                conv = await cw.ensureConversation({
                    account_id: CHATWOOT_ACCOUNT_ID,
                    inbox_id,
                    contact_id: contact.id
                });
                conversation_id = conv.id || conv;

                console.log('[INGEST] Retry with new IDs:', { contact_id: contact.id, conversation_id, isNew: conv._isNew });

                // ★★★ 关键修复：404 重试时如果是新会话，触发历史同步 ★★★
                if (conv._isNew && sessionId) {
                    const calcChatId = phone ? `${String(phone).replace(/\D/g, '')}@c.us`
                        : phone_lid ? `${String(phone_lid).replace(/\D/g, '')}@lid`
                            : null;

                    if (calcChatId) {
                        console.log('[INGEST] Retry: New conversation -> sync history', { conversation_id, chatId: calcChatId });

                        const HOURS = 12;
                        const now = new Date();
                        const after = new Date(now.getTime() - HOURS * 3600 * 1000);

                        try {
                            const syncResp = await axios.post(
                                `http://127.0.0.1:${PORT}/sync-messages`,
                                {
                                    conversation_id,
                                    chatId: calcChatId,
                                    sessionId,
                                    sessionName: sessionName || sessionId,
                                    phone,
                                    phone_lid,
                                    contactName: name,
                                    after: after.toISOString(),
                                    before: now.toISOString(),
                                    direction: 'both',
                                    batchSize: 50,
                                    useBatchCreate: true
                                },
                                { headers: INGEST_TOKEN ? { 'x-api-token': INGEST_TOKEN } : {}, timeout: 300000 }
                            );

                            const synced = syncResp.data?.summary?.synced || syncResp.data?.synced || 0;
                            console.log('[INGEST] Retry: History synced', { conversation_id, synced });

                            return res.json({ ok: true, conversation_id, synced, newConversation: true, retried: true });
                        } catch (syncErr) {
                            console.error('[INGEST] Retry: Sync failed, creating single message:', syncErr.message);
                        }
                    }
                }

                // 重新组装内容和附件（非新会话或同步失败时）
                let retryContent = (text || '').toString();
                const retryAttachments = normalizeAttachments({attachment, media, messageId});
                if (!retryContent && retryAttachments.length > 0) {
                    const cap = retryAttachments.find(a => a._caption)?.['_caption'];
                    if (cap) retryContent = cap;
                }
                retryAttachments.forEach(a => { delete a._caption; });

                // 重试创建消息
                const created = await cw.createIncomingMessage({
                    account_id: CHATWOOT_ACCOUNT_ID,
                    conversation_id,
                    content: retryContent,
                    attachments: retryAttachments,
                    text,
                    // ★★★ V5.3.12修复：传递source_id和引用信息 ★★★
                    source_id: messageId,
                    wa_message_id: messageId,
                    quotedMsg,
                    in_reply_to
                });

                // 标记入站消息已同步
                if (redis && messageId) {
                    await redis.set(`wa:synced:incoming:${messageId}`, '1', 'EX', 7 * 24 * 3600).catch(() => {});
                }

                // ★★★ V5.3.12修复：重试后也保存消息ID映射 ★★★
                const createdId = created?.id || created?.message?.id;
                if (createdId && messageId) {
                    await saveMessageMapping(conversation_id, messageId, createdId);
                    console.log(`[INGEST] Retry mapping saved: wa=${messageId.substring(0, 30)} -> cw=${createdId}`);
                }

                return res.json({ ok: true, conversation_id, message_id: createdId || null, retried: true });
            } catch (retryErr) {
                console.error('[INGEST] Retry failed:', retryErr?.message);
                return res.status(500).json({ ok: false, error: retryErr?.message });
            }
        }

        console.error('[INGEST_ERROR]', errData || errMsg);
        res.status(500).json({ok: false, error: errMsg, cw: errData});
    }
});
app.post('/message-status', async (req, res) => {
    try {
        const { sessionId, messageId, ack, status, timestamp } = req.body || {};

        if (ack === 0 || status === 'failed') {
            console.log(`[MSG_STATUS] FAILED: ${messageId?.substring(0, 30)}...`);
            logToCollector('[MSG_STATUS] FAILED', { sessionId, messageId, ack, status });
        }

        res.json({ ok: true });
    } catch (e) {
        res.status(500).json({ ok: false, error: e?.message });
    }
});

/**
 * 接口2：我方发送消息入站（同步 WhatsApp 端发送的消息到 Chatwoot）
 */
/**
 * 接口：同步手机端发送的消息到 Chatwoot（增强版）
 *
 * 去重机制：
 * 1. Redis 快速检查（7天TTL）
 * 2. Chatwoot API 检查（source_id）
 */
app.post('/ingest-outgoing', async (req, res) => {
    const startTime = Date.now();

    try {
        const {
            sessionId,
            sessionName,    // ★ 新增：从 bridge 传入的动态名称
            messageId,      // WhatsApp 消息ID（关键去重字段）
            phone,
            phone_lid,
            name,
            text,
            type,
            timestamp,
            to,
            chatId,
            fromMe,
            direction,
            attachment,
            quotedMsg       // ★ V5新增：引用消息对象
        } = req.body || {};

        // 1. 基本验证
        if (!fromMe || direction !== 'outgoing') {
            return res.json({ ok: true, skipped: 'not outgoing' });
        }

        if (!messageId) {
            logToCollector('[INGEST_OUT] No messageId', { to });
            return res.json({ ok: true, skipped: 'no messageId' });
        }

        logToCollector('[INGEST_OUT] Received', {
            sessionId,
            sessionName,  // ★ 添加到日志
            messageId: messageId.substring(0, 35),
            phone: phone || phone_lid || 'unknown',
            type,
            hasAttachment: !!attachment,
            hasQuote: !!quotedMsg  // ★ V5新增：是否有引用
        });

        // ★★★ 新增：媒体消息但没有 attachment 时，尝试补充获取 ★★★
        let enhancedAttachment = attachment;
        const MEDIA_TYPES = ['image', 'video', 'audio', 'ptt', 'sticker', 'document'];
        if (MEDIA_TYPES.includes(type) && !attachment && sessionId && messageId) {
            console.log(`[INGEST_OUT] Media message without attachment, trying to fetch: ${type}`);

            try {
                // 调用 Bridge API 尝试重新获取媒体
                const fetchMediaUrl = `${WA_BRIDGE_URL}/fetch-media`;
                const mediaResp = await axios.post(fetchMediaUrl, {
                    sessionId,
                    messageId
                }, {
                    headers: WA_BRIDGE_TOKEN ? { 'x-api-token': WA_BRIDGE_TOKEN } : {},
                    timeout: 20000
                }).catch(() => null);

                if (mediaResp?.data?.ok && mediaResp.data.media) {
                    const m = mediaResp.data.media;
                    enhancedAttachment = {
                        data_url: `data:${m.mimetype};base64,${m.data}`,
                        mime: m.mimetype,
                        filename: m.filename || `${type}_${Date.now()}.${m.mimetype?.split('/')[1] || 'bin'}`
                    };
                    console.log(`[INGEST_OUT] Media fetched successfully: ${m.mimetype}`);
                } else {
                    console.log(`[INGEST_OUT] Media fetch failed or returned no data`);
                }
            } catch (e) {
                console.log(`[INGEST_OUT] Media fetch error: ${e?.message}`);
            }
        }

        // 2. 跳过群组/广播
        if (/@g\.us$/i.test(to) || /@broadcast/i.test(to)) {
            return res.json({ ok: true, skipped: 'group/broadcast' });
        }

        // 3. Redis 快速去重（第一层，7天TTL）
        const redisKey = `wa:synced:outgoing:${messageId}`;
        if (redis) {
            const exists = await redis.get(redisKey);
            if (exists) {
                logToCollector('[INGEST_OUT] Skip (Redis)', { messageId: messageId.substring(0, 35) });
                return res.json({ ok: true, skipped: 'duplicate_redis' });
            }
        }

        // ★★★ V5.3.11新增：处理中锁，防止同一消息被并发处理 ★★★
        const processingKey = `wa:processing:outgoing:${messageId}`;
        if (redis) {
            // 尝试获取处理中锁（30秒过期，防止死锁）
            const acquired = await redis.set(processingKey, '1', 'EX', 30, 'NX');
            if (!acquired) {
                logToCollector('[INGEST_OUT] Skip (Processing)', { messageId: messageId.substring(0, 35) });
                return res.json({ ok: true, skipped: 'already_processing' });
            }
        }

        // ★★★ 新增：LID 转电话号码三层兜底逻辑 ★★★
        let resolvedPhone = phone;
        if (!resolvedPhone && phone_lid && sessionId) {
            console.log(`[INGEST_OUT] No phone, trying to resolve LID: ${phone_lid}`);

            // 层级2: 调用 Bridge API 解析 LID
            try {
                const resolveUrl = `${WA_BRIDGE_URL}/resolve-lid/${sessionId}/${phone_lid}`;
                const resolveResp = await axios.get(resolveUrl, {
                    headers: WA_BRIDGE_TOKEN ? { 'x-api-token': WA_BRIDGE_TOKEN } : {},
                    timeout: 10000
                }).catch(() => null);

                if (resolveResp?.data?.ok && resolveResp.data.phone) {
                    resolvedPhone = resolveResp.data.phone;
                    console.log(`[INGEST_OUT] LID resolved to phone via API: ${phone_lid} -> ${resolvedPhone}`);
                }
            } catch (e) {
                console.log(`[INGEST_OUT] LID resolve API error: ${e?.message}`);
            }

            // 层级3: 检查联系人名称是否看起来像电话号码
            if (!resolvedPhone && name) {
                const phoneFromName = extractPhoneFromName(name);
                if (phoneFromName) {
                    resolvedPhone = phoneFromName;
                    console.log(`[INGEST_OUT] Extracted phone from contact name: "${name}" -> ${resolvedPhone}`);
                }
            }
        }

        // 4. 确定目标电话
        const targetPhone = resolvedPhone || phone_lid || to?.replace(/@.*/, '').replace(/\D/g, '');
        if (!targetPhone) {
            return res.json({ ok: true, skipped: 'no phone' });
        }

        // 5. 确保联系人存在
        let contact = await cw.ensureContact({
            account_id: CHATWOOT_ACCOUNT_ID,
            rawPhone: targetPhone,
            rawPhone_lid: phone_lid,
            rawName: name || targetPhone,
            sessionId,
            sessionName,  // ★ 传递动态名称
            messageId
        });

        if (!contact?.id) {
            logToCollector('[INGEST_OUT] Contact failed', { targetPhone });
            return res.status(400).json({ ok: false, error: 'Failed to create contact' });
        }

        // 6. 确保会话存在
        const inbox_id = await resolveInboxId(sessionId);  // ★ 传入 sessionId
        const conv = await cw.ensureConversation({
            account_id: CHATWOOT_ACCOUNT_ID,
            inbox_id,
            contact_id: contact.id,
            custom_attributes: {
                wa_chat_id: chatId || to,
                session_id: sessionId
            }
        });

        let conversation_id = conv?.id || conv;
        if (!conversation_id) {
            logToCollector('[INGEST_OUT] Conversation failed', { contact_id: contact.id });
            return res.status(400).json({ ok: false, error: 'Failed to create conversation' });
        }

        // ★★★ 关键修复：新建联系人/会话时，先同步历史消息 ★★★
        if (conv._isNew && sessionId) {
            const calcChatId = chatId || (phone ? `${String(phone).replace(/\D/g, '')}@c.us`
                : phone_lid ? `${String(phone_lid).replace(/\D/g, '')}@lid`
                    : null);

            if (calcChatId) {
                logToCollector('[INGEST_OUT] New conversation -> sync history first', {
                    conversation_id,
                    chatId: calcChatId,
                    contact_id: contact.id
                });

                const HOURS = 12;
                const now = new Date();
                const after = new Date(now.getTime() - HOURS * 3600 * 1000);

                try {
                    const syncResp = await axios.post(
                        `http://127.0.0.1:${PORT}/sync-messages`,
                        {
                            conversation_id,
                            chatId: calcChatId,
                            sessionId,
                            sessionName: sessionName || sessionId,
                            phone: targetPhone,
                            phone_lid,
                            contactName: name || targetPhone,
                            after: after.toISOString(),
                            before: now.toISOString(),
                            direction: 'both',
                            batchSize: 50,
                            useBatchCreate: true
                        },
                        { headers: INGEST_TOKEN ? { 'x-api-token': INGEST_TOKEN } : {}, timeout: 300000 }
                    );

                    const synced = syncResp.data?.summary?.synced || syncResp.data?.synced || 0;
                    logToCollector('[INGEST_OUT] History synced for new contact', {
                        conversation_id,
                        synced,
                        contact_id: contact.id
                    });

                    // ★★★ V5.3.12修复：历史同步为0时，继续创建当前消息 ★★★
                    // 问题：WA上没有历史记录时，synced=0，但当前这条出站消息也不会被创建
                    // 导致 Chatwoot 显示"没有可用的内容"
                    if (synced > 0) {
                        // 历史同步已包含该消息，标记到 Redis 并返回
                        if (redis) {
                            await redis.set(redisKey, '1', 'EX', 7 * 24 * 3600).catch(() => {});
                        }
                        return res.json({ ok: true, conversation_id, synced, newConversation: true });
                    }
                    // synced === 0 时，继续往下创建当前消息
                    logToCollector('[INGEST_OUT] History sync returned 0, creating current message', {
                        conversation_id,
                        contact_id: contact.id
                    });
                } catch (syncErr) {
                    logToCollector('[INGEST_OUT] History sync failed, creating single message', {
                        error: syncErr.message
                    });
                    // 同步失败，继续创建单条消息
                }
            }
        }

        // 7. Chatwoot API 去重检查（第二层）
        if (typeof cw.checkMessageExists === 'function') {
            const existsInCW = await cw.checkMessageExists({
                account_id: CHATWOOT_ACCOUNT_ID,
                conversation_id,
                source_id: messageId
            });

            if (existsInCW) {
                logToCollector('[INGEST_OUT] Skip (Chatwoot)', {
                    messageId: messageId.substring(0, 35),
                    conversation_id
                });
                // 标记到 Redis 避免下次再查
                if (redis) {
                    await redis.set(redisKey, '1', 'EX', 7 * 24 * 3600);
                }
                return res.json({ ok: true, skipped: 'duplicate_chatwoot' });
            }
        }

        // 8. 处理附件（使用可能被增强的附件）
        const attachments = normalizeAttachments({ attachment: enhancedAttachment, messageId });

        // ★★★ 新增：重试标志 ★★★
        let retryAttempted = false;

        // ★★★ V5.3修复：改进重复检测逻辑 ★★★
        // 问题：空内容的消息（如图片无文字说明）被误判为重复
        // 解决：空内容或媒体消息应该用source_id判断，而不是content
        try {
            const recentMessages = await cw.getConversationMessages({
                account_id: CHATWOOT_ACCOUNT_ID,
                conversation_id,
                before: Date.now() + 60000  // 最近的消息
            });

            const messageContent = text || '';
            const nowTs = Date.now();
            const hasAttachment = attachments && attachments.length > 0;

            // ★★★ V5.3修复：空内容或媒体消息使用source_id判断重复 ★★★
            const duplicate = (recentMessages || []).find(msg => {
                const msgTs = msg.created_at * 1000;  // Chatwoot 时间戳是秒
                const timeDiff = Math.abs(nowTs - msgTs);
                const isOutgoing = msg.message_type === 1 || msg.message_type === 'outgoing';

                // 时间窗口内且是出站消息
                if (timeDiff >= 60000 || !isOutgoing) return false;

                // 1. 如果source_id已经匹配，一定是重复
                if (msg.source_id && msg.source_id === messageId) {
                    return true;
                }

                // 2. 空内容的消息（如图片、album）不能仅基于content判断重复
                //    必须同时检查source_id或wa_message_id
                if (!messageContent || messageContent.trim() === '') {
                    // 检查content_attributes中的wa_message_id
                    const existingWaId = msg.content_attributes?.wa_message_id || msg.source_id;
                    if (existingWaId && existingWaId === messageId) {
                        return true;
                    }
                    // 空内容不能判断为重复（避免图片丢失）
                    return false;
                }

                // 3. 有内容的消息：基于content判断
                const contentMatch = msg.content === messageContent ||
                    (msg.content && msg.content.includes(messageContent));
                return contentMatch;
            });

            if (duplicate) {
                logToCollector('[INGEST_OUT] SKIP_DUPLICATE', {
                    reason: 'recent message with same content exists',
                    existing_id: duplicate.id,
                    existing_source_id: duplicate.source_id?.substring(0, 30),
                    content_preview: messageContent.substring(0, 30)
                });

                // ★★★ V5.3修复：改用Redis存储消息映射（替代失败的API调用） ★★★
                if (messageId && duplicate.id) {
                    await saveMessageMapping(conversation_id, messageId, duplicate.id);
                }

                return res.json({ ok: true, skipped: 'duplicate_content', existing_id: duplicate.id });
            }
        } catch (dupCheckErr) {
            console.log(`[INGEST_OUT] Duplicate check failed (continuing): ${dupCheckErr.message}`);
        }

        // ★★★ 新增：日志确认 conversation_id ★★★
        logToCollector('[INGEST_OUT] Creating message', {
            conversation_id,
            contact_id: contact?.id,
            account_id: CHATWOOT_ACCOUNT_ID,
            hasAttachments: attachments.length > 0
        });

        // ★★★ V5.3.5修复：使用综合查找函数（Redis + source_id） ★★★
        let in_reply_to = null;
        if (quotedMsg && quotedMsg.id && conversation_id) {
            in_reply_to = await findCwMessageId(conversation_id, quotedMsg.id);
            if (in_reply_to) {
                console.log(`[INGEST_OUT] Found native reply: wa=${quotedMsg.id.substring(0, 30)} -> cw=${in_reply_to}`);
            }
        }

        // ★★★ V5.3.4修复：正确处理引用 ★★★
        // 关键：有原生引用时，finalText 只包含实际消息内容！
        let finalText = text || '';
        let quotedMessageData = null;

        if (quotedMsg) {
            // 保存引用消息的WhatsApp ID
            quotedMessageData = {
                wa_message_id: quotedMsg.id,
                fromMe: quotedMsg.fromMe,
                type: quotedMsg.type
            };

            // 只有没有原生引用时，才用文本模拟
            if (!in_reply_to) {
                let quotedBody = quotedMsg.body || quotedMsg.caption || '';
                if (!quotedBody && quotedMsg.type) {
                    const typeLabels = {
                        'image': '[图片]',
                        'video': '[视频]',
                        'audio': '[语音]',
                        'ptt': '[语音消息]',
                        'document': '[文件]',
                        'sticker': '[表情贴纸]',
                        'location': '[位置]',
                        'contact': '[联系人]',
                        'contact_card': '[名片]'
                    };
                    quotedBody = typeLabels[quotedMsg.type] || `[${quotedMsg.type || '媒体'}消息]`;
                }

                const quotedFrom = quotedMsg.fromMe ? '我' : '对方';
                const quotedText = (quotedBody || '').replace(/\n/g, ' ').substring(0, 40);
                finalText = `▎💬 ${quotedFrom}：${quotedText}\n\n${text || ''}`;
                console.log(`[INGEST_OUT] Fallback to text quote (no CW message ID found)`);
            }

            console.log(`[INGEST_OUT] Quote: id=${quotedMsg.id?.substring(0, 30)}, hasNativeReply=${!!in_reply_to}`);
        }

        // 9. 创建消息
        let created;
        try {
            if (attachments.length > 0) {
                // 有附件：使用 FormData
                const FormData = require('form-data');
                const form = new FormData();
                form.append('content', finalText);
                form.append('message_type', 'outgoing');
                form.append('private', 'false');
                form.append('source_id', messageId);  // ← 关键：保存 WA 消息ID

                // ★★★ V5.3.4修复：完整的content_attributes ★★★
                const content_attrs = {
                    wa_message_id: messageId,
                    synced_from_device: true
                };
                if (in_reply_to) {
                    content_attrs.in_reply_to = in_reply_to;
                }
                if (quotedMsg && quotedMsg.id) {
                    content_attrs.quoted_wa_message_id = quotedMsg.id;
                }
                form.append('content_attributes', JSON.stringify(content_attrs));

                for (const att of attachments) {
                    if (att.data_url) {
                        const m = att.data_url.match(/^data:(.+);base64,(.+)$/);
                        if (m) {
                            const buffer = Buffer.from(m[2], 'base64');
                            form.append('attachments[]', buffer, {
                                filename: att.filename || 'file',
                                contentType: m[1]
                            });
                        }
                    }
                }

                created = await cw.request(
                    'POST',
                    `/api/v1/accounts/${CHATWOOT_ACCOUNT_ID}/conversations/${conversation_id}/messages`,
                    form,
                    form.getHeaders()
                );
            } else {
                // 无附件：JSON 请求
                // ★★★ V5.3.4修复：完整的content_attributes ★★★
                const content_attrs = {
                    wa_message_id: messageId,
                    wa_timestamp: timestamp,
                    wa_type: type,
                    synced_from_device: true
                };
                // 添加原生引用
                if (in_reply_to) {
                    content_attrs.in_reply_to = in_reply_to;
                }
                // 保存被引用消息的WhatsApp ID
                if (quotedMsg && quotedMsg.id) {
                    content_attrs.quoted_wa_message_id = quotedMsg.id;
                }

                created = await cw.request(
                    'POST',
                    `/api/v1/accounts/${CHATWOOT_ACCOUNT_ID}/conversations/${conversation_id}/messages`,
                    {
                        content: finalText,
                        message_type: 'outgoing',
                        private: false,
                        source_id: messageId,  // ← 关键：保存 WA 消息ID
                        content_attributes: content_attrs
                    }
                );
            }
        } catch (e) {
            const errMsg = e?.message || '';
            const errResponse = e?.response?.data || e?._cw || {};

            // 检查是否是重复消息错误
            if (/source_id|duplicate/i.test(errMsg)) {
                logToCollector('[INGEST_OUT] Duplicate detected', { messageId: messageId.substring(0, 35) });
                if (redis) {
                    await redis.set(redisKey, '1', 'EX', 7 * 24 * 3600);
                }
                return res.json({ ok: true, skipped: 'duplicate_source_id' });
            }

            // ★★★ 新增：404 时清除缓存并重试 ★★★
            const is404 = e?.response?.status === 404 || /not.*found|Resource could not be found/i.test(errMsg);
            if (is404 && !retryAttempted) {
                logToCollector('[INGEST_OUT] 404 detected, clearing cache and retrying', {
                    conversation_id,
                    contact_id: contact?.id
                });

                // 清除缓存（包含 sessionId）
                const digits = (String(phone || phone_lid || '').match(/\d+/g) || []).join('');
                cw.clearAllCacheForDigits(digits, sessionId);

                // 标记已重试
                retryAttempted = true;

                // 重新获取联系人和会话
                contact = await cw.ensureContact({
                    account_id: CHATWOOT_ACCOUNT_ID,
                    rawPhone: phone,
                    rawPhone_lid: phone_lid,
                    rawName: name || to,
                    sessionId,
                    sessionName,
                    messageId
                });

                const inbox_id = await resolveInboxId(sessionId);  // ★ 传入 sessionId
                const newConv = await cw.ensureConversation({
                    account_id: CHATWOOT_ACCOUNT_ID,
                    inbox_id,
                    contact_id: contact.id
                });
                conversation_id = newConv?.id || newConv;

                logToCollector('[INGEST_OUT] Retry with new IDs', {
                    contact_id: contact?.id,
                    conversation_id,
                    isNew: newConv._isNew
                });

                // ★★★ 关键修复：404 重试时如果是新会话，也要触发历史同步 ★★★
                if (newConv._isNew && sessionId) {
                    const calcChatId = chatId || (phone ? `${String(phone).replace(/\D/g, '')}@c.us`
                        : phone_lid ? `${String(phone_lid).replace(/\D/g, '')}@lid`
                            : null);

                    if (calcChatId) {
                        logToCollector('[INGEST_OUT] Retry: New conversation -> sync history', {
                            conversation_id,
                            chatId: calcChatId
                        });

                        const HOURS = 12;
                        const now = new Date();
                        const after = new Date(now.getTime() - HOURS * 3600 * 1000);

                        try {
                            const syncResp = await axios.post(
                                `http://127.0.0.1:${PORT}/sync-messages`,
                                {
                                    conversation_id,
                                    chatId: calcChatId,
                                    sessionId,
                                    sessionName: sessionName || sessionId,
                                    phone: targetPhone,
                                    phone_lid,
                                    contactName: name || targetPhone,
                                    after: after.toISOString(),
                                    before: now.toISOString(),
                                    direction: 'both',
                                    batchSize: 50,
                                    useBatchCreate: true
                                },
                                { headers: INGEST_TOKEN ? { 'x-api-token': INGEST_TOKEN } : {}, timeout: 300000 }
                            );

                            const synced = syncResp.data?.summary?.synced || syncResp.data?.synced || 0;
                            logToCollector('[INGEST_OUT] Retry: History synced', { conversation_id, synced });

                            // ★★★ V5.3.12修复：历史同步为0时，继续创建当前消息 ★★★
                            if (synced > 0) {
                                if (redis) {
                                    await redis.set(redisKey, '1', 'EX', 7 * 24 * 3600).catch(() => {});
                                }
                                return res.json({ ok: true, conversation_id, synced, newConversation: true, retried: true });
                            }
                            // synced === 0 时，继续往下创建当前消息
                            logToCollector('[INGEST_OUT] Retry: History sync returned 0, creating current message', {
                                conversation_id
                            });
                        } catch (syncErr) {
                            logToCollector('[INGEST_OUT] Retry: Sync failed, creating single message', {
                                error: syncErr.message
                            });
                        }
                    }
                }

                // 重试创建单条消息（非新会话或同步失败）
                if (attachments.length > 0) {
                    const FormData = require('form-data');
                    const form = new FormData();
                    form.append('content', text || '');
                    form.append('message_type', 'outgoing');
                    form.append('private', 'false');
                    form.append('source_id', messageId);
                    for (const att of attachments) {
                        if (att.data_url) {
                            const m = att.data_url.match(/^data:(.+);base64,(.+)$/);
                            if (m) {
                                const buffer = Buffer.from(m[2], 'base64');
                                form.append('attachments[]', buffer, {
                                    filename: att.filename || 'file',
                                    contentType: m[1]
                                });
                            }
                        }
                    }
                    created = await cw.request(
                        'POST',
                        `/api/v1/accounts/${CHATWOOT_ACCOUNT_ID}/conversations/${conversation_id}/messages`,
                        form,
                        form.getHeaders()
                    );
                } else {
                    created = await cw.request(
                        'POST',
                        `/api/v1/accounts/${CHATWOOT_ACCOUNT_ID}/conversations/${conversation_id}/messages`,
                        {
                            content: text || '',
                            message_type: 'outgoing',
                            private: false,
                            source_id: messageId,
                            content_attributes: {
                                wa_message_id: messageId,
                                wa_timestamp: timestamp,
                                wa_type: type,
                                synced_from_device: true
                            }
                        }
                    );
                }
                // 重试成功，跳出 catch
            } else {
                // ★★★ 详细错误日志 ★★★
                logToCollector('[INGEST_OUT] Error', {
                    error: errResponse.error || errMsg,
                    conversation_id,
                    contact_id: contact?.id,
                    messageId: messageId?.substring(0, 35),
                    status: e?.response?.status
                });

                throw e;
            }
        }

        // 10. 标记到 Redis（7天TTL）
        if (redis) {
            await redis.set(redisKey, '1', 'EX', 7 * 24 * 3600);

            // 保存映射关系
            const mappingData = {
                sessionId,
                phone: phone || '',
                phone_lid: phone_lid || '',
                chatId: chatId || to,
                contact_id: contact.id,
                conversation_id,
                updatedAt: Date.now()
            };
            await redis.set(
                `cw:mapping:conv:${conversation_id}`,
                JSON.stringify(mappingData),
                'EX', 86400 * 30
            ).catch(() => {});
        }

        const duration = Date.now() - startTime;
        logToCollector('[INGEST_OUT] Created', {
            conversation_id,
            message_id: created?.id,
            duration
        });

        // ★★★ V5.3.11新增：标记消息已处理，并释放处理中锁 ★★★
        if (redis && messageId) {
            await redis.set(redisKey, '1', 'EX', 7 * 24 * 3600).catch(() => {});
            await redis.del(processingKey).catch(() => {});
        }

        // ★★★ V5.3新增：存储消息ID映射到Redis ★★★
        // 这样后续引用消息可以通过WhatsApp消息ID查找Chatwoot消息ID
        if (created?.id && messageId) {
            await saveMessageMapping(conversation_id, messageId, created.id);
        }

        // 新建联系人后同步历史消息
        if (conv._isNew && sessionId) {
            // ★ 修复：正确处理 @lid 的情况
            const calcChatId = chatId || (phone ? `${targetPhone}@c.us` : phone_lid ? `${targetPhone}@lid` : null);
            if (calcChatId) {
                logToCollector('[INGEST_OUT] New conversation, triggering history sync', { conversation_id, chatId: calcChatId });
                syncNewContactHistory({
                    sessionId,
                    sessionName: sessionName || sessionId,
                    chatId: calcChatId,
                    phone: phone || null,
                    phone_lid: phone_lid || null,
                    contactName: name || targetPhone,
                    conversation_id
                }).catch(e => logToCollector('[INGEST_OUT] History sync error', { error: e.message }));
            }
        }

        res.json({
            ok: true,
            conversation_id,
            message_id: created?.id,
            duration
        });

    } catch (e) {
        const errMsg = e?.response?.data?.error || e?.response?.data?.message || e?.message || String(e);
        const errData = e?.response?.data;

        // ★★★ V5.3.11新增：错误时也释放处理中锁 ★★★
        const messageId = req.body?.messageId;
        if (redis && messageId) {
            const processingKey = `wa:processing:outgoing:${messageId}`;
            await redis.del(processingKey).catch(() => {});
        }

        // 处理"已存在"错误
        if (/identifier.*already.*taken|already.*exists|duplicate/i.test(errMsg) ||
            /identifier.*already.*taken|already.*exists|duplicate/i.test(JSON.stringify(errData || {}))) {
            logToCollector('[INGEST_OUT] Already exists', { error: errMsg });
            return res.json({ ok: true, skipped: 'already_exists' });
        }

        logToCollector('[INGEST_OUT] Error', { error: errMsg });
        res.status(500).json({ ok: false, error: errMsg, details: errData });
    }
});

/**
 * POST /sync-messages
 * 同步 WhatsApp 消息到 Chatwoot（增强版 - 支持批量创建 + 启动同步）
 *
 * Body:
 * {
 *   conversation_id: 149,           // Chatwoot 会话 ID（与 isStartupSync 二选一）
 *   hours: 12,                       // 同步最近多少小时（默认 12）
 *   after: "2025-01-01T00:00:00Z",   // 或指定开始时间 ISO 格式
 *   before: "2025-01-01T12:00:00Z",  // 或指定结束时间 ISO 格式
 *   direction: "both",               // "incoming" | "outgoing" | "both"
 *   replace: false,                  // 是否删除 Chatwoot 中该时间段的消息再同步
 *   dryRun: false,                   // 测试模式，不实际执行
 *   messageIds: null,                // 只同步指定的消息 ID（用于重试）
 *   alignMessages: true,             // 是否启用消息对齐（默认 true）
 *   batchSize: 50,                   // 批量创建时每批数量（默认 50）
 *   useBatchCreate: true,            // 是否使用批量创建（默认 true）
 *
 *   // ★★★ 新增：启动同步参数 ★★★
 *   isStartupSync: false,            // 是否是启动同步模式
 *   chatId: null,                    // WhatsApp 聊天 ID（如 85270360156@c.us）
 *   sessionId: null,                 // WhatsApp 会话 ID
 *   sessionName: null,               // WhatsApp 会话名称
 *   phone: null,                     // 电话号码
 *   phone_lid: null,                 // LID 格式的电话
 *   contactName: null                // 联系人名称
 * }
 */
app.post('/sync-messages', async (req, res) => {
    let conversation_id;
    try {
        const {
            conversation_id: convId,
            hours = 12,
            after,
            before,
            direction = 'both',
            replace = false,
            dryRun = false,
            messageIds = null,
            alignMessages = true,
            batchSize = 50,
            useBatchCreate = true,

            // ★★★ 新增：启动同步参数 ★★★
            isStartupSync = false,
            chatId: inputChatId = null,
            sessionId: inputSessionId = null,
            sessionName: inputSessionName = null,
            phone: inputPhone = null,
            phone_lid: inputPhoneLid = null,
            contactName: inputContactName = null
        } = req.body || {};

        conversation_id = convId;

        logToCollector('[SYNC] Start', {
            conversation_id, hours, after, before, direction, dryRun,
            messageIds: messageIds?.length || 0,
            alignMessages,
            batchSize,
            useBatchCreate,
            isStartupSync,
            chatId: inputChatId,
            sessionId: inputSessionId
        });

        // ★★★ 新增：启动同步模式 - 通过 chatId/sessionId 查找或创建会话 ★★★
        let startupSyncMapping = null;
        if (!conversation_id && isStartupSync && inputChatId && inputSessionId) {
            logToCollector('[SYNC] Startup sync mode - finding/creating conversation', {
                chatId: inputChatId,
                sessionId: inputSessionId,
                phone: inputPhone,
                phone_lid: inputPhoneLid,
                contactName: inputContactName
            });

            try {
                const inbox_id = await resolveInboxId(inputSessionId);  // ★ 传入 sessionId

                // ★★★ 简化：直接调用 ensureContact/ensureConversation ★★★
                // 它们内部已经有查找已存在联系人/会话的逻辑，不需要 Redis 缓存查找
                logToCollector('[SYNC] Finding/creating contact and conversation', {
                    inbox_id,
                    phone: inputPhone,
                    phone_lid: inputPhoneLid,
                    name: inputContactName,
                    sessionId: inputSessionId
                });

                // 1. 创建或查找联系人
                let contact;
                try {
                    const rawContact = await cw.ensureContact({
                        account_id: CHATWOOT_ACCOUNT_ID,
                        rawPhone: inputPhone,
                        rawPhone_lid: inputPhoneLid,
                        rawName: inputContactName,
                        sessionId: inputSessionId,
                        sessionName: inputSessionName
                    });

                    // 解析返回值
                    if (rawContact?.payload?.contact?.id) {
                        contact = rawContact.payload.contact;
                    } else if (rawContact?.id) {
                        contact = rawContact;
                    } else {
                        contact = rawContact;
                    }
                } catch (contactErr) {
                    logToCollector('[SYNC] ensureContact error', { error: contactErr?.message });
                    throw contactErr;
                }

                if (!contact || !contact.id) {
                    throw new Error('ensureContact failed - contact.id is undefined');
                }

                logToCollector('[SYNC] Contact ensured', { contact_id: contact.id, name: contact.name });

                // 2. 创建或查找会话
                let conv;
                try {
                    conv = await cw.ensureConversation({
                        account_id: CHATWOOT_ACCOUNT_ID,
                        inbox_id,
                        contact_id: contact.id
                    });
                } catch (convErr) {
                    logToCollector('[SYNC] ensureConversation error', { error: convErr?.message });
                    throw convErr;
                }

                const newConvId = conv?.id || conv;
                if (!newConvId) {
                    throw new Error('ensureConversation failed - conversation.id is undefined');
                }

                conversation_id = newConvId;
                logToCollector('[SYNC] Conversation ensured', { conversation_id, contact_id: contact.id });

                // 3. 保存联系人信息到 Redis（仅用于存储，不用于查找 conversation_id）
                if (redis) {
                    const mappingData = {
                        sessionId: inputSessionId,
                        sessionName: inputSessionName,
                        phone: inputPhone || '',
                        phone_lid: inputPhoneLid || '',
                        chatId: inputChatId,
                        contact_id: contact.id,
                        conversation_id,
                        updatedAt: Date.now()
                    };
                    await redis.set(`cw:mapping:wa:${inputChatId}`, JSON.stringify(mappingData), 'EX', 86400 * 30).catch(() => {});
                }

                // 设置启动同步映射
                startupSyncMapping = {
                    sessionId: inputSessionId,
                    chatId: inputChatId,
                    phone: inputPhone,
                    phone_lid: inputPhoneLid
                };

            } catch (e) {
                logToCollector('[SYNC] Startup sync conversation setup failed', { error: e?.message });
                return res.status(500).json({
                    ok: false,
                    error: `Failed to setup conversation: ${e?.message}`,
                    isStartupSync: true
                });
            }
        }

        if (!conversation_id) {
            return res.status(400).json({
                ok: false,
                error: 'conversation_id required (or provide chatId+sessionId for startup sync)'
            });
        }

        // 1. 获取会话详情
        let conv = null;
        try {
            conv = await cw.getConversationDetails(CHATWOOT_ACCOUNT_ID, conversation_id);
        } catch (convErr) {
            logToCollector('[SYNC] Conversation not found', { conversation_id, error: convErr?.message });
            return res.status(404).json({
                ok: false,
                error: `Conversation ${conversation_id} not found: ${convErr?.message}`
            });
        }

        if (!conv) {
            return res.status(404).json({ ok: false, error: 'Conversation not found' });
        }

        const sender = conv.meta?.sender || {};
        const inboxId = conv.inbox_id;

        logToCollector('[SYNC] Conversation', {
            contact_id: sender.id,
            contact_name: sender.name,
            inbox_id: inboxId
        });

        // 2. 获取 WhatsApp 映射
        // ★★★ 修改：启动同步模式优先使用传入的参数 ★★★
        let waMapping = startupSyncMapping;

        // ★★★ 关键修复：如果传入了 chatId/sessionId 参数，直接使用 ★★★
        if (!waMapping && inputChatId && inputSessionId) {
            waMapping = {
                sessionId: inputSessionId,
                chatId: inputChatId,
                phone: inputPhone,
                phone_lid: inputPhoneLid
            };
            logToCollector('[SYNC] Using provided chatId/sessionId', waMapping);
        }

        if (!waMapping && redis) {
            const keys = [
                `cw:mapping:conv:${conversation_id}`,
                `cw:mapping:contact:${sender.id}`,
                `wa:conv:${conversation_id}`
            ];

            for (const key of keys) {
                const data = await redis.get(key);
                if (data) {
                    try {
                        waMapping = JSON.parse(data);
                        logToCollector('[SYNC] Found mapping', { key, data: waMapping });
                        break;
                    } catch (_) {}
                }
            }
        }

        // 如果没有映射，尝试从联系人信息推断
        if (!waMapping) {
            const phone = sender.phone_number?.replace(/[^\d]/g, '') ||
                sender.identifier?.replace(/[^\d]/g, '');

            if (phone) {
                // ★★★ 修复：检查 identifier 是否包含 @lid 来决定格式 ★★★
                const isLid = sender.identifier?.includes('@lid') ||
                    sender.identifier?.includes(':lid:') ||
                    (inputChatId && inputChatId.includes('@lid'));

                waMapping = {
                    sessionId: inputSessionId || WA_DEFAULT_SESSION.split(',')[0]?.trim(),
                    phone: isLid ? null : phone,
                    phone_lid: isLid ? phone : null,
                    chatId: isLid ? `${phone}@lid` : `${phone}@c.us`
                };
                logToCollector('[SYNC] Inferred mapping from phone', waMapping);
            }
        }

        if (!waMapping?.sessionId) {
            return res.status(400).json({
                ok: false,
                error: 'No WhatsApp mapping found',
                hint: 'Contact needs to have exchanged messages first'
            });
        }

        const { sessionId, chatId, phone, phone_lid } = waMapping;
        const waChatId = chatId || (phone ? `${phone}@c.us` : (phone_lid ? `${phone_lid}@lid` : null));

        if (!waChatId) {
            return res.status(400).json({ ok: false, error: 'Cannot determine WhatsApp chat ID' });
        }

        // 3. 计算时间范围
        let afterTs, beforeTs;
        const nowTs = Math.floor(Date.now() / 1000);

        if (after) {
            afterTs = Math.floor(new Date(after).getTime() / 1000);
        } else {
            afterTs = nowTs - hours * 3600;
        }

        if (before) {
            beforeTs = Math.floor(new Date(before).getTime() / 1000);
        } else {
            beforeTs = nowTs;
        }

        const afterISO = new Date(afterTs * 1000).toISOString();
        const beforeISO = new Date(beforeTs * 1000).toISOString();

        logToCollector('[SYNC] Time range', { after: afterISO, before: beforeISO });

        // 4. 从 WhatsApp 获取消息
        const waUrl = `${WA_BRIDGE_URL}/messages-sync/${sessionId}/${encodeURIComponent(waChatId)}`;
        const waParams = new URLSearchParams({
            limit: '99999',
            after: String(afterTs),
            before: String(beforeTs),
            includeMedia: '1',
            includeBase64: '1'
        });

        const waHeaders = {};
        if (WA_BRIDGE_TOKEN) waHeaders['x-api-token'] = WA_BRIDGE_TOKEN;

        let waMessages = [];
        let waFetchError = null;  // ★★★ V5新增：记录错误状态 ★★★

        try {
            const waResp = await axios.get(`${waUrl}?${waParams}`, {
                headers: waHeaders,
                timeout: 1800000
            });

            // ★★★ V5修复：检查返回值是否表示错误 ★★★
            if (waResp.data?.ok === false) {
                waFetchError = waResp.data?.error || 'Unknown error';
                logToCollector('[SYNC] WA returned error', {
                    error: waFetchError,
                    retryable: waResp.data?.retryable
                });

                // 如果是可重试的错误（如 Store 未准备好），直接返回错误
                if (waResp.data?.retryable) {
                    return res.status(503).json({
                        ok: false,
                        error: `WhatsApp temporarily unavailable: ${waFetchError}`,
                        retryable: true
                    });
                }
            }

            waMessages = waResp.data?.messages || [];
            logToCollector('[SYNC] WA messages', { count: waMessages.length });
        } catch (e) {
            const statusCode = e?.response?.status;
            const errorData = e?.response?.data;

            logToCollector('[SYNC] WA fetch error', {
                error: e?.message,
                status: statusCode,
                chatId: waChatId,
                retryable: errorData?.retryable
            });

            // ★★★ V5修复：503 错误表示临时不可用，不应该删除消息 ★★★
            if (statusCode === 503 || errorData?.retryable) {
                return res.status(503).json({
                    ok: false,
                    error: `WhatsApp temporarily unavailable: ${errorData?.error || e?.message}`,
                    retryable: true
                });
            }

            // 其他错误（如聊天不存在），设置为空数组但记录错误
            waFetchError = e?.message;
            waMessages = [];
            logToCollector('[SYNC] Continuing with empty WA messages (non-retryable error)');
        }

        // 5. 获取 Chatwoot 现有消息（使用批量查询 API）
        let cwMessages = [];
        try {
            cwMessages = await cw.batchQueryMessages({
                account_id: CHATWOOT_ACCOUNT_ID,
                conversation_id,
                after: afterISO,
                before: beforeISO
            });
            logToCollector('[SYNC] CW messages (batch API)', { count: cwMessages.length });
        } catch (e) {
            logToCollector('[SYNC] CW batch query failed, fallback', { error: e?.message });
            try {
                cwMessages = await cw.getConversationMessages({
                    account_id: CHATWOOT_ACCOUNT_ID,
                    conversation_id,
                    after: afterTs * 1000,
                    before: beforeTs * 1000
                });
                logToCollector('[SYNC] CW messages (fallback)', { count: cwMessages.length });
            } catch (e2) {
                logToCollector('[SYNC] CW fetch error', { error: e2?.message });
            }
        }

        // 6. 消息对齐逻辑
        let timeOffset = 0;
        let alignmentInfo = null;

        if (alignMessages && cwMessages.length > 0 && waMessages.length > 0) {
            logToCollector('[SYNC] Starting message alignment');
            const offsetResult = cw.calculateTimeOffset(cwMessages, waMessages);
            timeOffset = offsetResult.offset;
            alignmentInfo = offsetResult;

            logToCollector('[SYNC] Time offset calculated', {
                offset: timeOffset,
                confidence: offsetResult.confidence,
                samples: offsetResult.samples
            });

            // 验证首尾消息
            if (waMessages.length > 0) {
                const firstWa = waMessages[0];
                const lastWa = waMessages[waMessages.length - 1];
                const firstWaAdjustedTs = firstWa.timestamp + timeOffset;
                const lastWaAdjustedTs = lastWa.timestamp + timeOffset;

                const matchedFirst = cw.findMessageByContentAndTime(cwMessages, firstWa.body, firstWaAdjustedTs, 60);
                const matchedLast = cw.findMessageByContentAndTime(cwMessages, lastWa.body, lastWaAdjustedTs, 60);

                alignmentInfo.firstMatch = !!matchedFirst;
                alignmentInfo.lastMatch = !!matchedLast;

                logToCollector('[SYNC] Alignment verification', {
                    firstWaContent: (firstWa.body || '').substring(0, 30),
                    firstMatch: !!matchedFirst,
                    lastWaContent: (lastWa.body || '').substring(0, 30),
                    lastMatch: !!matchedLast
                });
            }
        }

        // 7. 比对并找出需要同步的消息
        const cwSourceIds = new Set(cwMessages.map(m => m.source_id).filter(Boolean));
        const cwContentIndex = new Map();

        for (const m of cwMessages) {
            const ts = m.created_at_unix || Math.floor(new Date(m.created_at).getTime() / 1000);
            const contentKey = `${ts}_${(m.content || '').trim().toLowerCase().substring(0, 50)}`;
            cwContentIndex.set(contentKey, m);
        }

        const messageIdSet = messageIds && Array.isArray(messageIds) && messageIds.length > 0
            ? new Set(messageIds)
            : null;

        const toSync = [];
        const skipped = [];

        for (const waMsg of waMessages) {
            // 如果指定了 messageIds，只处理这些消息
            if (messageIdSet && !messageIdSet.has(waMsg.id)) {
                skipped.push({ id: waMsg.id, reason: 'not_in_retry_list' });
                continue;
            }

            // 检查 source_id 是否已存在
            if (!messageIdSet && cwSourceIds.has(waMsg.id)) {
                skipped.push({ id: waMsg.id, reason: 'source_id_exists' });
                continue;
            }

            // 检查内容+时间是否已存在
            const adjustedTs = waMsg.timestamp + timeOffset;
            const contentKey = `${adjustedTs}_${(waMsg.body || '').trim().toLowerCase().substring(0, 50)}`;

            if (!messageIdSet && cwContentIndex.has(contentKey)) {
                skipped.push({ id: waMsg.id, reason: 'content_time_match' });
                continue;
            }

            // 根据方向过滤
            if (direction === 'incoming' && waMsg.fromMe) {
                skipped.push({ id: waMsg.id, reason: 'direction_filter' });
                continue;
            }
            if (direction === 'outgoing' && !waMsg.fromMe) {
                skipped.push({ id: waMsg.id, reason: 'direction_filter' });
                continue;
            }

            toSync.push(waMsg);
        }

        logToCollector('[SYNC] Analysis', {
            waTotal: waMessages.length,
            cwTotal: cwMessages.length,
            toSync: toSync.length,
            skipped: skipped.length,
            timeOffset,
            alignment: alignmentInfo
        });

        // Dry run 模式
        if (dryRun) {
            return res.json({
                ok: true,
                dryRun: true,
                summary: {
                    waMessages: waMessages.length,
                    cwMessages: cwMessages.length,
                    toSync: toSync.length,
                    skipped: skipped.length,
                    timeOffset,
                    alignment: alignmentInfo
                },
                messagesToSync: toSync.slice(0, 20).map(m => ({
                    id: m.id,
                    fromMe: m.fromMe,
                    type: m.type,
                    body: (m.body || '').substring(0, 100),
                    timestamp: m.timestamp
                })),
                skipped: skipped.slice(0, 20)
            });
        }

        // 8. 如果需要替换，使用批量删除
        // ★★★ V5修复：如果 WA 返回空数组但 CW 有消息，跳过删除以避免数据丢失 ★★★
        if (replace && cwMessages.length > 0) {
            // 安全检查：如果 WA 为空但 CW 有消息，不要删除（可能是获取失败）
            if (waMessages.length === 0 && cwMessages.length > 0) {
                logToCollector('[SYNC] SAFETY: Skipping delete - WA empty but CW has messages', {
                    cwCount: cwMessages.length,
                    waFetchError: waFetchError || 'none'
                });
                // 不删除，直接返回
                return res.json({
                    ok: true,
                    synced: 0,
                    failed: 0,
                    skipped: cwMessages.length,
                    note: 'Skipped delete due to empty WA response (safety measure)'
                });
            }

            const msgIdsToDelete = cwMessages.map(m => m.id).filter(Boolean);

            logToCollector('[SYNC] Replace mode - batch deleting CW messages', {
                count: cwMessages.length,
                messageIds: msgIdsToDelete.slice(0, 10)
            });

            try {
                if (msgIdsToDelete.length > 0) {
                    const deleteResult = await cw.batchDeleteMessages({
                        account_id: CHATWOOT_ACCOUNT_ID,
                        conversation_id,
                        message_ids: msgIdsToDelete,
                        hard_delete: true
                    });

                    logToCollector('[SYNC] Batch delete by IDs complete', {
                        requested: msgIdsToDelete.length,
                        deleted: deleteResult.deleted,
                        failed: deleteResult.failed
                    });
                } else {
                    const deleteResult = await cw.batchDeleteMessages({
                        account_id: CHATWOOT_ACCOUNT_ID,
                        conversation_id,
                        after: afterISO,
                        before: beforeISO,
                        hard_delete: true
                    });

                    logToCollector('[SYNC] Batch delete by time range complete', {
                        deleted: deleteResult.deleted,
                        failed: deleteResult.failed
                    });
                }
            } catch (e) {
                logToCollector('[SYNC] Batch delete failed', { error: e?.message });
            }

            // 替换模式下，同步所有 WA 消息
            toSync.length = 0;
            toSync.push(...waMessages.filter(m => {
                if (direction === 'incoming' && m.fromMe) return false;
                if (direction === 'outgoing' && !m.fromMe) return false;
                return true;
            }));
        }

        // 9. 按时间顺序排序
        toSync.sort((a, b) => a.timestamp - b.timestamp);

        // 【Bug 3 修复】使用 syncLockManager 设置同步锁
        const lockResult = syncLockManager.acquireLock(conversation_id);
        if (!lockResult.success) {
            logToCollector('[SYNC] Rejected - sync already in progress', {
                conversation_id,
                reason: lockResult.reason,
                elapsed: lockResult.elapsed
            });
            return res.status(429).json({
                ok: false,
                error: '同步正在进行中，请稍后再试',
                retryAfter: Math.max(30 - (lockResult.elapsed || 0), 10)
            });
        }
        const syncLock = lockResult.lock;
        logToCollector('[SYNC] Lock acquired', { conversation_id, lockId: syncLock.lockId });

        let syncResults = [];
        let successCount = 0;
        let failedCount = 0;
        let skippedCount = 0;

        // ========== V5.3.13修复：简化同步逻辑，依靠fallbackBatchCreate实时查找 ==========
        if (useBatchCreate && toSync.length > 0) {
            logToCollector('[SYNC] Using batch create with realtime lookup', {
                total: toSync.length,
                batchSize
            });

            syncLockManager.setMessageCount(conversation_id, toSync.length);

            // 预处理消息：下载媒体文件
            const preparedMessages = [];

            for (const waMsg of toSync) {
                // ★★★ V5.3.13修复：不传conversation_id，不提前查找in_reply_to ★★★
                const prepared = await prepareMessageForBatch(waMsg, WA_BRIDGE_URL, WA_BRIDGE_TOKEN, null);
                if (prepared) {
                    preparedMessages.push(prepared);
                }
            }

            logToCollector('[SYNC] Messages prepared', {
                total: toSync.length,
                prepared: preparedMessages.length
            });

            // 分批创建
            for (let i = 0; i < preparedMessages.length; i += batchSize) {
                const batch = preparedMessages.slice(i, i + batchSize);
                const batchNum = Math.floor(i / batchSize) + 1;
                const totalBatches = Math.ceil(preparedMessages.length / batchSize);

                logToCollector('[SYNC] Processing batch', {
                    batch: batchNum,
                    total: totalBatches,
                    size: batch.length
                });

                try {
                    // ★★★ V5.3.13修复：传入findCwMessageId函数，实时查找in_reply_to ★★★
                    const result = await cw.batchCreateMessages({
                        account_id: CHATWOOT_ACCOUNT_ID,
                        conversation_id,
                        messages: batch,
                        findCwMessageIdFn: findCwMessageId  // 实时查找函数
                    });

                    successCount += result.created || 0;
                    failedCount += result.failed || 0;
                    skippedCount += result.skipped || 0;

                    // ★★★ V5.3新增：存储消息ID映射到Redis ★★★
                    if (result.created_mappings?.length > 0) {
                        await batchSaveMessageMappings(conversation_id, result.created_mappings);
                    }

                    // ★★★ V5新增：标记已同步的消息到 Redis ★★★
                    if (redis) {
                        for (const msg of batch) {
                            if (msg.source_id) {
                                await redis.set(`wa:synced:incoming:${msg.source_id}`, '1', 'EX', 7 * 24 * 3600).catch(() => {});
                            }
                        }
                    }

                    logToCollector('[SYNC] Batch complete', {
                        batch: batchNum,
                        created: result.created,
                        failed: result.failed,
                        skipped: result.skipped,
                        mappings: result.created_mappings?.length || 0
                    });

                } catch (e) {
                    logToCollector('[SYNC] Batch create failed, fallback to single', {
                        batch: batchNum,
                        error: e?.message
                    });

                    // 降级到逐条创建
                    for (const msg of batch) {
                        try {
                            const result = await createSingleMessage(CHATWOOT_ACCOUNT_ID, conversation_id, msg);
                            successCount++;
                            syncResults.push({ source_id: msg.source_id, success: true });

                            // ★★★ V5.3新增：存储消息ID映射 ★★★
                            if (result?.id && msg.source_id) {
                                await saveMessageMapping(conversation_id, msg.source_id, result.id);
                            }

                            // ★★★ V5新增：标记已同步的消息到 Redis ★★★
                            if (redis && msg.source_id) {
                                await redis.set(`wa:synced:incoming:${msg.source_id}`, '1', 'EX', 7 * 24 * 3600).catch(() => {});
                            }
                        } catch (err) {
                            const errMsg = err?.message || '';
                            if (errMsg.includes('duplicate') || errMsg.includes('same_second')) {
                                skippedCount++;
                                syncResults.push({ source_id: msg.source_id, success: true, note: 'duplicate' });
                            } else {
                                failedCount++;
                                syncResults.push({ source_id: msg.source_id, success: false, error: errMsg });
                            }
                        }
                        await new Promise(r => setTimeout(r, 50));
                    }
                }

                // 批次间延迟
                if (i + batchSize < preparedMessages.length) {
                    await new Promise(r => setTimeout(r, 200));
                }
            }

        } else if (toSync.length > 0) {
            // ========== 原有逻辑：逐条同步 ==========
            logToCollector('[SYNC] Using single create', { total: toSync.length });

            for (const waMsg of toSync) {
                const msgKey = `${waMsg.body || ''}`.substring(0, 100);
                syncLock.msgIds.add(msgKey);

                let lastError = null;
                let success = false;

                for (let attempt = 1; attempt <= 3; attempt++) {
                    try {
                        const syncResult = await syncOneMessage({
                            account_id: CHATWOOT_ACCOUNT_ID,
                            conversation_id,
                            message: waMsg,
                            waBridgeUrl: WA_BRIDGE_URL,
                            waBridgeToken: WA_BRIDGE_TOKEN
                        });
                        success = true;
                        successCount++;
                        syncResults.push({ id: waMsg.id, success: true });

                        // ★★★ V5.3新增：存储消息ID映射 ★★★
                        if (syncResult?.cwMessageId && waMsg.id) {
                            await saveMessageMapping(conversation_id, waMsg.id, syncResult.cwMessageId);
                        }

                        // ★★★ V5新增：标记已同步的消息到 Redis ★★★
                        if (redis && waMsg.id) {
                            await redis.set(`wa:synced:incoming:${waMsg.id}`, '1', 'EX', 7 * 24 * 3600).catch(() => {});
                        }
                        break;
                    } catch (e) {
                        lastError = e?.message || '';

                        if (lastError.includes('record_invalid') ||
                            lastError.includes('duplicate') ||
                            lastError.includes('same_second') ||
                            lastError.includes('Duplicate message')) {
                            success = true;
                            skippedCount++;
                            syncResults.push({ id: waMsg.id, success: true, note: 'duplicate_skipped' });

                            // ★★★ V5新增：重复消息也标记为已同步 ★★★
                            if (redis && waMsg.id) {
                                await redis.set(`wa:synced:incoming:${waMsg.id}`, '1', 'EX', 7 * 24 * 3600).catch(() => {});
                            }
                            break;
                        }

                        if (attempt < 3) {
                            await new Promise(r => setTimeout(r, 500 * attempt));
                        }
                    }
                }

                if (!success) {
                    failedCount++;
                    syncResults.push({ id: waMsg.id, success: false, error: lastError });
                }

                await new Promise(r => setTimeout(r, 80));
            }
        }

        logToCollector('[SYNC] Complete', {
            synced: successCount,
            failed: failedCount,
            skipped: skippedCount
        });

        // ★★★ 新增：启动同步时根据消息时间标记已读/未读 ★★★
        let recentUnreadCount = 0;
        if (isStartupSync && successCount > 0) {
            const UNREAD_THRESHOLD_MS = 2 * 60 * 1000;  // 2分钟
            const nowMs = Date.now();

            // 统计2分钟内的来信消息数量
            for (const waMsg of toSync) {
                const msgTimestampMs = waMsg.timestamp * 1000;
                const isRecent = (nowMs - msgTimestampMs) < UNREAD_THRESHOLD_MS;
                const isIncoming = !waMsg.fromMe;

                if (isRecent && isIncoming) {
                    recentUnreadCount++;
                }
            }

            if (recentUnreadCount > 0) {
                logToCollector('[SYNC] Marking conversation as unread (recent messages)', {
                    conversation_id,
                    recentUnreadCount
                });

                // 有2分钟内的消息 → 设为 open（未读）
                try {
                    await axios.post(
                        `${process.env.CHATWOOT_BASE_URL}/api/v1/accounts/${CHATWOOT_ACCOUNT_ID}/conversations/${conversation_id}/toggle_status`,
                        { status: 'open' },
                        {
                            headers: {
                                'api_access_token': CHATWOOT_TOKEN,
                                'Content-Type': 'application/json'
                            },
                            timeout: 5000
                        }
                    ).catch(() => {});
                } catch (_) {}
            }
            // ★★★ 修复：移除 resolved 标记和私有消息 ★★★
            // 不再自动标记为已解决，不再发送任何系统消息
            // 用户体验：同步过程完全透明，无任何干扰

            logToCollector('[SYNC] Startup sync completed', {
                conversation_id,
                synced: successCount,
                recentUnread: recentUnreadCount
            });
        }

        // ★★★ V5.3.2修复：同步完成后等待 3 秒再释放锁（之前是30秒）★★★
        syncLockManager.markComplete(conversation_id, 3000);

        res.json({
            ok: true,
            summary: {
                waMessages: waMessages.length,
                cwMessages: cwMessages.length,
                synced: successCount,
                failed: failedCount,
                skipped: skippedCount + skipped.length,
                timeOffset,
                alignment: alignmentInfo,
                usedBatchCreate: useBatchCreate,
                isStartupSync: isStartupSync,           // ★★★ 新增 ★★★
                recentUnread: recentUnreadCount         // ★★★ 新增 ★★★
            },
            results: syncResults.slice(0, 100)
        });

    } catch (e) {
        if (typeof conversation_id !== 'undefined') {
            syncLockManager.forceRelease(conversation_id);
        }
        logToCollector('[SYNC] Error', { error: e?.message, stack: e?.stack });
        res.status(500).json({ ok: false, error: e?.message });
    }
});
/**
 * POST /startup-sync
 *
 * 启动时批量同步多个联系人的历史消息到 Chatwoot
 *
 * 关键特性：
 * 1. 使用 syncLockManager 阻止同步期间的 Chatwoot 回调
 * 2. 基于每个联系人最后一条消息的时间往前推 N 小时
 * 3. 替换现有消息（如果联系人已存在）
 * 4. 2分钟内的消息设为未读
 *
 * 请求体：
 * {
 *   contacts: [
 *     {
 *       chatId: "6281234567890@c.us",
 *       phone: "6281234567890",
 *       phone_lid: "",
 *       name: "John Doe",
 *       sessionId: "k17se6o0",
 *       sessionName: "Briana",
 *       messages: [
 *         { id: "xxx", body: "hello", fromMe: false, timestamp: 1234567890, ... },
 *         ...
 *       ],
 *       lastMsgTimestamp: 1234567890,
 *       cutoffTimestamp: 1234524690
 *     },
 *     ...
 *   ],
 *   replace: true,
 *   markRecentAsUnread: true
 * }
 */
app.post('/startup-sync', async (req, res) => {
    const startTime = Date.now();

    try {
        const {
            contacts = [],
            replace = true,
            markRecentAsUnread = true,
            batchSize = 50,
            delayMs = 100
        } = req.body || {};

        if (!contacts || !Array.isArray(contacts) || contacts.length === 0) {
            return res.status(400).json({ ok: false, error: 'Missing contacts array' });
        }

        logToCollector('[STARTUP_SYNC] Start', {
            contactCount: contacts.length,
            totalMessages: contacts.reduce((sum, c) => sum + (c.messages?.length || 0), 0),
            replace,
            markRecentAsUnread
        });

        // ★★★ 移除：不再在这里预先解析 inbox_id ★★★
        // const inbox_id = await resolveInboxId();

        const results = {
            processed: 0,
            skipped: 0,
            created: 0,
            replaced: 0,
            failed: 0,
            errors: [],
            contacts: []
        };

        // 2分钟阈值（毫秒）
        const UNREAD_THRESHOLD_MS = 2 * 60 * 1000;
        const nowMs = Date.now();

        // 逐个联系人处理
        for (const contactData of contacts) {
            const {
                chatId,
                phone,
                phone_lid,
                name,
                sessionId,
                sessionName,
                messages = [],
                lastMsgTimestamp,
                cutoffTimestamp
            } = contactData;

            if (!messages || messages.length === 0) {
                results.skipped++;
                continue;
            }

            const contactResult = {
                chatId,
                phone: phone || phone_lid,
                messageCount: messages.length,
                status: 'pending'
            };

            try {
                logToCollector('[STARTUP_SYNC] Processing contact', {
                    chatId,
                    phone: phone || phone_lid,
                    messageCount: messages.length,
                    sessionId
                });

                // 1. 确保联系人存在
                const contact = await cw.ensureContact({
                    account_id: CHATWOOT_ACCOUNT_ID,
                    rawPhone: phone,
                    rawPhone_lid: phone_lid,
                    rawName: name,
                    sessionId,
                    sessionName
                });

                // ★★★ 新增：每个联系人根据 sessionId 解析 inbox ★★★
                const inbox_id = await resolveInboxId(sessionId);

                // 2. 确保会话存在
                const conv = await cw.ensureConversation({
                    account_id: CHATWOOT_ACCOUNT_ID,
                    inbox_id,
                    contact_id: contact.id
                });
                const conversation_id = conv.id || conv;

                contactResult.contact_id = contact.id;
                contactResult.conversation_id = conversation_id;

                // 3. 使用 syncLockManager 获取锁
                const lockResult = syncLockManager.acquireLock(conversation_id);
                if (!lockResult.success) {
                    logToCollector('[STARTUP_SYNC] Lock failed', {
                        conversation_id,
                        reason: lockResult.reason
                    });
                    contactResult.status = 'lock_failed';
                    contactResult.error = lockResult.reason;
                    results.errors.push({ chatId, error: 'sync_lock_failed' });
                    results.contacts.push(contactResult);
                    continue;
                }

                try {
                    // 设置消息数量
                    syncLockManager.setMessageCount(conversation_id, messages.length);

                    // 4. 如果需要替换，先删除现有消息
                    if (replace && lastMsgTimestamp && cutoffTimestamp) {
                        const afterISO = new Date(cutoffTimestamp * 1000).toISOString();
                        const beforeISO = new Date(lastMsgTimestamp * 1000).toISOString();

                        // 获取现有消息
                        let existingMessages = [];
                        try {
                            existingMessages = await cw.batchQueryMessages({
                                account_id: CHATWOOT_ACCOUNT_ID,
                                conversation_id,
                                after: afterISO,
                                before: beforeISO
                            });
                        } catch (e) {
                            // 回退到普通查询
                            try {
                                existingMessages = await cw.getConversationMessages({
                                    account_id: CHATWOOT_ACCOUNT_ID,
                                    conversation_id,
                                    after: cutoffTimestamp * 1000,
                                    before: lastMsgTimestamp * 1000
                                });
                            } catch (_) {}
                        }

                        // 删除现有消息
                        if (existingMessages.length > 0) {
                            const messageIds = existingMessages.map(m => m.id).filter(Boolean);

                            logToCollector('[STARTUP_SYNC] Deleting existing messages', {
                                conversation_id,
                                count: messageIds.length
                            });

                            try {
                                await cw.batchDeleteMessages({
                                    account_id: CHATWOOT_ACCOUNT_ID,
                                    conversation_id,
                                    message_ids: messageIds,
                                    hard_delete: true
                                });
                                results.replaced += messageIds.length;
                            } catch (delErr) {
                                logToCollector('[STARTUP_SYNC] Delete failed', {
                                    conversation_id,
                                    error: delErr?.message
                                });
                            }
                        }
                    }

                    // 5. 准备消息数据
                    const preparedMessages = messages.map(m => {
                        const msgTimestampMs = m.timestamp * 1000;
                        const isRecent = (nowMs - msgTimestampMs) < UNREAD_THRESHOLD_MS;

                        // ★★★ V5.1修复：不再把引用内容合并到消息体 ★★★
                        let content = m.body || '';

                        return {
                            content: content,
                            message_type: m.fromMe ? 1 : 0,  // 1=outgoing, 0=incoming
                            source_id: m.id,
                            private: false,
                            content_attributes: {
                                wa_timestamp: m.timestamp,
                                wa_type: m.type || 'chat',
                                synced_from_startup: true,
                                // 标记是否为最近消息
                                is_recent: isRecent,
                                // ★★★ V5新增：保存引用消息数据 ★★★
                                quoted_message: m.quotedMsg ? {
                                    wa_message_id: m.quotedMsg.id,
                                    body: m.quotedMsg.body,
                                    fromMe: m.quotedMsg.fromMe,
                                    type: m.quotedMsg.type
                                } : null
                            }
                        };
                    });

                    // 按时间排序（升序）
                    preparedMessages.sort((a, b) =>
                        (a.content_attributes.wa_timestamp || 0) - (b.content_attributes.wa_timestamp || 0)
                    );

                    // 6. 批量创建消息
                    const batchResult = await cw.syncMessagesInBatches({
                        account_id: CHATWOOT_ACCOUNT_ID,
                        conversation_id,
                        messages: preparedMessages,
                        batchSize,
                        delayMs
                    });

                    results.created += batchResult.created || 0;
                    results.failed += batchResult.failed || 0;

                    contactResult.created = batchResult.created || 0;
                    contactResult.failed = batchResult.failed || 0;
                    contactResult.status = 'success';

                    logToCollector('[STARTUP_SYNC] Contact complete', {
                        chatId,
                        created: batchResult.created,
                        failed: batchResult.failed
                    });

                    // 7. 保存 Redis 映射
                    if (redis) {
                        const mappingData = {
                            sessionId,
                            phone: phone || '',
                            phone_lid: phone_lid || '',
                            chatId,
                            contact_id: contact.id,
                            conversation_id,
                            updatedAt: Date.now(),
                            syncedAt: Date.now()
                        };

                        await redis.set(
                            `cw:mapping:conv:${conversation_id}`,
                            JSON.stringify(mappingData),
                            'EX', 86400 * 30
                        ).catch(() => {});

                        await redis.set(
                            `cw:mapping:contact:${contact.id}`,
                            JSON.stringify(mappingData),
                            'EX', 86400 * 30
                        ).catch(() => {});
                    }

                } finally {
                    // ★★★ V5.3.2修复：标记同步完成并延迟释放锁（从20秒改为3秒）★★★
                    syncLockManager.markComplete(conversation_id, 3000);
                }

                results.processed++;
                results.contacts.push(contactResult);

            } catch (contactErr) {
                logToCollector('[STARTUP_SYNC] Contact error', {
                    chatId,
                    error: contactErr?.message
                });

                contactResult.status = 'error';
                contactResult.error = contactErr?.message;
                results.errors.push({ chatId, error: contactErr?.message });
                results.failed += messages.length;
                results.contacts.push(contactResult);
            }
        }

        const duration = Date.now() - startTime;

        logToCollector('[STARTUP_SYNC] Complete', {
            duration,
            processed: results.processed,
            skipped: results.skipped,
            created: results.created,
            replaced: results.replaced,
            failed: results.failed,
            errorCount: results.errors.length
        });

        res.json({
            ok: true,
            duration,
            ...results
        });

    } catch (e) {
        logToCollector('[STARTUP_SYNC] Fatal error', { error: e?.message });
        res.status(500).json({ ok: false, error: e?.message });
    }
});


/**
 * GET /startup-sync/status
 * 获取启动同步的当前状态
 */
app.get('/startup-sync/status', (req, res) => {
    const locks = syncLockManager.getAllLocks();
    const activeLocks = Object.entries(locks)
        .filter(([_, v]) => v && !v.released)
        .length;

    res.json({
        ok: true,
        activeLocks,
        locks
    });
});


/**
 * POST /startup-sync/release/:conversation_id
 * 强制释放同步锁（调试用）
 */
app.post('/startup-sync/release/:conversation_id', (req, res) => {
    const { conversation_id } = req.params;
    const before = syncLockManager.getStatus(conversation_id);

    syncLockManager.forceRelease(conversation_id);

    res.json({
        ok: true,
        conversation_id,
        before,
        after: syncLockManager.getStatus(conversation_id)
    });
});
























/**
 * 预处理消息用于批量创建
 * V5.3.13修复：不提前查找in_reply_to，保存_quotedMsgWaId供创建时实时查找
 * @param {Object} waMsg - WhatsApp 消息
 * @param {string} waBridgeUrl - 桥接器 URL
 * @param {string} waBridgeToken - 桥接器 Token
 * @param {number} conversation_id - Chatwoot会话ID（不再使用，保留参数兼容）
 * @returns {Object} 处理后的消息对象
 */
async function prepareMessageForBatch(waMsg, waBridgeUrl, waBridgeToken, conversation_id = null) {
    const { id, fromMe, type, body, timestamp, media, quotedMsg } = waMsg;

    // ★★★ V5.3.13修复：不提前查找in_reply_to，保存原始内容 ★★★
    let content = body || '';
    let quotedBody = '';
    let quotedTextFallback = null;  // 文本格式引用的fallback

    if (quotedMsg) {
        // 根据消息类型生成引用内容预览
        quotedBody = quotedMsg.body || quotedMsg.caption || '';
        if (!quotedBody && quotedMsg.type) {
            const typeLabels = {
                'image': '[图片]',
                'video': '[视频]',
                'audio': '[语音]',
                'ptt': '[语音消息]',
                'document': '[文件]',
                'sticker': '[表情贴纸]',
                'location': '[位置]',
                'contact': '[联系人]',
                'contact_card': '[名片]'
            };
            quotedBody = typeLabels[quotedMsg.type] || `[${quotedMsg.type || '媒体'}消息]`;
        }

        // ★★★ V5.3.13修复：生成文本格式引用作为fallback ★★★
        const quotedSender = quotedMsg.fromMe ? '我' : '对方';
        const quotedPreview = quotedBody.length > 50
            ? quotedBody.substring(0, 50) + '...'
            : quotedBody;
        const quotedText = quotedPreview.replace(/\n/g, ' ').substring(0, 40);
        quotedTextFallback = `▎💬 ${quotedSender}：${quotedText}\n\n${body || ''}`;
    }

    const prepared = {
        content: content,  // ★★★ 保存原始内容，不加文本格式引用 ★★★
        message_type: fromMe ? 1 : 0,  // 1=outgoing, 0=incoming
        timestamp: timestamp,
        source_id: id,
        attachments: [],
        // ★★★ 关键修复：为 outgoing 消息设置 status: 'sent' ★★★
        // 同步的历史消息已发送成功，不应该显示为 pending
        ...(fromMe ? { status: 'sent' } : {}),
        // ★★★ V5新增：保存引用消息数据 ★★★
        content_attributes: {
            wa_type: type,
            wa_timestamp: timestamp,
            quoted_message: quotedMsg ? {
                wa_message_id: quotedMsg.id,
                body: quotedBody,
                fromMe: quotedMsg.fromMe,
                type: quotedMsg.type
            } : null
        },
        // ★★★ V5.3.13修复：保存被引用消息的WA ID，供创建时实时查找 ★★★
        _quotedMsgWaId: quotedMsg?.id || null,
        _quotedTextFallback: quotedTextFallback,  // 文本格式引用fallback
        _in_reply_to: null  // 不再提前查找，由fallbackBatchCreate实时查找
    };

    // 处理媒体附件
    if (media && !media.error) {
        if (media.data_url) {
            // 已经是 base64
            prepared.attachments.push({
                data_url: media.data_url,
                file_type: media.mimetype,
                filename: media.filename || `media_${Date.now()}`
            });
        } else if (media.fileUrl && waBridgeUrl) {
            // 需要下载
            try {
                let mediaUrl = media.fileUrl;
                if (!mediaUrl.startsWith('http')) {
                    mediaUrl = `${waBridgeUrl}${mediaUrl}`;
                }

                const headers = {};
                if (waBridgeToken) headers['x-api-token'] = waBridgeToken;

                const resp = await axios.get(mediaUrl, {
                    responseType: 'arraybuffer',
                    headers,
                    timeout: 30000
                });

                const buffer = Buffer.from(resp.data);
                const mime = media.mimetype || resp.headers['content-type'] || 'application/octet-stream';
                const b64 = `data:${mime};base64,${buffer.toString('base64')}`;

                prepared.attachments.push({
                    data_url: b64,
                    file_type: mime,
                    filename: media.filename || `media_${Date.now()}`
                });
            } catch (e) {
                console.warn(`[prepareMessageForBatch] ${id}: Download failed: ${e?.message}`);
            }
        }
    }

    return prepared;
}

/**
 * 创建单条消息（批量创建降级使用）
 */
async function createSingleMessage(account_id, conversation_id, msg) {
    if (msg.message_type === 1) {
        // outgoing
        return await cw.createOutgoingMessage({
            account_id,
            conversation_id,
            content: msg.content || '',
            attachments: msg.attachments || [],
            source_id: msg.source_id
        });
    } else {
        // incoming
        return await cw.createIncomingMessage({
            account_id,
            conversation_id,
            content: msg.content || '',
            attachments: msg.attachments || [],
            source_id: msg.source_id
        });
    }
}


/**
 * 同步单条消息到 Chatwoot
 */
async function syncOneMessage({ account_id, conversation_id, message, waBridgeUrl, waBridgeToken }) {
    const { id, fromMe, type, body, timestamp, media } = message;

    let attachments = [];

    if (media && !media.error) {
        // 优先使用 data_url (base64)
        if (media.data_url) {
            console.log(`[syncOneMessage] ${id}: Using data_url (${media.mimetype})`);
            attachments.push({
                data_url: media.data_url,
                file_type: media.mimetype,
                filename: media.filename || `media_${Date.now()}`
            });
        }
        // 备用：从纳管器下载
        else if (media.fileUrl && waBridgeUrl) {
            try {
                let mediaUrl = media.fileUrl;
                if (!mediaUrl.startsWith('http')) {
                    mediaUrl = `${waBridgeUrl}${mediaUrl}`;
                }
                console.log(`[syncOneMessage] ${id}: Downloading from ${mediaUrl}`);

                const headers = {};
                if (waBridgeToken) headers['x-api-token'] = waBridgeToken;

                const resp = await axios.get(mediaUrl, {
                    responseType: 'arraybuffer',
                    headers,
                    timeout: 30000
                });

                const buffer = Buffer.from(resp.data);
                const mime = media.mimetype || resp.headers['content-type'] || 'application/octet-stream';
                const b64 = `data:${mime};base64,${buffer.toString('base64')}`;

                attachments.push({
                    data_url: b64,
                    file_type: mime,
                    filename: media.filename || `media_${Date.now()}`
                });

                console.log(`[syncOneMessage] ${id}: Downloaded OK, size=${buffer.length}`);
            } catch (e) {
                console.error(`[syncOneMessage] ${id}: Download failed:`, e?.message);
            }
        }
    }

    console.log(`[syncOneMessage] ${id}: fromMe=${fromMe}, type=${type}, body=${(body || '').substring(0, 50)}, attachments=${attachments.length}`);

    try {
        let result;
        if (fromMe) {
            // 我方发送的消息 -> outgoing
            result = await cw.createOutgoingMessage({
                account_id,
                conversation_id,
                content: body || '',
                attachments,
                source_id: id
            });
        } else {
            // 对方发送的消息 -> incoming
            result = await cw.createIncomingMessage({
                account_id,
                conversation_id,
                content: body || '',
                attachments,
                source_id: id
            });
        }
        // ★★★ V5.3新增：返回消息ID供调用者存储映射 ★★★
        return { ok: true, cwMessageId: result?.id, waMessageId: id };
    } catch (e) {
        console.error(`[syncOneMessage] ${id} failed:`, e?.message);
        throw e;
    }
}


/**
 * GET /sync-status/:conversation_id
 * 获取会话的同步状态
 */
app.get('/sync-status/:conversation_id', async (req, res) => {
    try {
        const { conversation_id } = req.params;

        const conv = await cw.getConversationDetails(CHATWOOT_ACCOUNT_ID, conversation_id);
        if (!conv) {
            return res.status(404).json({ ok: false, error: 'Conversation not found' });
        }

        const sender = conv.meta?.sender || {};

        // 获取映射信息
        let waMapping = null;
        if (redis) {
            const keys = [
                `cw:mapping:conv:${conversation_id}`,
                `cw:mapping:contact:${sender.id}`
            ];

            for (const key of keys) {
                const data = await redis.get(key);
                if (data) {
                    try {
                        waMapping = JSON.parse(data);
                        break;
                    } catch (_) {}
                }
            }
        }

        // 尝试从联系人信息推断
        if (!waMapping) {
            const phone = sender.phone_number?.replace(/[^\d]/g, '') ||
                sender.identifier?.replace(/[^\d]/g, '');
            if (phone) {
                waMapping = {
                    sessionId: WA_DEFAULT_SESSION.split(',')[0]?.trim(),
                    phone: phone,
                    chatId: `${phone}@c.us`,
                    inferred: true
                };
            }
        }

        res.json({
            ok: true,
            conversation_id: parseInt(conversation_id),
            contact: {
                id: sender.id,
                name: sender.name,
                phone: sender.phone_number,
                identifier: sender.identifier
            },
            waMapping: waMapping ? {
                sessionId: waMapping.sessionId,
                chatId: waMapping.chatId,
                phone: waMapping.phone,
                phone_lid: waMapping.phone_lid,
                inferred: waMapping.inferred || false
            } : null,
            canSync: !!(waMapping?.sessionId && (waMapping.chatId || waMapping.phone || waMapping.phone_lid))
        });
    } catch (e) {
        res.status(500).json({ ok: false, error: e?.message });
    }
});

// ★★★ V5 新增: WhatsApp 消息撤回 API ★★★
// 供前端直接调用，执行 WhatsApp 消息撤回
app.post('/api/whatsapp/revoke', async (req, res) => {
    try {
        const { conversation_id, message_id, wa_message_id, everyone = true } = req.body;

        if (!wa_message_id) {
            logToCollector('[WA_REVOKE] Missing wa_message_id', { conversation_id, message_id });
            return res.status(400).json({ ok: false, error: 'Missing wa_message_id' });
        }

        logToCollector('[WA_REVOKE] Request', {
            conversation_id,
            message_id,
            wa_message_id: wa_message_id.substring(0, 40),
            everyone
        });

        // 确定使用哪个 session
        let sessionId = WA_DEFAULT_SESSION.split(',')[0]?.trim();

        // 从 Redis 获取 conversation 的 session
        if (conversation_id && redis) {
            try {
                // 尝试多个 key
                const keys = [
                    `conv:${conversation_id}`,
                    `cw:mapping:conv:${conversation_id}`
                ];

                for (const key of keys) {
                    const mapping = await redis.get(key);
                    if (mapping) {
                        const parsed = JSON.parse(mapping);
                        if (parsed.sessionId) {
                            sessionId = parsed.sessionId;
                            logToCollector('[WA_REVOKE] Session from Redis', { key, sessionId });
                            break;
                        }
                    }
                }
            } catch (e) {
                console.warn('[WA_REVOKE] Redis lookup failed:', e.message);
            }
        }

        if (!sessionId) {
            logToCollector('[WA_REVOKE] No session, using default', { default: WA_DEFAULT_SESSION });
            sessionId = WA_DEFAULT_SESSION.split(',')[0]?.trim() || 'default';
        }

        // 调用 Bridge 删除 API
        const bridgeUrl = `${WA_BRIDGE_URL}/delete-message/${sessionId}`;

        logToCollector('[WA_REVOKE] Calling Bridge', {
            url: bridgeUrl,
            messageId: wa_message_id.substring(0, 40),
            everyone
        });

        const headers = {};
        if (WA_BRIDGE_TOKEN) headers['x-api-token'] = WA_BRIDGE_TOKEN;

        const result = await postWithRetry(
            bridgeUrl,
            {
                messageId: wa_message_id,
                everyone: everyone
            },
            headers,
            2,  // 重试次数
            30000  // 超时
        );

        logToCollector('[WA_REVOKE] Bridge Response', {
            ok: result?.ok,
            error: result?.error,
            wa_message_id: wa_message_id.substring(0, 40)
        });

        if (result?.ok) {
            res.json({ ok: true, revoked: true, wa_message_id });
        } else {
            res.json({
                ok: false,
                error: result?.error || 'Bridge returned error',
                wa_message_id
            });
        }

    } catch (error) {
        const errorMsg = error?.response?.data?.error || error?.message || 'unknown error';
        logToCollector('[WA_REVOKE] Error', { error: errorMsg });
        res.status(500).json({ ok: false, error: errorMsg });
    }
});

// ★★★ V5.3.13新增: 供Chatwoot同步删除调用的API ★★★
// Chatwoot删除消息时会先调用这个API，成功后才执行本地删除
app.post('/delete-wa-message', async (req, res) => {
    try {
        const { conversation_id, cw_message_id, source_id, everyone = true } = req.body;

        logToCollector('[DELETE_WA_MSG] Request', {
            conversation_id,
            cw_message_id,
            source_id: source_id?.substring(0, 40) || 'none',
            everyone
        });

        // 1. 查找WhatsApp消息ID
        let waMessageId = source_id;

        // 如果没有source_id，从Redis映射查找
        if (!waMessageId && cw_message_id && conversation_id && redis) {
            try {
                const reverseKey = `cw:msgmap:rev:${conversation_id}:${cw_message_id}`;
                waMessageId = await redis.get(reverseKey);
                if (waMessageId) {
                    logToCollector('[DELETE_WA_MSG] Found WA ID from mapping', {
                        cw_message_id,
                        wa_message_id: waMessageId.substring(0, 40)
                    });
                }
            } catch (e) {
                logToCollector('[DELETE_WA_MSG] Redis lookup failed', { error: e.message });
            }
        }

        // 如果找不到WhatsApp消息ID，无法删除
        if (!waMessageId) {
            logToCollector('[DELETE_WA_MSG] No WA message ID found', {
                cw_message_id,
                source_id: source_id || 'none'
            });
            return res.json({
                ok: false,
                error: 'no_wa_message_id',
                message: 'Cannot find WhatsApp message ID'
            });
        }

        // 2. 确定session
        let sessionId = WA_DEFAULT_SESSION.split(',')[0]?.trim();

        if (conversation_id && redis) {
            const keys = [`conv:${conversation_id}`, `cw:mapping:conv:${conversation_id}`];
            for (const key of keys) {
                try {
                    const mapping = await redis.get(key);
                    if (mapping) {
                        const parsed = JSON.parse(mapping);
                        if (parsed.sessionId) {
                            sessionId = parsed.sessionId;
                            break;
                        }
                    }
                } catch (e) {}
            }
        }

        logToCollector('[DELETE_WA_MSG] Calling Bridge', {
            session: sessionId,
            wa_message_id: waMessageId.substring(0, 40),
            everyone
        });

        // 3. 调用Bridge删除
        const headers = {};
        if (WA_BRIDGE_TOKEN) headers['x-api-token'] = WA_BRIDGE_TOKEN;

        const result = await postWithRetry(
            `${WA_BRIDGE_URL}/delete-message/${sessionId}`,
            { messageId: waMessageId, everyone },
            headers,
            2,
            30000
        ).catch(e => ({ ok: false, error: e.message }));

        logToCollector('[DELETE_WA_MSG] Bridge result', {
            ok: result?.ok,
            error: result?.error || 'none',
            wa_message_id: waMessageId.substring(0, 40)
        });

        if (result?.ok) {
            res.json({ ok: true, deleted: true, wa_message_id: waMessageId });
        } else {
            res.json({
                ok: false,
                error: result?.error || 'WhatsApp deletion failed',
                wa_message_id: waMessageId
            });
        }

    } catch (error) {
        const errorMsg = error?.message || 'unknown error';
        logToCollector('[DELETE_WA_MSG] Error', { error: errorMsg });
        res.status(500).json({ ok: false, error: errorMsg });
    }
});

// ★★★ V5 新增: 获取消息的 WhatsApp ID ★★★
// 供前端在没有 source_id 时查询
app.get('/api/whatsapp/message-id/:conversationId/:messageId', async (req, res) => {
    try {
        const { conversationId, messageId } = req.params;

        logToCollector('[WA_MSG_ID] Request', { conversationId, messageId });

        // 从 Chatwoot API 获取消息详情
        const messages = await cw.getMessages(CHATWOOT_ACCOUNT_ID, conversationId);
        const message = messages?.find(m => m.id === Number(messageId));

        if (message && message.source_id) {
            res.json({
                ok: true,
                wa_message_id: message.source_id,
                message_type: message.message_type
            });
        } else {
            res.json({
                ok: false,
                error: 'Message not found or no source_id'
            });
        }

    } catch (error) {
        const errorMsg = error?.response?.data?.error || error?.message || 'unknown error';
        logToCollector('[WA_MSG_ID] Error', { error: errorMsg });
        res.status(500).json({ ok: false, error: errorMsg });
    }
});

app.post('/chatwoot/webhook', async (req, res) => {
    // 统一入站打点：不再丢失任何一次调用
    logToCollector('[CW_WEBHOOK] ARRIVE', {
        ip: req.ip,
        method: req.method,
        ua: req.headers['user-agent'] || '',
        ct: req.headers['content-type'] || '',
        hasToken: Boolean(req.query?.token),
    });

    // 小工具：带日志的早退
    const SKIP = (reason, extra = {}) => {
        logToCollector('[CW_WEBHOOK] SKIP', {reason, ...extra});
        return res.json({ok: true, skipped: reason});
    };

    try {
        const body = req.body || {};

        // Chatwoot 会有两种结构：
        // A) { event: 'message_created', data: { message: {...}, conversation: {...} } }
        // B) { event: 'message_created', id, message_type, content, private, attachments, conversation: {...}, ... }  // 扁平
        const event =
            body.event ||
            body.type ||
            body?.data?.event ||
            body?.payload_type; // 多做一层兼容

        // 先把 payload 摘出来，后面好统一用
        const conversation = body.data?.conversation || body.conversation || null;

        // 组装 message 兼容体
        const message =
            body.data?.message ||
            (event === 'message_created' && (body.id || body.message_type) ? {
                id: body.id,
                // ★★★ V5修复：Chatwoot message_type 枚举：0=incoming, 1=outgoing ★★★
                message_type: (body.message_type === 'outgoing' ? 1 :
                    body.message_type === 'incoming' ? 0 :
                        body.message_type),
                content: body.content,
                private: body.private,
                attachments: body.attachments || [],
                conversation_id: (conversation && conversation.id) || body.conversation_id,
                // ★★★ 修复：添加 source_id 和 content_attributes 用于去重 ★★★
                source_id: body.source_id,
                content_attributes: body.content_attributes || {}
            } : null);

        // —— 把最关键的“事件+是否有message/conv”日志提前 —— //
        logToCollector('[CW_WEBHOOK] HEAD', {
            event,
            hasMessage: Boolean(message),
            hasConversation: Boolean(conversation),
        });

        // 非 message_created 的一律早退（但我们已记录 HEAD 了）
        if (!['message_created', 'message_updated'].includes(event)) {
            return SKIP('unhandled event', {event});
        }

        // ★★★ V5.3.13重写：处理 message_updated 事件（删除操作）★★★
        if (event === 'message_updated') {
            // Chatwoot删除消息时，content_attributes会包含deleted: true
            const contentAttributes = body.content_attributes || body.data?.message?.content_attributes || {};
            const isDeleted = contentAttributes.deleted === true;

            // 获取消息信息
            const cwMessageId = body.id || body.data?.message?.id;
            const sourceId = body.source_id || body.data?.message?.source_id;
            const messageType = body.message_type || body.data?.message?.message_type;
            const convId = body.conversation?.id || body.data?.conversation?.id || body.conversation_id;
            const isOutgoing = messageType === 1 || messageType === 'outgoing';

            logToCollector('[CW_WEBHOOK] message_updated', {
                cw_message_id: cwMessageId,
                source_id: sourceId?.substring(0, 40) || 'none',
                isDeleted,
                isOutgoing,
                conversation_id: convId,
                content_attributes: JSON.stringify(contentAttributes).substring(0, 100)
            });

            // 只处理删除操作 + 出站消息
            if (!isDeleted) {
                return SKIP('message_updated but not deleted', { isDeleted, hasDeleteAttr: !!contentAttributes.deleted });
            }

            if (!isOutgoing) {
                return SKIP('message_updated delete but not outgoing', { messageType });
            }

            // ★★★ 核心：查找WhatsApp消息ID ★★★
            let waMessageId = sourceId; // 优先用source_id

            // 如果没有source_id，从Redis映射中查找
            if (!waMessageId && cwMessageId && convId && redis) {
                try {
                    const reverseKey = `cw:msgmap:rev:${convId}:${cwMessageId}`;
                    waMessageId = await redis.get(reverseKey);
                    if (waMessageId) {
                        logToCollector('[CW_WEBHOOK] Found WA message ID from mapping', {
                            cw_message_id: cwMessageId,
                            wa_message_id: waMessageId.substring(0, 40)
                        });
                    }
                } catch (e) {
                    logToCollector('[CW_WEBHOOK] Redis lookup failed', { error: e.message });
                }
            }

            // 如果找不到WhatsApp消息ID，无法删除
            if (!waMessageId) {
                logToCollector('[CW_WEBHOOK] Cannot delete: no WA message ID found', {
                    cw_message_id: cwMessageId,
                    source_id: sourceId || 'none',
                    conversation_id: convId
                });
                return res.json({
                    ok: false,
                    error: 'no_wa_message_id',
                    message: 'Cannot find WhatsApp message ID for deletion'
                });
            }

            // 查找session
            let sessionId = WA_DEFAULT_SESSION.split(',')[0]?.trim();
            if (convId && redis) {
                const keys = [`conv:${convId}`, `cw:mapping:conv:${convId}`];
                for (const key of keys) {
                    try {
                        const mapping = await redis.get(key);
                        if (mapping) {
                            const parsed = JSON.parse(mapping);
                            if (parsed.sessionId) {
                                sessionId = parsed.sessionId;
                                break;
                            }
                        }
                    } catch (e) {}
                }
            }

            logToCollector('[CW_WEBHOOK] Attempting WA message delete', {
                wa_message_id: waMessageId.substring(0, 40),
                session: sessionId,
                conversation_id: convId
            });

            // 调用Bridge删除API
            try {
                const headers = {};
                if (WA_BRIDGE_TOKEN) headers['x-api-token'] = WA_BRIDGE_TOKEN;

                const result = await postWithRetry(
                    `${WA_BRIDGE_URL}/delete-message/${sessionId}`,
                    { messageId: waMessageId, everyone: true },
                    headers, 2, 30000
                ).catch(e => ({ ok: false, error: e.message }));

                logToCollector('[CW_WEBHOOK] WA delete result', {
                    ok: result?.ok,
                    wa_message_id: waMessageId.substring(0, 40),
                    session: sessionId,
                    error: result?.error || 'none'
                });

                if (result?.ok) {
                    return res.json({ ok: true, action: 'deleted', wa_message_id: waMessageId });
                } else {
                    return res.json({
                        ok: false,
                        action: 'delete_failed',
                        error: result?.error,
                        wa_message_id: waMessageId
                    });
                }
            } catch (e) {
                logToCollector('[CW_WEBHOOK] WA delete error', { error: e.message });
                return res.json({ ok: false, action: 'delete_error', error: e.message });
            }
        }

        if (!message || !conversation) return SKIP('no message/conversation', {
            event, keys: Object.keys(body || {}),
        });

        // message_type: 0=incoming, 1=outgoing（也可能是字符串）
        const mt = message.message_type;
        const isOutgoing = (mt === 1) || (String(mt).toLowerCase() === 'outgoing');

        if (!isOutgoing) return SKIP('not outgoing', {mt});
        if (message.private) return SKIP('is private');

        // ★★★ 关键修复：检查消息是否来自 WhatsApp 同步 ★★★
        // 通过 /ingest-outgoing 创建的消息会有以下标记：
        // 1. source_id (WhatsApp 消息ID)
        // 2. content_attributes.synced_from_device = true
        // 3. content_attributes.wa_message_id
        const contentAttrs = message.content_attributes || {};
        const isSyncedFromDevice = message.source_id
            || contentAttrs.synced_from_device
            || contentAttrs.wa_message_id;

        if (isSyncedFromDevice) {
            return SKIP('synced from WhatsApp device', {
                source_id: message.source_id?.substring(0, 25),
                synced_from_device: contentAttrs.synced_from_device
            });
        }

        const account_id = conversation.account_id || CHATWOOT_ACCOUNT_ID;
        const conversation_id = conversation.id || message.conversation_id;
        const message_id = message.id;  // 【Bug 1 修复】保存消息 ID 用于失败标记
// —— 去重：同一 message 在 created/updated/重试场景只发一次 —— //
        const msgId = message.id || message.source_id || message.message_id;
        const stamp = message.updated_at || message.created_at || Date.now();
        if (msgId) {
            const deKey = `cw:dedupe:${msgId}:${stamp}`;
            const seen = await redis.get(deKey);
            if (seen) return SKIP('dup event', {msgId, stamp});
            await redis.set(deKey, '1');
            await redis.expire(deKey, 300); // 5 分钟窗口
        }

        // 【v3】使用 shouldBlockSend - 同步进行中时完全阻止所有发送
        const blockCheck = syncLockManager.shouldBlockSend(conversation_id);
        if (blockCheck.block) {
            logToCollector('[CW_WEBHOOK] Blocked by sync lock', {
                conversation_id,
                reason: blockCheck.reason,
                elapsed: blockCheck.elapsed,
                lockId: blockCheck.lockId
            });
            return SKIP(`sync_lock: ${blockCheck.reason}`);
        }

        if (!WA_DEFAULT_SESSION || !WA_BRIDGE_URL) {
            console.warn('[WEBHOOK] skip: WA_DEFAULT_SESSION/WA_BRIDGE_URL 未配置');
            return SKIP('no WA config');
        }


        // 记录入参（去敏）
        logToCollector('[CW_WEBHOOK] PAYLOAD', {
            event,
            hasMessage: Boolean(message),
            hasConversation: Boolean(conversation)
        });
        // 查客户号码
        const conv = await cw.getConversationDetails(account_id, conversation_id);
        const meta = (conv && conv.meta) || {};
        const sender = meta.sender || {};

        let phone =
            sender.phone_number ||
            (sender.additional_attributes && (sender.additional_attributes.phone_e164 || sender.additional_attributes.phone_raw || sender.additional_attributes.phone)) ||
            sender.identifier;

        if (!phone) return res.json({ok: true, skipped: 'no phone'});
        const chatIdOrPhone = String(phone).replace(/^wa:[^:]*:/, '').replace(/^wa:/, '');

        const headers = {};
        if (WA_BRIDGE_TOKEN) headers['x-api-token'] = WA_BRIDGE_TOKEN;

        const text = message.content || '';
        // 尽量多兜几个位置，防止不同版本 payload 有差异
        const attachmentsRaw =
            message.attachments ||
            body.attachments ||
            (body.data && body.data.attachments) ||
            [];
        const attachments = Array.isArray(attachmentsRaw) ? attachmentsRaw : [];

        // === 发送媒体（支持多图/多附件）===
        if (attachments.length > 0) {
            const mediaList = [];

            for (const a of attachments) {
                if (!a) continue;

                // 从 file_type 判断媒体类型
                let mediaType = 'file';
                const ft = String(a.file_type || '').toLowerCase();
                if (ft.startsWith('image')) mediaType = 'image';
                else if (ft.startsWith('video')) mediaType = 'video';
                else if (ft.startsWith('audio')) mediaType = 'audio';

                let b64, url;
                let mimetype = a.file_type || 'application/octet-stream';

                const dataUrl = a.data_url || a.dataUrl;
                if (dataUrl) {
                    if (/^data:/i.test(dataUrl)) {
                        const m = dataUrl.match(/^data:(.*?);base64,(.*)$/i);
                        if (m) {
                            mimetype = m[1] || mimetype;
                            b64 = m[2];
                        }
                    } else if (/^https?:\/\//i.test(dataUrl)) {
                        url = rewriteCwDataUrl(dataUrl);
                    }
                }

                if (!b64 && !url && a.file_url && /^https?:\/\//i.test(a.file_url)) {
                    url = rewriteCwDataUrl(a.file_url);
                }

                if (!b64 && !url) continue;

                // ===== 关键修复：确保 MIME 类型是完整格式 =====
                // Chatwoot 可能返回 'image' 而不是 'image/png'
                if (mimetype && !mimetype.includes('/')) {
                    // 从文件名推断
                    const fn = (a.file_name || a.filename || '').toLowerCase();
                    if (mimetype === 'image') {
                        if (fn.endsWith('.png')) mimetype = 'image/png';
                        else if (fn.endsWith('.gif')) mimetype = 'image/gif';
                        else if (fn.endsWith('.webp')) mimetype = 'image/webp';
                        else mimetype = 'image/jpeg';  // 默认
                    } else if (mimetype === 'video') {
                        if (fn.endsWith('.webm')) mimetype = 'video/webm';
                        else if (fn.endsWith('.mov')) mimetype = 'video/quicktime';
                        else if (fn.endsWith('.avi')) mimetype = 'video/x-msvideo';
                        else mimetype = 'video/mp4';  // 默认
                    } else if (mimetype === 'audio') {
                        if (fn.endsWith('.mp3')) mimetype = 'audio/mpeg';
                        else if (fn.endsWith('.wav')) mimetype = 'audio/wav';
                        else if (fn.endsWith('.ogg') || fn.endsWith('.opus')) mimetype = 'audio/ogg';
                        else mimetype = 'audio/mpeg';  // 默认
                    } else {
                        mimetype = 'application/octet-stream';
                    }
                }

                mediaList.push({
                    type: mediaType,
                    url,
                    b64,
                    mimetype,
                    filename: a.file_name || a.filename
                });
            }

            if (mediaList.length > 0) {
                const {sessionId: finalSession, to, to_lid} = await resolveWaTarget({
                    redis,
                    conversation_id: conversation.id || conversation.conversation_id,
                    sender,
                    fallback: chatIdOrPhone,
                    WA_DEFAULT_SESSION
                });

                if (!to && !to_lid) {
                    logToCollector('[CW->WA] SKIP_MEDIA', {reason: 'no recipient'});
                    return res.json({ok: true, skipped: 'no recipient'});
                }

                logToCollector('[CW->WA] SEND_MEDIA', {
                    session: finalSession,
                    to: to || 'none',
                    to_lid: to_lid || 'none',
                    count: mediaList.length,
                    message_id
                });

                const payload = {
                    sessionId: finalSession,
                    to: to || '',
                    to_lid: to_lid || '',
                    caption: message.content || '',
                    message_id: message_id  // 用于纳管器去重
                };

                if (mediaList.length === 1) {
                    const m = mediaList[0];
                    payload.type = m.type;
                    payload.mimetype = m.mimetype;
                    if (m.b64) payload.b64 = m.b64;
                    else payload.url = m.url;
                    if (m.filename) payload.filename = m.filename;
                } else {
                    payload.attachments = mediaList;
                }

                // ★★★ 异步发送媒体：先返回成功，后台发送 ★★★
                // 这样即使发送40张图片需要1分钟，Chatwoot 也不会超时
                logToCollector('[CW->WA] MEDIA_ASYNC_START', {
                    message_id,
                    count: mediaList.length,
                    session: finalSession
                });

                // 立即返回成功给 Chatwoot
                res.json({
                    ok: true,
                    sent: true,
                    async: true,
                    mediaCount: mediaList.length,
                    message_id
                });

                // 后台异步发送（不阻塞响应）
                setImmediate(async () => {
                    const startTime = Date.now();
                    try {
                        const result = await postWithRetry(`${WA_BRIDGE_URL}/send/media`, payload, headers, 3, 180000); // 3分钟超时

                        const success = result?.ok || result?.success;
                        const failedItems = (result?.results || []).filter(r => !r.ok);
                        const duration = Date.now() - startTime;

                        if (success && failedItems.length === 0) {
                            logToCollector('[CW->WA] MEDIA_ASYNC_OK', {
                                message_id,
                                total: result?.total || mediaList.length,
                                duration: `${duration}ms`
                            });
                            console.log(`[MEDIA_ASYNC] ✓ 发送成功: message_id=${message_id}, count=${mediaList.length}, duration=${duration}ms`);

                            // ★★★ V5新增：回写 source_id 到 Chatwoot ★★★
                            // 使用第一条消息的 msgId 作为 source_id
                            const firstResult = (result?.results || [])[0];
                            const firstMsgId = firstResult?.msgId || result?.msgId;
                            if (firstMsgId && message_id && conversation_id) {
                                try {
                                    await cw.updateMessageSourceId({
                                        account_id: CHATWOOT_ACCOUNT_ID,
                                        conversation_id: conversation_id,
                                        message_id: message_id,
                                        source_id: firstMsgId
                                    });
                                    console.log(`[MEDIA_ASYNC] ✓ source_id 已回写: message_id=${message_id}`);
                                } catch (updateErr) {
                                    console.error(`[MEDIA_ASYNC] ✗ source_id 回写失败: ${updateErr.message}`);
                                }
                            }
                        } else {
                            const errorMsg = failedItems.map(f => f.error).join('; ') || 'partial failure';
                            logToCollector('[CW->WA] MEDIA_ASYNC_PARTIAL', {
                                message_id,
                                total: result?.total,
                                failed: failedItems.length,
                                error: errorMsg,
                                duration: `${duration}ms`
                            });
                            console.error(`[MEDIA_ASYNC] ⚠ 部分失败: message_id=${message_id}, failed=${failedItems.length}/${mediaList.length}, error=${errorMsg}`);
                        }
                    } catch (err) {
                        const duration = Date.now() - startTime;
                        const errorMsg = err?.response?.data?.error || err?.message || 'unknown error';
                        logToCollector('[CW->WA] MEDIA_ASYNC_ERROR', {
                            message_id,
                            error: errorMsg,
                            duration: `${duration}ms`
                        });
                        console.error(`[MEDIA_ASYNC] ✗ 发送失败: message_id=${message_id}, error=${errorMsg}, duration=${duration}ms`);
                    }
                });

                return; // 已经返回响应，直接退出
            }
        }

        // === 发送纯文本 ===
        if (text && text.trim()) {
            const { sessionId: finalSession, to, to_lid } = await resolveWaTarget({
                redis,
                conversation_id: conversation.id || conversation.conversation_id,
                sender,
                fallback: chatIdOrPhone,
                WA_DEFAULT_SESSION
            });

            if (!to && !to_lid) {
                logToCollector('[CW->WA] SKIP_TEXT', { reason: 'no recipient' });
                return res.json({ ok: true, skipped: 'no recipient' });
            }

            // ★★★ V5.3修复：增强引用消息ID查找 ★★★
            // 1. 优先使用 Chatwoot 提供的 external_id
            // 2. 如果没有，尝试从 Redis 反向查询
            let quotedMessageId = contentAttrs.in_reply_to_external_id ||  // Chatwoot 后端转换后的字段
                contentAttrs.quoted_wa_message_id ||      // 前端发送的原始字段
                contentAttrs.quotedMessageId ||
                null;

            // ★★★ V5.3新增：如果没有WhatsApp消息ID但有Chatwoot消息ID，从Redis查找 ★★★
            const inReplyTo = contentAttrs.in_reply_to;
            if (!quotedMessageId && inReplyTo && conversation_id) {
                console.log(`[CW->WA] Quote lookup: in_reply_to=${inReplyTo}, trying Redis...`);
                quotedMessageId = await getWaMessageIdByCwId(conversation_id, inReplyTo);
                if (quotedMessageId) {
                    console.log(`[CW->WA] Quote lookup success: cw=${inReplyTo} -> wa=${quotedMessageId.substring(0, 35)}`);
                } else {
                    console.log(`[CW->WA] Quote lookup failed: no mapping for cw=${inReplyTo}`);
                }
            }

            // ★★★ V5.3.9新增：提取消息后缀用于精确查找 ★★★
            let quotedMessageSuffix = null;
            if (quotedMessageId) {
                quotedMessageSuffix = extractMessageIdSuffix(quotedMessageId);
                console.log(`[CW->WA] Quote suffix: ${quotedMessageSuffix}`);
            }

            // ★★★ V5.3.13说明：关于跨设备查找 ★★★
            // 之前尝试在接收方设备上查找消息ID，这是错误的！
            // sendMessage在哪个设备执行，就需要那个设备上的消息ID。
            // 所以我们只需要传递suffix给send/reply，让它在发送方设备上查找。

            logToCollector('[CW->WA] SEND_TEXT', {
                session: finalSession,
                to: to || 'none',
                to_lid: to_lid || 'none',
                len: text.length,
                message_id,
                hasQuote: !!quotedMessageId,
                quoteSuffix: quotedMessageSuffix || 'none'
            });

            // ★★★ 修复：改为异步模式，先返回成功给 Chatwoot ★★★
            // 这样即使 Bridge 响应较慢，Chatwoot 也不会显示"发送失败"
            res.json({
                ok: true,
                sent: true,
                async: true,
                message_id
            });

            // 后台异步发送（不阻塞响应）
            setImmediate(async () => {
                const startTime = Date.now();
                try {
                    let result;

                    // ★★★ V5 新增：如果有引用，使用 /send/reply 端点 ★★★
                    if (quotedMessageId) {
                        // ★★★ V5修复：构建正确格式的 chatId ★★★
                        // Bridge 需要完整格式: phone@c.us 或 lid@lid
                        let chatId = '';
                        if (to && to !== 'none') {
                            chatId = to.includes('@') ? to : `${to}@c.us`;
                        } else if (to_lid && to_lid !== 'none') {
                            chatId = to_lid.includes('@') ? to_lid : `${to_lid}@lid`;
                        }

                        // ★★★ V5.3.13修复：直接传递原始ID和suffix ★★★
                        // send/reply会在发送方设备上用suffix查找正确的消息ID
                        result = await postWithRetry(
                            `${WA_BRIDGE_URL}/send/reply/${finalSession}`,
                            {
                                chatIdOrPhone: chatId,
                                text,
                                quotedMessageId,  // 原始ID（作为备用）
                                quotedMessageSuffix  // suffix用于精确查找
                            },
                            headers,
                            3,
                            60000
                        );

                        const success = result?.ok;
                        const duration = Date.now() - startTime;

                        logToCollector('[CW->WA] REPLY_RESULT', {
                            success,
                            msgId: result?.msgId,
                            quotedMessageId: quotedMessageId?.substring(0, 30),
                            quoteSuffix: quotedMessageSuffix,
                            message_id,
                            duration: `${duration}ms`
                        });

                        if (success) {
                            console.log(`[REPLY_ASYNC] ✓ 引用发送成功: message_id=${message_id}, msgId=${result?.msgId}`);

                            // ★★★ V5.3修复：改用Redis存储消息ID映射（替代失败的Chatwoot API） ★★★
                            if (result?.msgId && message_id && conversation_id) {
                                await saveMessageMapping(conversation_id, result.msgId, message_id);
                                console.log(`[REPLY_ASYNC] ✓ 消息映射已保存: wa=${result.msgId.substring(0, 35)} -> cw=${message_id}`);
                            }
                        } else {
                            console.error(`[REPLY_ASYNC] ✗ 引用发送失败: message_id=${message_id}, error=${result?.error}`);
                        }
                    } else {
                        // 普通发送
                        result = await postWithRetry(
                            `${WA_BRIDGE_URL}/send/text`,
                            { sessionId: finalSession, to: to || '', to_lid: to_lid || '', text },
                            headers,
                            3,  // 3次重试
                            60000  // 60秒超时
                        );

                        const success = result?.ok;
                        const duration = Date.now() - startTime;

                        logToCollector('[CW->WA] TEXT_RESULT', {
                            success,
                            msgId: result?.msgId,
                            message_id,
                            duration: `${duration}ms`
                        });

                        if (!success) {
                            console.error(`[TEXT_ASYNC] ✗ 发送失败: message_id=${message_id}, error=${result?.error}`);
                        } else {
                            console.log(`[TEXT_ASYNC] ✓ 发送成功: message_id=${message_id}, msgId=${result?.msgId}, duration=${duration}ms`);

                            // ★★★ V5.3修复：改用Redis存储消息ID映射（替代失败的Chatwoot API） ★★★
                            // Chatwoot不支持update_source_id API，所以改用Redis做映射
                            if (result?.msgId && message_id && conversation_id) {
                                await saveMessageMapping(conversation_id, result.msgId, message_id);
                                console.log(`[TEXT_ASYNC] ✓ 消息映射已保存: wa=${result.msgId.substring(0, 35)} -> cw=${message_id}`);
                            }
                        }
                    }
                } catch (err) {
                    const duration = Date.now() - startTime;
                    const errorMsg = err?.response?.data?.error || err?.message || 'unknown error';
                    console.error(`[TEXT_ASYNC] ✗ 发送异常: message_id=${message_id}, error=${errorMsg}, duration=${duration}ms`);
                    logToCollector('[CW->WA] TEXT_ERROR', {
                        error: errorMsg,
                        message_id,
                        duration: `${duration}ms`
                    });
                }
            });

            return; // 已经返回响应，直接退出
        }

        return res.json({ ok: true, skipped: 'no content' });
    } catch (err) {
        console.error('[WEBHOOK_ERROR]', err.response?.data || err.message);
        return res.status(500).json({ok: false, error: err.message, bridge: err.response?.data});
    }
});

// ===== 【调试端点】同步锁状态查询和强制释放 =====
app.get('/sync-lock/:conversation_id', (req, res) => {
    const { conversation_id } = req.params;
    const status = syncLockManager.getStatus(Number(conversation_id));
    res.json({ ok: true, conversation_id: Number(conversation_id), lock: status });
});

// 查看所有活跃的锁
app.get('/sync-locks', (req, res) => {
    const allLocks = syncLockManager.getAllLocks();
    res.json({ ok: true, locks: allLocks, count: Object.keys(allLocks).length });
});

app.delete('/sync-lock/:conversation_id', (req, res) => {
    const { conversation_id } = req.params;
    syncLockManager.forceRelease(Number(conversation_id));
    res.json({ ok: true, released: true });
});


// 功能：接收 manager 发来的历史消息数据，批量同步到 Chatwoot
app.post('/batch-sync-history', async (req, res) => {
    const startTime = Date.now();
    logToCollector('[BATCH_SYNC] Start', { timestamp: startTime });

    try {
        const { sessions } = req.body;

        if (!sessions || !Array.isArray(sessions)) {
            return res.status(400).json({ ok: false, error: 'Missing sessions array' });
        }

        const results = {
            totalSessions: sessions.length,
            totalContacts: 0,
            totalMessages: 0,
            created: 0,
            skipped: 0,
            failed: 0,
            errors: []
        };

        // ★★★ 移除：不再在这里预先解析 inbox_id ★★★
        // const inbox_id = await resolveInboxId();

        for (const sessionData of sessions) {
            const { sessionId, sessionName, contacts } = sessionData;

            if (!contacts || contacts.length === 0) continue;

            // ★★★ 新增：每个 session 解析自己的 inbox ★★★
            const inbox_id = await resolveInboxId(sessionId);

            logToCollector('[BATCH_SYNC] Processing session', {
                sessionId,
                sessionName,
                contactCount: contacts.length,
                inbox_id
            });

            for (const contact of contacts) {
                try {
                    const { chatId, phone, phone_lid, name, messages } = contact;

                    if (!messages || messages.length === 0) continue;

                    results.totalContacts++;
                    results.totalMessages += messages.length;

                    // 确保联系人存在
                    const contactResult = await cw.ensureContact({
                        account_id: CHATWOOT_ACCOUNT_ID,
                        rawPhone: phone,
                        rawPhone_lid: phone_lid,
                        rawName: name,
                        sessionId
                    });

                    if (!contactResult?.id) {
                        results.failed += messages.length;
                        results.errors.push({ chatId, error: 'Failed to create contact' });
                        continue;
                    }

                    // 确保会话存在
                    const convResult = await cw.ensureConversation({
                        account_id: CHATWOOT_ACCOUNT_ID,
                        inbox_id,
                        contact_id: contactResult.id,
                        custom_attributes: {
                            wa_chat_id: chatId,
                            session_id: sessionId
                        }
                    });

                    if (!convResult?.id) {
                        results.failed += messages.length;
                        results.errors.push({ chatId, error: 'Failed to create conversation' });
                        continue;
                    }

                    // 保存 Redis 映射
                    if (redis) {
                        const mappingData = {
                            sessionId,
                            phone: phone || '',
                            phone_lid: phone_lid || '',
                            chatId,
                            contact_id: contactResult.id,
                            conversation_id: convResult.id,
                            updatedAt: Date.now()
                        };

                        await redis.set(
                            `cw:mapping:conv:${convResult.id}`,
                            JSON.stringify(mappingData),
                            'EX', 86400 * 30
                        ).catch(() => {});

                        await redis.set(
                            `cw:mapping:contact:${contactResult.id}`,
                            JSON.stringify(mappingData),
                            'EX', 86400 * 30
                        ).catch(() => {});
                    }

                    // 转换消息格式
                    const cwMessages = messages.map(m => {
                        // ★★★ V5.3.1修复：引用消息格式化（支持媒体消息） ★★★
                        let content = m.body || '';
                        let quotedBodyText = '';

                        if (m.quotedMsg) {
                            quotedBodyText = m.quotedMsg.body || m.quotedMsg.caption || '';
                            if (!quotedBodyText && m.quotedMsg.type) {
                                const typeLabels = {
                                    'image': '[图片]',
                                    'video': '[视频]',
                                    'audio': '[语音]',
                                    'ptt': '[语音消息]',
                                    'document': '[文件]',
                                    'sticker': '[表情贴纸]',
                                    'location': '[位置]',
                                    'contact': '[联系人]',
                                    'contact_card': '[名片]'
                                };
                                quotedBodyText = typeLabels[m.quotedMsg.type] || `[${m.quotedMsg.type || '媒体'}消息]`;
                            }

                            const quotedSender = m.quotedMsg.fromMe ? '我' : '对方';
                            const quotedPreview = quotedBodyText.length > 50
                                ? quotedBodyText.substring(0, 50) + '...'
                                : quotedBodyText;
                            // ★★★ V5.3.2优化：简洁单行引用格式 ★★★
                            const quotedText = quotedPreview.replace(/\n/g, ' ').substring(0, 40);
                            content = `▎💬 ${quotedSender}：${quotedText}\n\n${m.body || ''}`;
                        }

                        return {
                            content: content,
                            message_type: m.fromMe ? 1 : 0, // 1=outgoing, 0=incoming
                            source_id: m.id,
                            private: false,
                            content_attributes: {
                                wa_timestamp: m.timestamp,
                                wa_type: m.type,
                                synced_from_history: true,
                                // ★★★ V5新增：保存引用消息数据 ★★★
                                quoted_message: m.quotedMsg ? {
                                    wa_message_id: m.quotedMsg.id,
                                    body: quotedBodyText,
                                    fromMe: m.quotedMsg.fromMe,
                                    type: m.quotedMsg.type
                                } : null
                            }
                        };
                    });

                    // 批量创建消息
                    const batchResult = await cw.syncMessagesInBatches({
                        account_id: CHATWOOT_ACCOUNT_ID,
                        conversation_id: convResult.id,
                        messages: cwMessages,
                        batchSize: 50,
                        delayMs: 100
                    });

                    results.created += batchResult.created || 0;
                    results.skipped += batchResult.skipped || 0;
                    results.failed += batchResult.failed || 0;

                    if (batchResult.errors && batchResult.errors.length > 0) {
                        results.errors.push(...batchResult.errors.slice(0, 5)); // 只保留前5个错误
                    }

                } catch (e) {
                    logToCollector('[BATCH_SYNC] Contact error', {
                        chatId: contact.chatId,
                        error: e.message
                    });
                    results.failed += (contact.messages?.length || 0);
                    results.errors.push({ chatId: contact.chatId, error: e.message });
                }
            }
        }

        const duration = Date.now() - startTime;
        logToCollector('[BATCH_SYNC] Complete', {
            duration,
            ...results,
            errorCount: results.errors.length
        });

        res.json({
            ok: true,
            ...results,
            duration,
            errors: results.errors.slice(0, 20) // 只返回前20个错误
        });

    } catch (e) {
        logToCollector('[BATCH_SYNC] Fatal error', { error: e.message });
        res.status(500).json({ ok: false, error: e.message });
    }
});


// 功能：比较 WA 最新消息与 Chatwoot 中的消息，找出差异
app.post('/compare-messages', async (req, res) => {
    try {
        const { sessionId, contacts } = req.body;

        if (!contacts || !Array.isArray(contacts)) {
            return res.status(400).json({ ok: false, error: 'Missing contacts array' });
        }

        const results = [];

        for (const contact of contacts) {
            const { chatId, phone, phone_lid, lastMessage } = contact;

            try {
                // 查找 Chatwoot 中的联系人
                const identifier = `wa:${sessionId}:${phone || phone_lid}`;
                const cwContact = await cw.searchContact({
                    account_id: CHATWOOT_ACCOUNT_ID,
                    identifier
                });

                if (!cwContact) {
                    results.push({
                        chatId,
                        status: 'contact_not_found',
                        needsSync: true
                    });
                    continue;
                }

                // 获取会话
                // 注：需要根据你的实现获取 conversation_id
                // 这里假设你有一个方法可以通过 contact_id 获取最近的会话

                results.push({
                    chatId,
                    contactId: cwContact.id,
                    status: 'found',
                    waLastMessage: lastMessage,
                    needsSync: false // 需要进一步比较来确定
                });

            } catch (e) {
                results.push({
                    chatId,
                    status: 'error',
                    error: e.message,
                    needsSync: true
                });
            }
        }

        res.json({ ok: true, results });

    } catch (e) {
        res.status(500).json({ ok: false, error: e.message });
    }
});


// 功能：同步指定聊天中缺失的消息
app.post('/sync-missing-messages', async (req, res) => {
    try {
        const { sessionId, chatId, messages, conversation_id } = req.body;

        if (!messages || !Array.isArray(messages) || !conversation_id) {
            return res.status(400).json({
                ok: false,
                error: 'Missing messages array or conversation_id'
            });
        }

        logToCollector('[SYNC_MISSING] Start', {
            sessionId,
            chatId,
            messageCount: messages.length
        });

        // 转换消息格式
        const cwMessages = messages.map(m => {
            // ★★★ V5.3.1修复：引用消息格式化（支持媒体消息） ★★★
            let content = m.body || m.text || '';
            let quotedBodyText = '';

            if (m.quotedMsg) {
                quotedBodyText = m.quotedMsg.body || m.quotedMsg.caption || '';
                if (!quotedBodyText && m.quotedMsg.type) {
                    const typeLabels = {
                        'image': '[图片]',
                        'video': '[视频]',
                        'audio': '[语音]',
                        'ptt': '[语音消息]',
                        'document': '[文件]',
                        'sticker': '[表情贴纸]',
                        'location': '[位置]',
                        'contact': '[联系人]',
                        'contact_card': '[名片]'
                    };
                    quotedBodyText = typeLabels[m.quotedMsg.type] || `[${m.quotedMsg.type || '媒体'}消息]`;
                }

                const quotedSender = m.quotedMsg.fromMe ? '我' : '对方';
                const quotedPreview = quotedBodyText.length > 50
                    ? quotedBodyText.substring(0, 50) + '...'
                    : quotedBodyText;
                // ★★★ V5.3.2优化：简洁单行引用格式 ★★★
                const quotedText = quotedPreview.replace(/\n/g, ' ').substring(0, 40);
                content = `▎💬 ${quotedSender}：${quotedText}\n\n${m.body || m.text || ''}`;
            }

            return {
                content: content,
                message_type: m.fromMe ? 1 : 0,
                source_id: m.id || m.messageId,
                private: false,
                content_attributes: {
                    wa_timestamp: m.timestamp,
                    wa_type: m.type,
                    synced_from_other_device: true,
                    // ★★★ V5新增：保存引用消息数据 ★★★
                    quoted_message: m.quotedMsg ? {
                        wa_message_id: m.quotedMsg.id,
                        body: quotedBodyText,
                        fromMe: m.quotedMsg.fromMe,
                        type: m.quotedMsg.type
                    } : null
                }
            };
        });

        // 批量创建
        const result = await cw.syncMessagesInBatches({
            account_id: CHATWOOT_ACCOUNT_ID,
            conversation_id,
            messages: cwMessages,
            batchSize: 20,
            delayMs: 50
        });

        logToCollector('[SYNC_MISSING] Complete', {
            sessionId,
            chatId,
            created: result.created,
            skipped: result.skipped,
            failed: result.failed
        });

        res.json({ ok: true, ...result });

    } catch (e) {
        logToCollector('[SYNC_MISSING] Error', { error: e.message });
        res.status(500).json({ ok: false, error: e.message });
    }
});


// 功能：调试 Redis 映射数据
app.get('/debug/mapping/:identifier', async (req, res) => {
    try {
        const { identifier } = req.params;

        if (!redis) {
            return res.status(400).json({ ok: false, error: 'Redis not configured' });
        }

        const results = {};

        // 尝试不同的键格式
        const keys = [
            `cw:mapping:conv:${identifier}`,
            `cw:mapping:contact:${identifier}`,
            `cw:mapping:phone:${identifier}`,
            `cw:contact:wa:*:${identifier}`
        ];

        for (const key of keys) {
            if (key.includes('*')) {
                // 模式匹配
                const matchedKeys = await redis.keys(key);
                for (const mk of matchedKeys.slice(0, 5)) {
                    const val = await redis.get(mk);
                    results[mk] = val ? JSON.parse(val) : null;
                }
            } else {
                const val = await redis.get(key);
                results[key] = val ? JSON.parse(val) : null;
            }
        }

        res.json({ ok: true, identifier, mappings: results });

    } catch (e) {
        res.status(500).json({ ok: false, error: e.message });
    }
});


// ============================================================
// ★★★ V5.3.12 新增：未读消息同步相关 API ★★★
// ============================================================

/**
 * API: 获取会话最后同步的消息信息
 * 用于未读同步时定位增量起点
 */
app.get('/get-last-synced-message', async (req, res) => {
    try {
        const { sessionId, phone, phone_lid } = req.query;

        if (!sessionId || (!phone && !phone_lid)) {
            return res.status(400).json({ ok: false, error: 'Missing sessionId or phone/phone_lid' });
        }

        logToCollector('[GET_LAST_MSG] Request', { sessionId, phone, phone_lid });

        // 1. 查找联系人
        const digits = phone || phone_lid;
        const identifierType = phone ? 'phone' : 'lid';
        const identifier = `wa:${sessionId}:${identifierType}:${digits}`;

        // 先尝试精确查找
        let contact = null;
        try {
            const searchResp = await cw.request('GET',
                `/api/v1/accounts/${CHATWOOT_ACCOUNT_ID}/contacts/search?q=${encodeURIComponent(digits)}`
            );
            const contacts = searchResp?.payload || searchResp || [];

            // 查找匹配的联系人
            contact = contacts.find(c => c.identifier === identifier);
            if (!contact) {
                // 尝试旧格式 identifier
                const oldIdentifier = `wa:${sessionId}:${digits}`;
                contact = contacts.find(c => c.identifier === oldIdentifier);
            }
        } catch (e) {
            console.log(`[GET_LAST_MSG] Contact search failed: ${e.message}`);
        }

        if (!contact) {
            logToCollector('[GET_LAST_MSG] Contact not found', { identifier });
            return res.json({ ok: true, lastMessage: null, reason: 'contact_not_found' });
        }

        // 2. 获取该联系人的会话
        let conversation = null;
        try {
            const convResp = await cw.request('GET',
                `/api/v1/accounts/${CHATWOOT_ACCOUNT_ID}/contacts/${contact.id}/conversations`
            );
            const conversations = convResp?.payload || convResp || [];

            // 找到最近的会话
            if (conversations.length > 0) {
                conversation = conversations.sort((a, b) =>
                    (b.last_activity_at || 0) - (a.last_activity_at || 0)
                )[0];
            }
        } catch (e) {
            console.log(`[GET_LAST_MSG] Conversation fetch failed: ${e.message}`);
        }

        if (!conversation) {
            logToCollector('[GET_LAST_MSG] Conversation not found', { contact_id: contact.id });
            return res.json({ ok: true, lastMessage: null, reason: 'conversation_not_found' });
        }

        // 3. 获取会话的最后一条消息
        try {
            const msgResp = await cw.request('GET',
                `/api/v1/accounts/${CHATWOOT_ACCOUNT_ID}/conversations/${conversation.id}/messages`
            );
            const messages = msgResp?.payload || msgResp || [];

            if (messages.length === 0) {
                return res.json({ ok: true, lastMessage: null, reason: 'no_messages' });
            }

            // 找最新的消息（按 created_at 降序）
            const sortedMsgs = messages.sort((a, b) => (b.created_at || 0) - (a.created_at || 0));
            const lastMsg = sortedMsgs[0];

            // 提取 WA 消息 ID
            const wa_message_id = lastMsg.source_id ||
                lastMsg.content_attributes?.wa_message_id ||
                null;

            const result = {
                conversation_id: conversation.id,
                cw_message_id: lastMsg.id,
                wa_message_id,
                timestamp: lastMsg.created_at,
                content: (lastMsg.content || '').substring(0, 100),
                message_type: lastMsg.message_type
            };

            logToCollector('[GET_LAST_MSG] Found', {
                conversation_id: conversation.id,
                cw_message_id: lastMsg.id,
                wa_message_id: wa_message_id?.substring(0, 30)
            });

            return res.json({ ok: true, lastMessage: result });

        } catch (e) {
            console.log(`[GET_LAST_MSG] Messages fetch failed: ${e.message}`);
            return res.json({ ok: true, lastMessage: null, reason: 'messages_fetch_failed' });
        }

    } catch (e) {
        console.error('[GET_LAST_MSG] Error:', e.message);
        res.status(500).json({ ok: false, error: e.message });
    }
});

/**
 * API: 增量同步消息
 * 接收 WA 消息列表，增量创建到 Chatwoot
 * ★★★ 关键：每条消息创建后都必须保存映射 ★★★
 */
app.post('/sync-incremental', async (req, res) => {
    const startTime = Date.now();

    try {
        const {
            sessionId,
            sessionName,
            chatId,
            phone,
            phone_lid,
            contactName,
            messages
        } = req.body || {};

        if (!sessionId || !messages || !Array.isArray(messages)) {
            return res.status(400).json({ ok: false, error: 'Missing required fields' });
        }

        logToCollector('[SYNC_INCR] Start', {
            sessionId,
            sessionName,
            chatId,
            phone: phone || phone_lid,
            messageCount: messages.length
        });

        // 1. 确保联系人存在
        const targetPhone = phone || phone_lid;
        let contact = await cw.ensureContact({
            account_id: CHATWOOT_ACCOUNT_ID,
            rawPhone: phone,
            rawPhone_lid: phone_lid,
            rawName: contactName || targetPhone,
            sessionId,
            sessionName
        });

        if (!contact?.id) {
            return res.status(400).json({ ok: false, error: 'Failed to ensure contact' });
        }

        // 2. 确保会话存在
        const inbox_id = await resolveInboxId(sessionId);
        const conv = await cw.ensureConversation({
            account_id: CHATWOOT_ACCOUNT_ID,
            inbox_id,
            contact_id: contact.id,
            custom_attributes: {
                wa_chat_id: chatId,
                session_id: sessionId
            }
        });

        const conversation_id = conv?.id || conv;
        if (!conversation_id) {
            return res.status(400).json({ ok: false, error: 'Failed to ensure conversation' });
        }

        // 3. 逐条同步消息
        let synced = 0;
        let skipped = 0;
        let failed = 0;
        const errors = [];

        // 按时间戳排序（从旧到新）
        const sortedMessages = [...messages].sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));

        for (const msg of sortedMessages) {
            const msgId = msg.id || '';

            // Redis 去重检查
            if (redis && msgId) {
                const syncKey = msg.fromMe ? `wa:synced:outgoing:${msgId}` : `wa:synced:incoming:${msgId}`;
                const exists = await redis.get(syncKey).catch(() => null);
                if (exists) {
                    skipped++;
                    continue;
                }
            }

            try {
                // 构建消息内容
                let content = msg.body || '';

                // 处理引用消息
                if (msg.quotedMsg && msg.quotedMsg.id) {
                    // 尝试查找被引用消息的 Chatwoot ID
                    const quotedCwId = await findCwMessageId(conversation_id, msg.quotedMsg.id);

                    if (!quotedCwId) {
                        // 没有原生引用，用文本模拟
                        const quotedFrom = msg.quotedMsg.fromMe ? '我' : '对方';
                        const quotedText = (msg.quotedMsg.body || '').replace(/\n/g, ' ').substring(0, 40);
                        content = `▎💬 ${quotedFrom}：${quotedText}\n\n${msg.body || ''}`;
                    }
                }

                // 创建消息
                const messageType = msg.fromMe ? 'outgoing' : 'incoming';
                const content_attrs = {
                    wa_message_id: msgId,
                    wa_timestamp: msg.timestamp,
                    wa_type: msg.type,
                    synced_from_unread_scan: true
                };

                const created = await cw.request('POST',
                    `/api/v1/accounts/${CHATWOOT_ACCOUNT_ID}/conversations/${conversation_id}/messages`,
                    {
                        content,
                        message_type: messageType,
                        private: false,
                        source_id: msgId,
                        content_attributes: content_attrs
                    }
                );

                const createdId = created?.id || created?.message?.id;

                // ★★★ 关键：保存消息ID映射（钢印原则）★★★
                if (createdId && msgId) {
                    await saveMessageMapping(conversation_id, msgId, createdId);
                    console.log(`[SYNC_INCR] Mapping saved: wa=${msgId.substring(0, 25)} -> cw=${createdId}`);
                }

                // 标记已同步
                if (redis && msgId) {
                    const syncKey = msg.fromMe ? `wa:synced:outgoing:${msgId}` : `wa:synced:incoming:${msgId}`;
                    await redis.set(syncKey, '1', 'EX', 7 * 24 * 3600).catch(() => {});
                }

                synced++;

            } catch (e) {
                const errMsg = e?.message || '';

                // 重复消息不算失败
                if (/duplicate|source_id.*taken/i.test(errMsg)) {
                    skipped++;
                } else {
                    failed++;
                    errors.push({ id: msgId, error: errMsg });
                }
            }

            // 消息之间小延迟
            await new Promise(r => setTimeout(r, 50));
        }

        const duration = Date.now() - startTime;

        logToCollector('[SYNC_INCR] Complete', {
            conversation_id,
            synced,
            skipped,
            failed,
            duration
        });

        res.json({
            ok: true,
            conversation_id,
            synced,
            skipped,
            failed,
            errors: errors.slice(0, 10),  // 只返回前10个错误
            duration
        });

    } catch (e) {
        console.error('[SYNC_INCR] Error:', e.message);
        res.status(500).json({ ok: false, error: e.message });
    }
});


app.listen(PORT, '0.0.0.0', () => {
    console.log(`[collector] up on 0.0.0.0:${PORT}`);
});