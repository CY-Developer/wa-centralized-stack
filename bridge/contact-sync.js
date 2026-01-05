/**
 * contact-sync.js - WhatsApp 联系人定时同步到 Chatwoot
 *
 * 功能：
 * - 定时获取 WhatsApp 今天有聊天的联系人
 * - 解析电话号码（复用现有逻辑）
 * - 同步到 Chatwoot（新增/更新）
 *
 * 环境变量配置：
 * - CONTACT_SYNC_ENABLED: 是否启用 (1/0)，默认 0
 * - CONTACT_SYNC_INTERVAL_HOURS: 同步间隔小时数，默认 6
 * - CONTACT_SYNC_ON_STARTUP: 启动时是否立即执行一次，默认 0
 *
 * 同步范围：只同步今天有聊天的联系人（根据最后消息时间判断）
 */

const axios = require('axios');

// 配置
const SYNC_ENABLED = process.env.CONTACT_SYNC_ENABLED === '1';
const SYNC_INTERVAL_HOURS = parseInt(process.env.CONTACT_SYNC_INTERVAL_HOURS || '6');
const SYNC_ON_STARTUP = process.env.CONTACT_SYNC_ON_STARTUP === '1';

// 兼容旧配置，但不再使用
const SYNC_COUNT = parseInt(process.env.CONTACT_SYNC_COUNT || '100');

// Chatwoot 配置
const CW_BASE = (process.env.CHATWOOT_BASE_URL || '').replace(/\/$/, '');
const CW_TOKEN = process.env.CHATWOOT_API_TOKEN;
const CW_ACCOUNT_ID = process.env.CHATWOOT_ACCOUNT_ID;

// Bridge 配置
const BRIDGE_BASE = process.env.WA_BRIDGE_URL || process.env.MANAGER_BASE || 'http://127.0.0.1:5010';

// 依赖注入
let sessionManager = null;

/**
 * 初始化模块
 */
function initialize(sm) {
    sessionManager = sm;

    if (!SYNC_ENABLED) {
        console.log('[ContactSync] Disabled (CONTACT_SYNC_ENABLED != 1)');
        return;
    }

    if (!CW_BASE || !CW_TOKEN || !CW_ACCOUNT_ID) {
        console.warn('[ContactSync] Missing Chatwoot config, sync disabled');
        return;
    }

    console.log(`[ContactSync] Initialized: interval=${SYNC_INTERVAL_HOURS}h, filter=last 24 hours`);

    // 启动时执行一次
    if (SYNC_ON_STARTUP) {
        setTimeout(() => {
            console.log('[ContactSync] Running startup sync...');
            syncAllSessions().catch(e => {
                console.error('[ContactSync] Startup sync failed:', e.message);
            });
        }, 30000);  // 等待 30 秒让所有 session 就绪
    }

    // 定时执行
    const intervalMs = SYNC_INTERVAL_HOURS * 60 * 60 * 1000;
    setInterval(() => {
        console.log('[ContactSync] Running scheduled sync...');
        syncAllSessions().catch(e => {
            console.error('[ContactSync] Scheduled sync failed:', e.message);
        });
    }, intervalMs);

    console.log(`[ContactSync] Next sync in ${SYNC_INTERVAL_HOURS} hours`);
}

/**
 * Chatwoot API 请求
 */
async function cwRequest(method, path, data = null) {
    const url = `${CW_BASE}${path}`;
    const config = {
        method,
        url,
        headers: {
            'api_access_token': CW_TOKEN,
            'Content-Type': 'application/json'
        },
        timeout: 30000
    };

    if (data) {
        config.data = data;
    }

    const res = await axios(config);
    return res.data;
}

/**
 * 在 Chatwoot 中搜索联系人
 * @param {string} query - 搜索关键词（电话或LID）
 * @param {string} sessionId - 用于过滤 identifier
 * @returns {Object|null}
 */
async function searchChatwootContact(query, sessionId) {
    if (!query) return null;

    try {
        const searchDigits = query.replace(/\D/g, '');
        const searchTerm = searchDigits.length > 10 ? searchDigits.slice(-10) : searchDigits;
        const q = encodeURIComponent(searchTerm);

        const res = await cwRequest('get', `/api/v1/accounts/${CW_ACCOUNT_ID}/contacts/search?q=${q}`);
        const list = Array.isArray(res?.payload) ? res.payload : (res || []);

        if (list.length === 0) return null;

        // 在结果中查找匹配的联系人
        for (const c of list) {
            // 检查 identifier 是否属于当前 session
            if (c.identifier) {
                const parts = c.identifier.split(':');
                // identifier 格式: wa:sessionId:type:digits
                if (parts[1] === sessionId) {
                    return c;
                }
            }

            // 检查 phone_number
            if (c.phone_number) {
                const phoneDigits = c.phone_number.replace(/\D/g, '');
                if (phoneDigits.includes(searchTerm) || searchTerm.includes(phoneDigits.slice(-10))) {
                    return c;
                }
            }
        }

        return null;
    } catch (e) {
        return null;  // 搜索失败静默处理
    }
}

/**
 * 创建 Chatwoot 联系人
 */
async function createChatwootContact({ sessionId, sessionName, phone, lid, name }) {
    const digits = phone || lid;
    const identifierType = phone ? 'phone' : 'lid';
    const identifier = `wa:${sessionId}:${identifierType}:${digits}`;

    // 构建显示名称: SessionName>ContactName
    const displayName = `${sessionName || sessionId}>${name || phone || lid || 'WA User'}`;

    const data = {
        name: displayName,
        identifier,
        custom_attributes: {
            session_id: sessionId,
            wa_phone: phone || null,
            wa_lid: lid || null,
            synced_at: new Date().toISOString()
        }
    };

    // 只有真实电话号码才设置 phone_number
    if (phone) {
        data.phone_number = '+' + phone;
    }

    try {
        const result = await cwRequest('post', `/api/v1/accounts/${CW_ACCOUNT_ID}/contacts`, data);
        const contact = result?.payload?.contact || result;
        return contact;
    } catch (e) {
        const msg = e.response?.data?.error || e.message || '';
        if (msg.includes('identifier') && msg.includes('taken')) {
            return null;  // 已存在
        }
        throw e;
    }
}

/**
 * 更新 Chatwoot 联系人
 */
async function updateChatwootContact(contactId, patch) {
    try {
        const result = await cwRequest('patch', `/api/v1/accounts/${CW_ACCOUNT_ID}/contacts/${contactId}`, patch);
        return result?.payload?.contact || result;
    } catch (e) {
        console.warn(`[ContactSync] Update failed for ${contactId}:`, e.message);
        return null;
    }
}

/**
 * 从名称中提取电话号码
 * （复用 chatwoot.js 中的逻辑）
 */
function extractPhoneFromName(str) {
    if (!str || typeof str !== 'string') {
        return { isPhone: false, digits: '', e164: null };
    }

    const trimmed = str.trim();
    const phoneCharsOnly = /^[\d\s\-\+\(\)\.]+$/;
    const digits = trimmed.replace(/[^\d]/g, '');

    const isPhoneFormat = phoneCharsOnly.test(trimmed);
    const validLength = digits.length >= 7 && digits.length <= 15;
    const digitRatio = digits.length / trimmed.replace(/\s/g, '').length;

    if (isPhoneFormat && validLength && digitRatio >= 0.5) {
        let e164 = null;
        if (trimmed.startsWith('+')) {
            e164 = '+' + digits;
        } else if (digits.length >= 10) {
            e164 = '+' + digits;
        }
        return { isPhone: true, digits, e164 };
    }

    return { isPhone: false, digits: '', e164: null };
}

/**
 * 解析联系人的电话号码
 *
 * 电话号码解析逻辑流程：
 * ┌─────────────────────────────────────────────────────────┐
 * │ 层级1: 检查是否有 @c.us 格式的电话号码                    │
 * │        ↓ 有 → 直接使用                                   │
 * │        ↓ 无 → 继续层级2                                  │
 * ├─────────────────────────────────────────────────────────┤
 * │ 层级2: 调用 Bridge API getContactLidAndPhone             │
 * │        将 @lid 转换为 @c.us 电话号码                      │
 * │        ↓ 成功 → 使用解析出的电话                         │
 * │        ↓ 失败 → 继续层级3                                │
 * ├─────────────────────────────────────────────────────────┤
 * │ 层级3: 检查联系人名称是否看起来像电话号码                  │
 * │        例如: "+1 408 555 1234" → 提取 14085551234        │
 * │        ↓ 匹配 → 使用提取的电话                           │
 * │        ↓ 不匹配 → 使用 LID 作为标识符                    │
 * └─────────────────────────────────────────────────────────┘
 *
 * @param {Object} chat - WhatsApp 聊天对象
 * @param {string} sessionId - Session ID
 * @returns {Object} { phone, lid, name }
 */
async function resolveContactPhone(chat, sessionId) {
    const chatId = chat.id?._serialized || '';
    const contactName = chat.name || '';

    let phone = null;
    let lid = null;

    // 层级1: 检查是否有 @c.us 格式
    if (chatId.endsWith('@c.us')) {
        phone = chatId.replace('@c.us', '');
    }

    // 层级2: 调用 Bridge API 解析 @lid
    if (!phone && chatId.endsWith('@lid')) {
        lid = chatId.replace('@lid', '');

        try {
            const response = await axios.get(
                `${BRIDGE_BASE}/resolve-lid/${sessionId}/${lid}`,
                { timeout: 10000 }
            );

            if (response.data?.ok && response.data?.phone) {
                phone = response.data.phone;
            }
        } catch (e) {
            // 静默处理，继续下一层级
        }
    }

    // 层级3: 检查名称是否像电话号码
    if (!phone && contactName) {
        const phoneCheck = extractPhoneFromName(contactName);
        if (phoneCheck.isPhone && phoneCheck.digits) {
            phone = phoneCheck.digits;
        }
    }

    return { phone, lid, name: contactName };
}

/**
 * 同步单个联系人到 Chatwoot
 */
async function syncContactToChatwoot(contact, sessionId, sessionName) {
    const { phone, lid, name } = contact;

    if (!phone && !lid) {
        return { action: 'skip', reason: 'no_identifier' };
    }

    const stats = { action: 'none', phone, lid, name, changes: [] };

    try {
        let existingByPhone = null;
        let existingByLid = null;

        if (phone) {
            existingByPhone = await searchChatwootContact(phone, sessionId);
        }

        if (lid) {
            existingByLid = await searchChatwootContact(lid, sessionId);
        }

        // 情况1: 都没找到 → 创建新联系人
        if (!existingByPhone && !existingByLid) {
            const newContact = await createChatwootContact({
                sessionId,
                sessionName,
                phone,
                lid,
                name
            });

            stats.action = 'created';
            stats.contactId = newContact?.id;
            console.log(`[ContactSync] Created: ${name || phone || lid}`);
            return stats;
        }

        // 情况2: 找到了电话 → 检查是否需要更新
        if (existingByPhone) {
            const patch = {};

            if (phone) {
                const existingPhone = (existingByPhone.phone_number || '').replace(/\D/g, '');
                if (existingPhone !== phone) {
                    patch.phone_number = '+' + phone;
                    stats.changes.push(`phone`);
                }
            }

            if (name) {
                const currentParts = (existingByPhone.name || '').split('>');
                const currentContactName = currentParts.length > 1 ? currentParts.slice(1).join('>') : currentParts[0];

                if (currentContactName !== name) {
                    const newName = currentParts.length > 1
                        ? `${currentParts[0]}>${name}`
                        : `${sessionName || sessionId}>${name}`;
                    patch.name = newName;
                    stats.changes.push(`name`);
                }
            }

            if (Object.keys(patch).length > 0) {
                await updateChatwootContact(existingByPhone.id, patch);
                stats.action = 'updated';
                console.log(`[ContactSync] Updated: ${name || phone} (${stats.changes.join(', ')})`);
            } else {
                stats.action = 'unchanged';
            }

            stats.contactId = existingByPhone.id;
            return stats;
        }

        // 情况3: 找到了LID但没找到电话 → 检查是否需要更新
        if (existingByLid) {
            const patch = {};

            if (phone) {
                const existingPhone = (existingByLid.phone_number || '').replace(/\D/g, '');
                if (existingPhone !== phone) {
                    patch.phone_number = '+' + phone;
                    stats.changes.push(`phone`);
                }
            }

            if (name) {
                const currentParts = (existingByLid.name || '').split('>');
                const currentContactName = currentParts.length > 1 ? currentParts.slice(1).join('>') : currentParts[0];

                if (currentContactName !== name) {
                    const newName = currentParts.length > 1
                        ? `${currentParts[0]}>${name}`
                        : `${sessionName || sessionId}>${name}`;
                    patch.name = newName;
                    stats.changes.push(`name`);
                }
            }

            if (phone && existingByLid.identifier?.includes(':lid:')) {
                const newIdentifier = `wa:${sessionId}:phone:${phone}`;
                if (existingByLid.identifier !== newIdentifier) {
                    patch.identifier = newIdentifier;
                    stats.changes.push(`identifier`);
                }
            }

            if (Object.keys(patch).length > 0) {
                await updateChatwootContact(existingByLid.id, patch);
                stats.action = 'updated';
                console.log(`[ContactSync] Updated: ${name || phone || lid} (${stats.changes.join(', ')})`);
            } else {
                stats.action = 'unchanged';
            }

            stats.contactId = existingByLid.id;
            return stats;
        }

        return stats;

    } catch (e) {
        console.error(`[ContactSync] Error: ${phone || lid} - ${e.message}`);
        stats.action = 'error';
        stats.error = e.message;
        return stats;
    }
}

/**
 * 获取24小时前的时间戳（秒级）
 */
function get24HoursAgoTimestamp() {
    const now = Date.now();
    const twentyFourHoursAgo = now - (24 * 60 * 60 * 1000);
    return Math.floor(twentyFourHoursAgo / 1000);
}

/**
 * 判断聊天是否在最近24小时内
 */
function isRecentChat(chat) {
    const cutoffTimestamp = get24HoursAgoTimestamp();
    const lastMsgTimestamp = chat.lastMessage?.timestamp || 0;
    const chatTimestamp = chat.timestamp || 0;
    const effectiveTimestamp = Math.max(lastMsgTimestamp, chatTimestamp);
    return effectiveTimestamp >= cutoffTimestamp;
}

/**
 * 同步单个 Session 的联系人
 */
async function syncSessionContacts(sessionId) {
    if (!sessionManager) {
        console.warn('[ContactSync] SessionManager not available');
        return { error: 'SessionManager not available' };
    }

    const session = sessionManager.getSession(sessionId);
    if (!session || session.sessionStatus !== 'ready') {
        console.log(`[ContactSync] Session ${sessionId} not ready, skipping`);
        return { error: 'Session not ready' };
    }

    const sessionName = sessionManager.getSessionName(sessionId) || sessionId;
    console.log(`[ContactSync] Syncing ${sessionId} (${sessionName})...`);

    const startTime = Date.now();
    const stats = {
        sessionId,
        sessionName,
        total: 0,
        created: 0,
        updated: 0,
        unchanged: 0,
        skipped: 0,
        errors: 0
    };

    try {
        const chats = session.chats || [];

        // 筛选最近24小时有聊天的私聊
        const recentPrivateChats = chats.filter(c => {
            const chatId = c.id?._serialized || '';
            if (c.isGroup || chatId.includes('@g.us') || chatId.includes('status@broadcast')) {
                return false;
            }
            return isRecentChat(c);
        });

        stats.total = recentPrivateChats.length;

        if (recentPrivateChats.length === 0) {
            console.log(`[ContactSync] No chats in last 24h, nothing to sync`);
            return stats;
        }

        // 逐个处理
        for (let i = 0; i < recentPrivateChats.length; i++) {
            const chat = recentPrivateChats[i];

            try {
                // 解析电话号码（使用三层逻辑）
                const contactInfo = await resolveContactPhone(chat, sessionId);

                // 同步到 Chatwoot
                const result = await syncContactToChatwoot(contactInfo, sessionId, sessionName);

                switch (result.action) {
                    case 'created':
                        stats.created++;
                        break;
                    case 'updated':
                        stats.updated++;
                        break;
                    case 'unchanged':
                        stats.unchanged++;
                        break;
                    case 'skip':
                        stats.skipped++;
                        break;
                    case 'error':
                        stats.errors++;
                        break;
                }

                // 小延迟避免 API 限流
                if (i < recentPrivateChats.length - 1) {
                    await new Promise(r => setTimeout(r, 200));
                }

            } catch (e) {
                console.error(`[ContactSync] Error processing chat:`, e.message);
                stats.errors++;
            }
        }

        const elapsed = Date.now() - startTime;
        console.log(`[ContactSync] ${sessionName}: created=${stats.created}, updated=${stats.updated}, unchanged=${stats.unchanged}, ${elapsed}ms`);

        return stats;

    } catch (e) {
        console.error(`[ContactSync] Session ${sessionId} failed:`, e.message);
        stats.error = e.message;
        return stats;
    }
}

/**
 * 同步所有 Session 的联系人
 */
async function syncAllSessions() {
    if (!sessionManager) {
        console.warn('[ContactSync] SessionManager not available');
        return;
    }

    console.log('[ContactSync] Starting full sync...');
    const startTime = Date.now();

    const allSessions = sessionManager.getAllSessions();
    const sessionIds = Object.keys(allSessions);

    const results = [];

    for (const sessionId of sessionIds) {
        const stats = await syncSessionContacts(sessionId);
        results.push(stats);
        await new Promise(r => setTimeout(r, 1000));
    }

    const elapsed = Date.now() - startTime;
    const totalCreated = results.reduce((sum, r) => sum + (r.created || 0), 0);
    const totalUpdated = results.reduce((sum, r) => sum + (r.updated || 0), 0);

    console.log(`[ContactSync] Complete: ${sessionIds.length} sessions, created=${totalCreated}, updated=${totalUpdated}, ${elapsed}ms`);

    return results;
}

/**
 * 手动触发同步（可通过 API 调用）
 */
async function triggerSync(sessionId = null) {
    if (sessionId) {
        return syncSessionContacts(sessionId);
    }
    return syncAllSessions();
}

module.exports = {
    initialize,
    syncAllSessions,
    syncSessionContacts,
    triggerSync,
    SYNC_ENABLED,
    SYNC_INTERVAL_HOURS,
    SYNC_COUNT
};