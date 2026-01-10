/**
 * contact-sync.js - WhatsApp 联系人定时同步到 Chatwoot
 *
 * 功能：
 * - 每天北京时间 17:30 自动同步
 * - 获取最近 24 小时有聊天的联系人
 * - 解析电话号码并同步到 Chatwoot
 *
 * 环境变量配置：
 * - CONTACT_SYNC_ENABLED: 是否启用 (1/0)，默认 0
 * - CONTACT_SYNC_ON_STARTUP: 启动时是否立即执行一次，默认 0
 * - CONTACT_SYNC_HOUR: 每天执行的小时（北京时间），默认 17
 * - CONTACT_SYNC_MINUTE: 每天执行的分钟，默认 30
 */

const axios = require('axios');

// 配置
const SYNC_ENABLED = process.env.CONTACT_SYNC_ENABLED === '1';
const SYNC_ON_STARTUP = process.env.CONTACT_SYNC_ON_STARTUP === '1';
const SYNC_HOUR = parseInt(process.env.CONTACT_SYNC_HOUR || '17');    // 北京时间小时
const SYNC_MINUTE = parseInt(process.env.CONTACT_SYNC_MINUTE || '30'); // 分钟

// Chatwoot 配置
const CW_BASE = (process.env.CHATWOOT_BASE_URL || '').replace(/\/$/, '');
const CW_TOKEN = process.env.CHATWOOT_API_TOKEN;
const CW_ACCOUNT_ID = process.env.CHATWOOT_ACCOUNT_ID;

// Bridge 配置
const BRIDGE_BASE = process.env.WA_BRIDGE_URL || process.env.MANAGER_BASE || 'http://127.0.0.1:5010';

// 依赖注入
let sessionManager = null;
let scheduledTimer = null;

/**
 * ★ 格式化北京时间为可读字符串
 * @returns {string} 例如: "2025年01月06日 17时30分25秒"
 */
function formatBeijingTime(date = new Date()) {
    // 北京时间 = UTC + 8
    const beijingOffset = 8 * 60;  // 分钟
    const localOffset = date.getTimezoneOffset();  // 本地时区偏移（分钟，西为正）
    const totalOffset = beijingOffset + localOffset;  // 服务器到北京时间的偏移

    const beijingTime = new Date(date.getTime() + totalOffset * 60 * 1000);

    const year = beijingTime.getFullYear();
    const month = String(beijingTime.getMonth() + 1).padStart(2, '0');
    const day = String(beijingTime.getDate()).padStart(2, '0');
    const hours = String(beijingTime.getHours()).padStart(2, '0');
    const minutes = String(beijingTime.getMinutes()).padStart(2, '0');
    const seconds = String(beijingTime.getSeconds()).padStart(2, '0');

    return `${year}年${month}月${day}日 ${hours}时${minutes}分${seconds}秒`;
}

/**
 * ★ 格式化持续时间为可读字符串
 * @param {number} ms - 毫秒
 * @returns {string} 例如: "2分35秒" 或 "1小时5分钟"
 */
function formatDurationChinese(ms) {
    const totalSeconds = Math.floor(ms / 1000);
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;

    if (hours > 0) {
        return `${hours}小时${minutes}分钟${seconds}秒`;
    } else if (minutes > 0) {
        return `${minutes}分${seconds}秒`;
    } else {
        return `${seconds}秒`;
    }
}

/**
 * 计算到下次北京时间指定时刻的毫秒数
 */
function getMillisecondsUntilBeijingTime(hour, minute) {
    const now = new Date();

    // 创建北京时间的目标时间
    // 北京时间 = UTC + 8
    const beijingOffset = 8 * 60;  // 分钟
    const localOffset = now.getTimezoneOffset();  // 本地时区偏移（分钟，西为正）
    const totalOffset = beijingOffset + localOffset;  // 服务器到北京时间的偏移

    // 计算北京时间的当前时刻
    const beijingNow = new Date(now.getTime() + totalOffset * 60 * 1000);

    // 设置目标时间（今天的 hour:minute）
    const target = new Date(beijingNow);
    target.setHours(hour, minute, 0, 0);

    // 如果已经过了今天的目标时间，设为明天
    if (target <= beijingNow) {
        target.setDate(target.getDate() + 1);
    }

    // 转换回服务器时间计算差值
    const targetServerTime = new Date(target.getTime() - totalOffset * 60 * 1000);
    return targetServerTime.getTime() - now.getTime();
}

/**
 * 格式化毫秒为可读时间
 */
function formatDuration(ms) {
    const hours = Math.floor(ms / (1000 * 60 * 60));
    const minutes = Math.floor((ms % (1000 * 60 * 60)) / (1000 * 60));
    return `${hours}h ${minutes}m`;
}

/**
 * 调度下次同步
 */
function scheduleNextSync() {
    const msUntilNext = getMillisecondsUntilBeijingTime(SYNC_HOUR, SYNC_MINUTE);

    console.log(`[ContactSync] Next sync at ${SYNC_HOUR}:${SYNC_MINUTE.toString().padStart(2, '0')} Beijing time (in ${formatDuration(msUntilNext)})`);

    scheduledTimer = setTimeout(() => {
        console.log('[ContactSync] ========================================');
        console.log('[ContactSync] Running scheduled sync...');
        syncAllSessions().catch(e => {
            console.error('[ContactSync] Scheduled sync failed:', e.message);
        }).finally(() => {
            // 完成后调度下一次（24小时后）
            scheduleNextSync();
        });
    }, msUntilNext);
}

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

    console.log(`[ContactSync] Initialized: daily at ${SYNC_HOUR}:${SYNC_MINUTE.toString().padStart(2, '0')} Beijing time`);

    // 启动时执行一次
    if (SYNC_ON_STARTUP) {
        setTimeout(() => {
            console.log('[ContactSync] ========================================');
            console.log('[ContactSync] Running startup sync...');
            syncAllSessions().catch(e => {
                console.error('[ContactSync] Startup sync failed:', e.message);
            });
        }, 30000);
    }

    // 调度每日定时同步
    scheduleNextSync();
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
            // 猜测国际格式
            e164 = '+' + digits;
        }
        return { isPhone: true, digits, e164 };
    }

    return { isPhone: false, digits: '', e164: null };
}

/**
 * 解析聊天对象获取电话号码
 * 三层解析：chatId → name → contact
 */
async function resolveContactPhone(chat, sessionId) {
    const chatId = chat.id?._serialized || '';
    const isLid = chatId.includes('@lid');

    let phone = null;
    let lid = null;
    let name = chat.name || null;

    // 第一层：从 chatId 提取
    if (isLid) {
        lid = chatId.replace('@lid', '');
    } else {
        const match = chatId.match(/^(\d+)@/);
        if (match) {
            phone = match[1];
        }
    }

    // 第二层：从 name 提取电话（如果还没有电话）
    if (!phone && name) {
        const extracted = extractPhoneFromName(name);
        if (extracted.isPhone && extracted.digits) {
            phone = extracted.digits;
        }
    }

    // 第三层：尝试通过 contact 获取
    if (!phone && chat.contact) {
        try {
            const contact = chat.contact;

            // 尝试从 number 字段获取
            if (contact.number) {
                const digits = contact.number.replace(/\D/g, '');
                if (digits.length >= 7) {
                    phone = digits;
                }
            }

            // 尝试从 pushname 提取
            if (!phone && contact.pushname) {
                const extracted = extractPhoneFromName(contact.pushname);
                if (extracted.isPhone) {
                    phone = extracted.digits;
                }
            }

            // 使用 pushname 作为名称
            if (contact.pushname && contact.pushname !== name) {
                name = contact.pushname;
            }
        } catch (e) {
            // 忽略错误
        }
    }

    return { phone, lid, name, chatId };
}

/**
 * 同步单个联系人到 Chatwoot
 */
async function syncContactToChatwoot(contactInfo, sessionId, sessionName) {
    const { phone, lid, name, chatId } = contactInfo;

    const stats = {
        action: 'skip',
        contactId: null,
        changes: []
    };

    // 必须至少有 phone 或 lid
    if (!phone && !lid) {
        return stats;
    }

    try {
        // 先搜索是否已存在
        const existingByPhone = phone ? await searchChatwootContact(phone, sessionId) : null;
        const existingByLid = (!existingByPhone && lid) ? await searchChatwootContact(lid, sessionId) : null;

        // 情况1: 完全不存在 → 创建
        if (!existingByPhone && !existingByLid) {
            const created = await createChatwootContact({
                sessionId,
                sessionName,
                phone,
                lid,
                name
            });

            if (created) {
                stats.action = 'created';
                stats.contactId = created.id;
                console.log(`[ContactSync] Created: ${name || phone || lid}`);
            }

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

    // ★ 记录开始时间
    const startTime = new Date();
    const startTimeStr = formatBeijingTime(startTime);

    console.log(`[ContactSync] ----------------------------------------`);
    console.log(`[ContactSync] [${sessionName}] 开始同步`);
    console.log(`[ContactSync] [${sessionName}] 开始时间: ${startTimeStr}`);

    const stats = {
        sessionId,
        sessionName,
        startTime: startTimeStr,
        endTime: null,
        duration: null,
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
        console.log(`[ContactSync] [${sessionName}] 找到 ${stats.total} 个最近24小时的聊天`);

        if (recentPrivateChats.length === 0) {
            const endTime = new Date();
            const duration = endTime - startTime;
            stats.endTime = formatBeijingTime(endTime);
            stats.duration = formatDurationChinese(duration);
            console.log(`[ContactSync] [${sessionName}] 无需同步`);
            console.log(`[ContactSync] [${sessionName}] 结束时间: ${stats.endTime}`);
            console.log(`[ContactSync] [${sessionName}] 耗时: ${stats.duration}`);
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

        // ★ 记录结束时间
        const endTime = new Date();
        const duration = endTime - startTime;
        stats.endTime = formatBeijingTime(endTime);
        stats.duration = formatDurationChinese(duration);

        console.log(`[ContactSync] [${sessionName}] 同步完成`);
        console.log(`[ContactSync] [${sessionName}] 结果: 新建=${stats.created}, 更新=${stats.updated}, 无变化=${stats.unchanged}, 跳过=${stats.skipped}, 错误=${stats.errors}`);
        console.log(`[ContactSync] [${sessionName}] 结束时间: ${stats.endTime}`);
        console.log(`[ContactSync] [${sessionName}] 耗时: ${stats.duration}`);

        return stats;

    } catch (e) {
        const endTime = new Date();
        const duration = endTime - startTime;
        stats.endTime = formatBeijingTime(endTime);
        stats.duration = formatDurationChinese(duration);
        stats.error = e.message;

        console.error(`[ContactSync] [${sessionName}] 同步失败: ${e.message}`);
        console.log(`[ContactSync] [${sessionName}] 结束时间: ${stats.endTime}`);
        console.log(`[ContactSync] [${sessionName}] 耗时: ${stats.duration}`);

        return stats;
    }
}

/**
 * 同步所有 Session 的联系人
 */
async function syncAllSessions() {

    // ★ 记录总开始时间
    const totalStartTime = new Date();
    const totalStartTimeStr = formatBeijingTime(totalStartTime);

    console.log('[ContactSync] ========================================');
    console.log('[ContactSync] ★★★ 联系人同步任务开始 ★★★');
    console.log(`[ContactSync] 开始时间: ${totalStartTimeStr}`);
    console.log('[ContactSync] ========================================');

    const allSessions = sessionManager.getAllSessions();
    const sessionIds = Object.keys(allSessions);

    console.log(`[ContactSync] 共 ${sessionIds.length} 个 Session 需要同步`);

    const results = [];

    for (const sessionId of sessionIds) {
        const stats = await syncSessionContacts(sessionId);
        results.push(stats);
        await new Promise(r => setTimeout(r, 1000));
    }

    // ★ 记录总结束时间
    const totalEndTime = new Date();
    const totalDuration = totalEndTime - totalStartTime;
    const totalEndTimeStr = formatBeijingTime(totalEndTime);
    const totalDurationStr = formatDurationChinese(totalDuration);

    const totalCreated = results.reduce((sum, r) => sum + (r.created || 0), 0);
    const totalUpdated = results.reduce((sum, r) => sum + (r.updated || 0), 0);
    const totalUnchanged = results.reduce((sum, r) => sum + (r.unchanged || 0), 0);
    const totalSkipped = results.reduce((sum, r) => sum + (r.skipped || 0), 0);
    const totalErrors = results.reduce((sum, r) => sum + (r.errors || 0), 0);
    const totalContacts = results.reduce((sum, r) => sum + (r.total || 0), 0);

    console.log('[ContactSync] ========================================');
    console.log('[ContactSync] ★★★ 联系人同步任务完成 ★★★');
    console.log(`[ContactSync] 开始时间: ${totalStartTimeStr}`);
    console.log(`[ContactSync] 结束时间: ${totalEndTimeStr}`);
    console.log(`[ContactSync] 总耗时: ${totalDurationStr}`);
    console.log('[ContactSync] ----------------------------------------');
    console.log(`[ContactSync] Session 数量: ${sessionIds.length}`);
    console.log(`[ContactSync] 联系人总数: ${totalContacts}`);
    console.log(`[ContactSync] 新建: ${totalCreated}`);
    console.log(`[ContactSync] 更新: ${totalUpdated}`);
    console.log(`[ContactSync] 无变化: ${totalUnchanged}`);
    console.log(`[ContactSync] 跳过: ${totalSkipped}`);
    console.log(`[ContactSync] 错误: ${totalErrors}`);
    console.log('[ContactSync] ========================================');

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
    SYNC_HOUR,
    SYNC_MINUTE
};