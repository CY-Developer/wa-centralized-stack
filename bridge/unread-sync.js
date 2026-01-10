/**
 * unread-sync.js - 未读消息定时同步模块 V1.0
 *
 * 功能：
 * - 每30分钟扫描所有 session 的 WA 未读消息
 * - 识别"超过30分钟未读"的异常聊天
 * - 智能增量同步：从 Chatwoot 最后一条消息之后开始同步
 * - 同步成功后自动标记 WA 已读
 *
 * 核心逻辑：
 * 1. 扫描 WA 未读消息，筛选超过阈值的聊天
 * 2. 获取 Chatwoot 该联系人最后一条消息的 wa_message_id
 * 3. 在 WA 端定位该消息，获取其后的所有新消息
 * 4. 增量同步这些新消息到 Chatwoot
 * 5. 同步成功 → 标记 WA 已读；同步失败 → 保持未读
 *
 * 环境变量配置：
 * - UNREAD_SYNC_ENABLED: 是否启用 (1/0)，默认 1
 * - UNREAD_SYNC_INTERVAL_MIN: 扫描间隔（分钟），默认 30
 * - UNREAD_SYNC_THRESHOLD_MIN: 未读阈值（分钟），默认 30
 * - UNREAD_SYNC_MAX_MESSAGES: 单次同步最大消息数，默认 200
 * - UNREAD_SYNC_FALLBACK_HOURS: 无历史记录时同步的小时数，默认 12
 */

const axios = require('axios');

// ============================================================
// 配置
// ============================================================
const SYNC_ENABLED = process.env.UNREAD_SYNC_ENABLED !== '0';  // 默认启用
const SYNC_INTERVAL_MIN = parseInt(process.env.UNREAD_SYNC_INTERVAL_MIN || '30');
const SYNC_THRESHOLD_MIN = parseInt(process.env.UNREAD_SYNC_THRESHOLD_MIN || '0');
const SYNC_MAX_MESSAGES = parseInt(process.env.UNREAD_SYNC_MAX_MESSAGES || '200');
const SYNC_FALLBACK_HOURS = parseInt(process.env.UNREAD_SYNC_FALLBACK_HOURS || '12');

// Collector 配置
const COLLECTOR_BASE = process.env.COLLECTOR_BASE || `http://127.0.0.1:${process.env.COLLECTOR_PORT || 7001}`;
const COLLECTOR_TOKEN = process.env.COLLECTOR_INGEST_TOKEN || process.env.API_TOKEN || '';

// ============================================================
// 依赖注入
// ============================================================
let sessionManager = null;
let sessions = null;  // whatsapp-manager 的 sessions 对象
let scheduledTimer = null;

// ============================================================
// 工具函数
// ============================================================

/**
 * 格式化北京时间
 */
function formatBeijingTime(date = new Date()) {
    const beijingOffset = 8 * 60;
    const localOffset = date.getTimezoneOffset();
    const totalOffset = beijingOffset + localOffset;
    const beijingTime = new Date(date.getTime() + totalOffset * 60 * 1000);

    const year = beijingTime.getFullYear();
    const month = String(beijingTime.getMonth() + 1).padStart(2, '0');
    const day = String(beijingTime.getDate()).padStart(2, '0');
    const hours = String(beijingTime.getHours()).padStart(2, '0');
    const minutes = String(beijingTime.getMinutes()).padStart(2, '0');
    const seconds = String(beijingTime.getSeconds()).padStart(2, '0');

    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

/**
 * 格式化持续时间
 */
function formatDuration(ms) {
    const totalSeconds = Math.floor(ms / 1000);
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;

    if (hours > 0) return `${hours}h${minutes}m${seconds}s`;
    if (minutes > 0) return `${minutes}m${seconds}s`;
    return `${seconds}s`;
}

/**
 * 生成请求头
 */
function getHeaders() {
    return COLLECTOR_TOKEN ? { 'x-api-token': COLLECTOR_TOKEN } : {};
}

// ============================================================
// 核心功能函数
// ============================================================

/**
 * 获取单个 session 的未读聊天列表
 * @param {string} sessionId
 * @returns {Array} 超过阈值的未读聊天列表
 */
async function getUnreadChats(sessionId) {
    const session = sessionManager?.getSession(sessionId);
    if (!session || session.sessionStatus !== 'ready' || !session.client) {
        return [];
    }

    const sessionName = sessionManager.getSessionName(sessionId) || sessionId;
    const thresholdMs = SYNC_THRESHOLD_MIN * 60 * 1000;
    const now = Date.now();

    try {
        const chats = await session.client.getChats();
        const unreadChats = [];

        for (const chat of chats) {
            // 跳过群聊和广播
            const chatId = chat.id?._serialized || '';
            if (chat.isGroup || /@g\.us$/i.test(chatId) || /broadcast/i.test(chatId)) {
                continue;
            }

            // 检查未读数
            const unreadCount = chat.unreadCount || 0;
            if (unreadCount <= 0) continue;

            // 获取最后一条消息时间
            const lastMsgTimestamp = chat.lastMessage?.timestamp || chat.timestamp || 0;
            const lastMsgTime = lastMsgTimestamp * 1000;  // 转毫秒

            // 检查是否超过阈值
            const elapsed = now - lastMsgTime;
            if (elapsed < thresholdMs) continue;  // 未超过阈值，跳过

            // 提取联系人信息
            let phone = '';
            let phone_lid = '';
            if (/@c\.us$/i.test(chatId)) {
                phone = chatId.replace(/@.*/, '').replace(/\D/g, '');
            } else if (/@lid$/i.test(chatId)) {
                phone_lid = chatId.replace(/@.*/, '').replace(/\D/g, '');
            }

            unreadChats.push({
                sessionId,
                sessionName,
                chatId,
                phone,
                phone_lid,
                contactName: chat.name || chat.pushname || phone || phone_lid || 'Unknown',
                unreadCount,
                lastMsgTimestamp,
                elapsedMinutes: Math.floor(elapsed / 60000)
            });
        }

        return unreadChats;

    } catch (e) {
        console.error(`[UnreadSync] Error getting unread chats for ${sessionId}:`, e.message);
        return [];
    }
}

/**
 * 获取 Chatwoot 会话的最后一条消息信息
 * @param {string} sessionId
 * @param {string} phone
 * @param {string} phone_lid
 * @returns {Object|null} { conversation_id, wa_message_id, cw_message_id, timestamp }
 */
async function getChatwootLastMessage(sessionId, phone, phone_lid) {
    try {
        const resp = await axios.get(
            `${COLLECTOR_BASE}/get-last-synced-message`,
            {
                params: { sessionId, phone, phone_lid },
                headers: getHeaders(),
                timeout: 30000
            }
        );

        if (resp.data?.ok && resp.data.lastMessage) {
            return resp.data.lastMessage;
        }
        return null;
    } catch (e) {
        console.log(`[UnreadSync] getChatwootLastMessage failed:`, e.message);
        return null;
    }
}

/**
 * 从 WA 获取指定消息之后的所有消息
 * @param {string} sessionId
 * @param {string} chatId
 * @param {string} afterMessageId - Chatwoot 最后一条消息的 WA ID
 * @param {number} afterTimestamp - 如果没有 messageId，使用时间戳
 * @returns {Array} 新消息列表
 */
async function fetchMessagesAfter(sessionId, chatId, afterMessageId, afterTimestamp) {
    const session = sessionManager?.getSession(sessionId);
    if (!session || !session.client) {
        return [];
    }

    try {
        const chat = await session.client.getChatById(chatId);
        if (!chat) return [];

        // 获取最近的消息（最多 SYNC_MAX_MESSAGES 条）
        const messages = await chat.fetchMessages({ limit: SYNC_MAX_MESSAGES });

        // 按时间戳升序排列（从旧到新）
        messages.sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));

        // 找到定位点
        let startIndex = 0;

        if (afterMessageId) {
            // 方法1：通过消息ID定位
            const foundIndex = messages.findIndex(m => {
                const msgId = m.id?._serialized || '';
                // 完全匹配
                if (msgId === afterMessageId) return true;
                // 后缀匹配（同一消息在不同端ID不同，但后缀相同）
                const msgSuffix = msgId.split('_').pop();
                const targetSuffix = afterMessageId.split('_').pop();
                return msgSuffix && targetSuffix && msgSuffix === targetSuffix;
            });

            if (foundIndex >= 0) {
                startIndex = foundIndex + 1;  // 从该消息之后开始
                console.log(`[UnreadSync] Located by messageId at index ${foundIndex}`);
            }
        }

        // 方法2：如果消息ID找不到，使用时间戳定位
        if (startIndex === 0 && afterTimestamp) {
            const foundIndex = messages.findIndex(m => (m.timestamp || 0) > afterTimestamp);
            if (foundIndex >= 0) {
                startIndex = foundIndex;
                console.log(`[UnreadSync] Located by timestamp at index ${foundIndex}`);
            }
        }

        // 提取新消息
        const newMessages = messages.slice(startIndex);
        console.log(`[UnreadSync] Found ${newMessages.length} new messages after position ${startIndex}`);

        return newMessages;

    } catch (e) {
        console.error(`[UnreadSync] fetchMessagesAfter error:`, e.message);
        return [];
    }
}

/**
 * 将消息同步到 Chatwoot
 * @param {Object} chatInfo - 聊天信息
 * @param {Array} messages - 要同步的消息列表
 * @returns {Object} { success, synced, failed }
 */
async function syncMessagesToChatwoot(chatInfo, messages) {
    if (!messages || messages.length === 0) {
        return { success: true, synced: 0, failed: 0, skipped: 0 };
    }

    const { sessionId, sessionName, chatId, phone, phone_lid, contactName } = chatInfo;

    try {
        // 准备消息数据
        const preparedMessages = messages.map(msg => {
            const msgId = msg.id?._serialized || '';
            return {
                id: msgId,
                body: msg.body || '',
                fromMe: !!msg.fromMe,
                timestamp: msg.timestamp || Math.floor(Date.now() / 1000),
                type: msg.type || 'chat',
                hasMedia: !!msg.hasMedia,
                // 引用消息
                quotedMsg: msg.hasQuotedMsg ? {
                    id: msg._data?.quotedStanzaID || '',
                    body: msg._data?.quotedMsg?.body || '',
                    fromMe: !!msg._data?.quotedMsg?.fromMe
                } : null
            };
        });

        // 调用 Collector 的增量同步接口
        const resp = await axios.post(
            `${COLLECTOR_BASE}/sync-incremental`,
            {
                sessionId,
                sessionName,
                chatId,
                phone,
                phone_lid,
                contactName,
                messages: preparedMessages
            },
            {
                headers: getHeaders(),
                timeout: 300000  // 5分钟超时
            }
        );

        if (resp.data?.ok) {
            return {
                success: true,
                synced: resp.data.synced || 0,
                failed: resp.data.failed || 0,
                skipped: resp.data.skipped || 0,
                conversation_id: resp.data.conversation_id
            };
        }

        return { success: false, synced: 0, failed: messages.length, error: resp.data?.error };

    } catch (e) {
        console.error(`[UnreadSync] syncMessagesToChatwoot error:`, e.message);
        return { success: false, synced: 0, failed: messages.length, error: e.message };
    }
}

/**
 * 标记聊天已读
 * @param {string} sessionId
 * @param {string} chatId
 */
async function markChatAsRead(sessionId, chatId) {
    const session = sessionManager?.getSession(sessionId);
    if (!session || !session.client) {
        return false;
    }

    try {
        const chat = await session.client.getChatById(chatId);
        if (chat) {
            await chat.sendSeen();
            console.log(`[UnreadSync] Marked as read: ${chatId}`);
            return true;
        }
    } catch (e) {
        console.error(`[UnreadSync] markChatAsRead error:`, e.message);
    }
    return false;
}

/**
 * 同步单个未读聊天
 * @param {Object} chatInfo - 未读聊天信息
 * @returns {Object} 同步结果
 */
async function syncUnreadChat(chatInfo) {
    const { sessionId, sessionName, chatId, phone, phone_lid, contactName, unreadCount, elapsedMinutes } = chatInfo;

    console.log(`[UnreadSync] ----------------------------------------`);
    console.log(`[UnreadSync] Processing: ${sessionName}>${contactName}`);
    console.log(`[UnreadSync] ChatId: ${chatId}, Unread: ${unreadCount}, Elapsed: ${elapsedMinutes}min`);

    const result = {
        sessionId,
        chatId,
        contactName,
        action: 'unknown',
        synced: 0,
        markedRead: false,
        error: null
    };

    try {
        // 1. 获取 Chatwoot 最后同步的消息
        const lastSynced = await getChatwootLastMessage(sessionId, phone, phone_lid);

        let afterMessageId = null;
        let afterTimestamp = null;

        if (lastSynced) {
            afterMessageId = lastSynced.wa_message_id;
            afterTimestamp = lastSynced.timestamp;
            console.log(`[UnreadSync] Last synced: wa_id=${afterMessageId?.substring(0, 30)}, ts=${afterTimestamp}`);
        } else {
            // Chatwoot 没有该联系人的消息记录，使用回退时间
            afterTimestamp = Math.floor(Date.now() / 1000) - (SYNC_FALLBACK_HOURS * 3600);
            console.log(`[UnreadSync] No history in Chatwoot, fallback to ${SYNC_FALLBACK_HOURS}h ago`);
        }

        // 2. 获取 WA 端新消息
        const newMessages = await fetchMessagesAfter(sessionId, chatId, afterMessageId, afterTimestamp);

        if (newMessages.length === 0) {
            console.log(`[UnreadSync] No new messages to sync`);
            result.action = 'no_new_messages';
            // 即使没有新消息，也标记已读
            result.markedRead = await markChatAsRead(sessionId, chatId);
            return result;
        }

        console.log(`[UnreadSync] Found ${newMessages.length} messages to sync`);

        // 3. 同步消息到 Chatwoot
        const syncResult = await syncMessagesToChatwoot(chatInfo, newMessages);

        result.synced = syncResult.synced;
        result.failed = syncResult.failed;
        result.skipped = syncResult.skipped;

        if (syncResult.success && syncResult.synced > 0) {
            result.action = 'synced';
            // 4. 同步成功，标记已读
            result.markedRead = await markChatAsRead(sessionId, chatId);
            console.log(`[UnreadSync] Sync OK: ${syncResult.synced} messages, markedRead=${result.markedRead}`);
        } else if (syncResult.synced === 0 && (syncResult.skipped || 0) > 0) {
            result.action = 'all_skipped';
            // 全部跳过（已存在），也标记已读
            result.markedRead = await markChatAsRead(sessionId, chatId);
            console.log(`[UnreadSync] All skipped (already synced), markedRead=${result.markedRead}`);
        } else {
            result.action = 'failed';
            result.error = syncResult.error;
            // 同步失败，保持未读，下次重试
            console.log(`[UnreadSync] Sync FAILED: ${syncResult.error}, keeping unread for retry`);
        }

        return result;

    } catch (e) {
        console.error(`[UnreadSync] syncUnreadChat error:`, e.message);
        result.action = 'error';
        result.error = e.message;
        return result;
    }
}

/**
 * 扫描单个 session 的未读消息
 */
async function scanSessionUnread(sessionId) {
    const sessionName = sessionManager?.getSessionName(sessionId) || sessionId;

    console.log(`[UnreadSync] Scanning session: ${sessionName} (${sessionId})`);

    const unreadChats = await getUnreadChats(sessionId);

    if (unreadChats.length === 0) {
        console.log(`[UnreadSync] [${sessionName}] No unread chats exceeding threshold`);
        return { sessionId, sessionName, processed: 0, results: [] };
    }

    console.log(`[UnreadSync] [${sessionName}] Found ${unreadChats.length} unread chats exceeding ${SYNC_THRESHOLD_MIN}min`);

    const results = [];

    for (const chat of unreadChats) {
        const result = await syncUnreadChat(chat);
        results.push(result);

        // 每个聊天之间稍微延迟
        await new Promise(r => setTimeout(r, 500));
    }

    return { sessionId, sessionName, processed: unreadChats.length, results };
}

/**
 * 扫描所有 session 的未读消息
 */
async function scanAllSessions() {
    if (!sessionManager) {
        console.warn('[UnreadSync] SessionManager not available');
        return;
    }

    const startTime = Date.now();
    const startTimeStr = formatBeijingTime(new Date(startTime));

    console.log('[UnreadSync] ========================================');
    console.log('[UnreadSync] ★★★ 未读消息扫描开始 ★★★');
    console.log(`[UnreadSync] 时间: ${startTimeStr}`);
    console.log(`[UnreadSync] 配置: 间隔=${SYNC_INTERVAL_MIN}min, 阈值=${SYNC_THRESHOLD_MIN}min, 最大=${SYNC_MAX_MESSAGES}条`);
    console.log('[UnreadSync] ========================================');

    const allSessions = sessionManager.getAllSessions();
    const sessionIds = Object.keys(allSessions);

    console.log(`[UnreadSync] 共 ${sessionIds.length} 个 session`);

    const allResults = [];
    let totalProcessed = 0;
    let totalSynced = 0;
    let totalFailed = 0;

    for (const sessionId of sessionIds) {
        const session = allSessions[sessionId];
        if (session.sessionStatus !== 'ready') {
            console.log(`[UnreadSync] Skip session ${sessionId}: status=${session.sessionStatus}`);
            continue;
        }

        const scanResult = await scanSessionUnread(sessionId);
        allResults.push(scanResult);

        totalProcessed += scanResult.processed;
        for (const r of scanResult.results) {
            totalSynced += r.synced || 0;
            totalFailed += r.failed || 0;
        }

        // session 之间延迟
        await new Promise(r => setTimeout(r, 1000));
    }

    const duration = Date.now() - startTime;
    const endTimeStr = formatBeijingTime(new Date());

    console.log('[UnreadSync] ========================================');
    console.log('[UnreadSync] ★★★ 未读消息扫描完成 ★★★');
    console.log(`[UnreadSync] 结束时间: ${endTimeStr}`);
    console.log(`[UnreadSync] 耗时: ${formatDuration(duration)}`);
    console.log(`[UnreadSync] 处理聊天: ${totalProcessed}, 同步消息: ${totalSynced}, 失败: ${totalFailed}`);
    console.log('[UnreadSync] ========================================');

    return {
        startTime: startTimeStr,
        endTime: endTimeStr,
        duration: formatDuration(duration),
        sessions: sessionIds.length,
        totalProcessed,
        totalSynced,
        totalFailed,
        results: allResults
    };
}

/**
 * 调度下次扫描
 */
function scheduleNextScan() {
    const intervalMs = SYNC_INTERVAL_MIN * 60 * 1000;

    console.log(`[UnreadSync] Next scan in ${SYNC_INTERVAL_MIN} minutes`);

    scheduledTimer = setTimeout(async () => {
        try {
            await scanAllSessions();
        } catch (e) {
            console.error('[UnreadSync] Scan error:', e.message);
        } finally {
            // 完成后调度下一次
            scheduleNextScan();
        }
    }, intervalMs);
}

/**
 * 初始化模块
 * @param {Object} sm - SessionManager 实例
 * @param {Object} sessionsObj - whatsapp-manager 的 sessions 对象
 */
function initialize(sm, sessionsObj) {
    sessionManager = sm;
    sessions = sessionsObj;

    if (!SYNC_ENABLED) {
        console.log('[UnreadSync] Disabled (UNREAD_SYNC_ENABLED=0)');
        return;
    }

    console.log('[UnreadSync] ========================================');
    console.log('[UnreadSync] Module initialized');
    console.log(`[UnreadSync] Interval: ${SYNC_INTERVAL_MIN} minutes`);
    console.log(`[UnreadSync] Threshold: ${SYNC_THRESHOLD_MIN} minutes`);
    console.log(`[UnreadSync] Max messages: ${SYNC_MAX_MESSAGES}`);
    console.log(`[UnreadSync] Fallback hours: ${SYNC_FALLBACK_HOURS}`);
    console.log('[UnreadSync] ========================================');

    // 启动后延迟 2 分钟执行第一次扫描（等待 sessions 就绪）
    setTimeout(() => {
        console.log('[UnreadSync] Starting first scan...');
        scanAllSessions().catch(e => {
            console.error('[UnreadSync] First scan error:', e.message);
        }).finally(() => {
            // 开始定时调度
            scheduleNextScan();
        });
    }, 2 * 60 * 1000);
}

/**
 * 手动触发扫描（可通过 API 调用）
 */
async function triggerScan(sessionId = null) {
    if (sessionId) {
        return scanSessionUnread(sessionId);
    }
    return scanAllSessions();
}

/**
 * 停止定时器
 */
function stop() {
    if (scheduledTimer) {
        clearTimeout(scheduledTimer);
        scheduledTimer = null;
        console.log('[UnreadSync] Stopped');
    }
}

// ============================================================
// 导出
// ============================================================
module.exports = {
    initialize,
    scanAllSessions,
    scanSessionUnread,
    syncUnreadChat,
    triggerScan,
    stop,
    // 配置常量导出（便于外部读取）
    SYNC_ENABLED,
    SYNC_INTERVAL_MIN,
    SYNC_THRESHOLD_MIN,
    SYNC_MAX_MESSAGES,
    SYNC_FALLBACK_HOURS
};