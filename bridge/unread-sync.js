/**
 * unread-sync.js - 消息同步守护模块 V2.0
 *
 * 功能一：未读消息同步
 * - 每30分钟扫描所有 session 的 WA 未读消息
 * - 筛选"超过30分钟未读"的聊天
 * - 增量同步到 Chatwoot，成功后标记已读
 *
 * 功能二：未回复消息同步（V2.0 新增）
 * - 每30分钟扫描过去30分钟～24小时内有消息的聊天
 * - 检查最后一条消息是否是客户发的（fromMe=false）
 * - 如果客服未回复 → 增量同步确保消息不丢失
 *
 * 核心原则：
 * - 以 WA 网页端时间为准（chat.lastMessage.timestamp）
 * - 增量同步：从 Chatwoot 最后消息之后开始
 * - 所有同步的消息都保存映射（钢印原则）
 *
 * 环境变量配置：
 * - UNREAD_SYNC_ENABLED: 是否启用 (1/0)，默认 1
 * - UNREAD_SYNC_INTERVAL_MIN: 扫描间隔（分钟），默认 30
 * - UNREAD_SYNC_THRESHOLD_MIN: 未读阈值（分钟），默认 30
 * - UNREPLIED_SYNC_ENABLED: 未回复同步是否启用 (1/0)，默认 1
 * - UNREPLIED_SYNC_MAX_HOURS: 未回复检查的最大小时数，默认 24
 * - SYNC_MAX_MESSAGES: 单次同步最大消息数，默认 200
 * - SYNC_FALLBACK_HOURS: 无历史记录时同步的小时数，默认 12
 */

const axios = require('axios');

// ============================================================
// 配置
// ============================================================
const SYNC_ENABLED = process.env.UNREAD_SYNC_ENABLED !== '0';
const SYNC_INTERVAL_MIN = parseInt(process.env.UNREAD_SYNC_INTERVAL_MIN || '30');
const SYNC_THRESHOLD_MIN = parseInt(process.env.UNREAD_SYNC_THRESHOLD_MIN || '30');
const SYNC_MAX_MESSAGES = parseInt(process.env.SYNC_MAX_MESSAGES || '200');
const SYNC_FALLBACK_HOURS = parseInt(process.env.SYNC_FALLBACK_HOURS || '12');

// V2.0 新增：未回复同步配置
const UNREPLIED_SYNC_ENABLED = process.env.UNREPLIED_SYNC_ENABLED !== '0';
const UNREPLIED_SYNC_MAX_HOURS = parseInt(process.env.UNREPLIED_SYNC_MAX_HOURS || '24');

// Collector 配置
const COLLECTOR_BASE = process.env.COLLECTOR_BASE || `http://127.0.0.1:${process.env.COLLECTOR_PORT || 7001}`;
const COLLECTOR_TOKEN = process.env.COLLECTOR_INGEST_TOKEN || process.env.API_TOKEN || '';

// ============================================================
// 依赖注入
// ============================================================
let sessionManager = null;
let sessions = null;
let scheduledTimer = null;

// ============================================================
// 工具函数
// ============================================================

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

function formatDuration(ms) {
    const totalSeconds = Math.floor(ms / 1000);
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;

    if (hours > 0) return `${hours}h${minutes}m${seconds}s`;
    if (minutes > 0) return `${minutes}m${seconds}s`;
    return `${seconds}s`;
}

function getHeaders() {
    return COLLECTOR_TOKEN ? { 'x-api-token': COLLECTOR_TOKEN } : {};
}

/**
 * 从 chatId 提取 phone 和 phone_lid
 */
function extractPhoneFromChatId(chatId) {
    let phone = '';
    let phone_lid = '';

    if (/@c\.us$/i.test(chatId)) {
        phone = chatId.replace(/@.*/, '').replace(/\D/g, '');
    } else if (/@lid$/i.test(chatId)) {
        phone_lid = chatId.replace(/@.*/, '').replace(/\D/g, '');
    }

    return { phone, phone_lid };
}

// ============================================================
// 功能一：未读消息同步
// ============================================================

/**
 * 获取单个 session 的未读聊天列表
 * 筛选条件：unreadCount > 0 且超过阈值时间
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
            const chatId = chat.id?._serialized || '';

            // 跳过群聊和广播
            if (chat.isGroup || /@g\.us$/i.test(chatId) || /broadcast/i.test(chatId)) {
                continue;
            }

            // 检查未读数
            const unreadCount = chat.unreadCount || 0;
            if (unreadCount <= 0) continue;

            // 获取最后一条消息时间（以 WA 时间为准）
            const lastMsgTimestamp = chat.lastMessage?.timestamp || chat.timestamp || 0;
            const lastMsgTime = lastMsgTimestamp * 1000;

            // 检查是否超过阈值
            const elapsed = now - lastMsgTime;
            if (elapsed < thresholdMs) continue;

            const { phone, phone_lid } = extractPhoneFromChatId(chatId);

            unreadChats.push({
                sessionId,
                sessionName,
                chatId,
                phone,
                phone_lid,
                contactName: chat.name || chat.pushname || phone || phone_lid || 'Unknown',
                unreadCount,
                lastMsgTimestamp,
                lastMsgFromMe: chat.lastMessage?.fromMe || false,
                elapsedMinutes: Math.floor(elapsed / 60000),
                syncType: 'unread'
            });
        }

        return unreadChats;

    } catch (e) {
        console.error(`[UnreadSync] Error getting unread chats for ${sessionId}:`, e.message);
        return [];
    }
}

// ============================================================
// 功能二：未回复消息同步（V2.0 新增）
// ============================================================

/**
 * 获取单个 session 的未回复聊天列表
 * 筛选条件：
 * - 最后消息时间在 [30分钟前, 24小时前] 区间
 * - 最后一条消息是客户发的（fromMe = false）
 * - 排除已经在未读列表中的聊天（避免重复处理）
 */
async function getUnrepliedChats(sessionId, excludeChatIds = new Set()) {
    const session = sessionManager?.getSession(sessionId);
    if (!session || session.sessionStatus !== 'ready' || !session.client) {
        return [];
    }

    const sessionName = sessionManager.getSessionName(sessionId) || sessionId;
    const now = Date.now();
    const minTimeMs = SYNC_THRESHOLD_MIN * 60 * 1000;  // 30分钟
    const maxTimeMs = UNREPLIED_SYNC_MAX_HOURS * 60 * 60 * 1000;  // 24小时

    try {
        const chats = await session.client.getChats();
        const unrepliedChats = [];

        for (const chat of chats) {
            const chatId = chat.id?._serialized || '';

            // 跳过群聊和广播
            if (chat.isGroup || /@g\.us$/i.test(chatId) || /broadcast/i.test(chatId)) {
                continue;
            }

            // 跳过已在未读列表中的
            if (excludeChatIds.has(chatId)) {
                continue;
            }

            // 获取最后一条消息信息
            const lastMsg = chat.lastMessage;
            if (!lastMsg) continue;

            const lastMsgTimestamp = lastMsg.timestamp || chat.timestamp || 0;
            const lastMsgTime = lastMsgTimestamp * 1000;
            const elapsed = now - lastMsgTime;

            // 检查时间范围：30分钟前 ~ 24小时前
            if (elapsed < minTimeMs || elapsed > maxTimeMs) {
                continue;
            }

            // 关键检查：最后一条消息是否是客户发的（fromMe = false）
            const lastMsgFromMe = lastMsg.fromMe || false;
            if (lastMsgFromMe) {
                // 最后一条是客服发的，说明已回复，跳过
                continue;
            }

            // 客户消息垫底，需要检查同步
            const { phone, phone_lid } = extractPhoneFromChatId(chatId);

            unrepliedChats.push({
                sessionId,
                sessionName,
                chatId,
                phone,
                phone_lid,
                contactName: chat.name || chat.pushname || phone || phone_lid || 'Unknown',
                unreadCount: chat.unreadCount || 0,
                lastMsgTimestamp,
                lastMsgFromMe: false,
                lastMsgBody: (lastMsg.body || '').substring(0, 50),
                elapsedMinutes: Math.floor(elapsed / 60000),
                syncType: 'unreplied'
            });
        }

        return unrepliedChats;

    } catch (e) {
        console.error(`[UnreadSync] Error getting unreplied chats for ${sessionId}:`, e.message);
        return [];
    }
}

// ============================================================
// 通用同步逻辑
// ============================================================

/**
 * 获取 Chatwoot 会话的最后一条消息信息
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
 */
async function fetchMessagesAfter(sessionId, chatId, afterMessageId, afterTimestamp) {
    const session = sessionManager?.getSession(sessionId);
    if (!session || !session.client) {
        return [];
    }

    try {
        const chat = await session.client.getChatById(chatId);
        if (!chat) return [];

        const messages = await chat.fetchMessages({ limit: SYNC_MAX_MESSAGES });

        // 按时间戳升序排列
        messages.sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));

        let startIndex = 0;

        // 方法1：通过消息ID定位
        if (afterMessageId) {
            const foundIndex = messages.findIndex(m => {
                const msgId = m.id?._serialized || '';
                if (msgId === afterMessageId) return true;
                // 后缀匹配
                const msgSuffix = msgId.split('_').pop();
                const targetSuffix = afterMessageId.split('_').pop();
                return msgSuffix && targetSuffix && msgSuffix === targetSuffix;
            });

            if (foundIndex >= 0) {
                startIndex = foundIndex + 1;
                console.log(`[UnreadSync] Located by messageId at index ${foundIndex}`);
            }
        }

        // 方法2：通过时间戳定位
        if (startIndex === 0 && afterTimestamp) {
            const foundIndex = messages.findIndex(m => (m.timestamp || 0) > afterTimestamp);
            if (foundIndex >= 0) {
                startIndex = foundIndex;
                console.log(`[UnreadSync] Located by timestamp at index ${foundIndex}`);
            }
        }

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
 */
async function syncMessagesToChatwoot(chatInfo, messages) {
    if (!messages || messages.length === 0) {
        return { success: true, synced: 0, failed: 0, skipped: 0 };
    }

    const { sessionId, sessionName, chatId, phone, phone_lid, contactName } = chatInfo;

    try {
        const preparedMessages = messages.map(msg => {
            const msgId = msg.id?._serialized || '';
            return {
                id: msgId,
                body: msg.body || '',
                fromMe: !!msg.fromMe,
                timestamp: msg.timestamp || Math.floor(Date.now() / 1000),
                type: msg.type || 'chat',
                hasMedia: !!msg.hasMedia,
                quotedMsg: msg.hasQuotedMsg ? {
                    id: msg._data?.quotedStanzaID || '',
                    body: msg._data?.quotedMsg?.body || '',
                    fromMe: !!msg._data?.quotedMsg?.fromMe
                } : null
            };
        });

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
                timeout: 300000
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
 * 同步单个聊天（通用逻辑，适用于未读和未回复）
 */
async function syncChat(chatInfo) {
    const { sessionId, sessionName, chatId, phone, phone_lid, contactName, syncType, elapsedMinutes } = chatInfo;

    console.log(`[UnreadSync] ----------------------------------------`);
    console.log(`[UnreadSync] [${syncType.toUpperCase()}] Processing: ${sessionName}>${contactName}`);
    console.log(`[UnreadSync] ChatId: ${chatId}, Elapsed: ${elapsedMinutes}min`);

    const result = {
        sessionId,
        chatId,
        contactName,
        syncType,
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
            // Chatwoot 没有该联系人的消息记录
            afterTimestamp = Math.floor(Date.now() / 1000) - (SYNC_FALLBACK_HOURS * 3600);
            console.log(`[UnreadSync] No history in Chatwoot, fallback to ${SYNC_FALLBACK_HOURS}h ago`);
        }

        // 2. 获取 WA 端新消息
        const newMessages = await fetchMessagesAfter(sessionId, chatId, afterMessageId, afterTimestamp);

        if (newMessages.length === 0) {
            console.log(`[UnreadSync] No new messages to sync`);
            result.action = 'no_new_messages';

            // 未读类型：即使没有新消息也标记已读
            if (syncType === 'unread') {
                result.markedRead = await markChatAsRead(sessionId, chatId);
            }
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
            // 未读类型：同步成功后标记已读
            if (syncType === 'unread') {
                result.markedRead = await markChatAsRead(sessionId, chatId);
            }
            console.log(`[UnreadSync] Sync OK: ${syncResult.synced} messages, markedRead=${result.markedRead}`);
        } else if (syncResult.synced === 0 && (syncResult.skipped || 0) > 0) {
            result.action = 'all_skipped';
            if (syncType === 'unread') {
                result.markedRead = await markChatAsRead(sessionId, chatId);
            }
            console.log(`[UnreadSync] All skipped (already synced), markedRead=${result.markedRead}`);
        } else {
            result.action = 'failed';
            result.error = syncResult.error;
            console.log(`[UnreadSync] Sync FAILED: ${syncResult.error}`);
        }

        return result;

    } catch (e) {
        console.error(`[UnreadSync] syncChat error:`, e.message);
        result.action = 'error';
        result.error = e.message;
        return result;
    }
}

// ============================================================
// 主扫描逻辑
// ============================================================

/**
 * 扫描单个 session
 */
async function scanSession(sessionId) {
    const sessionName = sessionManager?.getSessionName(sessionId) || sessionId;
    console.log(`[UnreadSync] Scanning session: ${sessionName} (${sessionId})`);

    const results = {
        sessionId,
        sessionName,
        unread: { processed: 0, synced: 0, failed: 0 },
        unreplied: { processed: 0, synced: 0, failed: 0 },
        details: []
    };

    // 1. 获取未读聊天
    const unreadChats = await getUnreadChats(sessionId);
    const unreadChatIds = new Set(unreadChats.map(c => c.chatId));

    console.log(`[UnreadSync] [${sessionName}] Unread chats: ${unreadChats.length}`);

    // 2. 获取未回复聊天（排除未读的，避免重复）
    let unrepliedChats = [];
    if (UNREPLIED_SYNC_ENABLED) {
        unrepliedChats = await getUnrepliedChats(sessionId, unreadChatIds);
        console.log(`[UnreadSync] [${sessionName}] Unreplied chats: ${unrepliedChats.length}`);
    }

    // 3. 合并处理
    const allChats = [...unreadChats, ...unrepliedChats];

    if (allChats.length === 0) {
        console.log(`[UnreadSync] [${sessionName}] No chats to process`);
        return results;
    }

    // 4. 逐个同步
    for (const chat of allChats) {
        const result = await syncChat(chat);
        results.details.push(result);

        if (chat.syncType === 'unread') {
            results.unread.processed++;
            results.unread.synced += result.synced || 0;
            if (result.action === 'failed' || result.action === 'error') {
                results.unread.failed++;
            }
        } else {
            results.unreplied.processed++;
            results.unreplied.synced += result.synced || 0;
            if (result.action === 'failed' || result.action === 'error') {
                results.unreplied.failed++;
            }
        }

        // 每个聊天之间稍微延迟
        await new Promise(r => setTimeout(r, 500));
    }

    return results;
}

/**
 * 扫描所有 session
 */
async function scanAllSessions() {
    if (!sessionManager) {
        console.warn('[UnreadSync] SessionManager not available');
        return;
    }

    const startTime = Date.now();
    const startTimeStr = formatBeijingTime(new Date(startTime));

    console.log('[UnreadSync] ========================================');
    console.log('[UnreadSync] ★★★ 消息同步守护扫描开始 ★★★');
    console.log(`[UnreadSync] 时间: ${startTimeStr}`);
    console.log(`[UnreadSync] 配置: 间隔=${SYNC_INTERVAL_MIN}min, 未读阈值=${SYNC_THRESHOLD_MIN}min`);
    console.log(`[UnreadSync] 未回复检查: ${UNREPLIED_SYNC_ENABLED ? `启用 (最大${UNREPLIED_SYNC_MAX_HOURS}小时)` : '禁用'}`);
    console.log('[UnreadSync] ========================================');

    const allSessions = sessionManager.getAllSessions();
    const sessionIds = Object.keys(allSessions);

    console.log(`[UnreadSync] 共 ${sessionIds.length} 个 session`);

    const allResults = [];
    let totalUnreadProcessed = 0, totalUnreadSynced = 0;
    let totalUnrepliedProcessed = 0, totalUnrepliedSynced = 0;

    for (const sessionId of sessionIds) {
        const session = allSessions[sessionId];
        if (session.sessionStatus !== 'ready') {
            console.log(`[UnreadSync] Skip session ${sessionId}: status=${session.sessionStatus}`);
            continue;
        }

        const scanResult = await scanSession(sessionId);
        allResults.push(scanResult);

        totalUnreadProcessed += scanResult.unread.processed;
        totalUnreadSynced += scanResult.unread.synced;
        totalUnrepliedProcessed += scanResult.unreplied.processed;
        totalUnrepliedSynced += scanResult.unreplied.synced;

        await new Promise(r => setTimeout(r, 1000));
    }

    const duration = Date.now() - startTime;
    const endTimeStr = formatBeijingTime(new Date());

    console.log('[UnreadSync] ========================================');
    console.log('[UnreadSync] ★★★ 消息同步守护扫描完成 ★★★');
    console.log(`[UnreadSync] 结束时间: ${endTimeStr}`);
    console.log(`[UnreadSync] 耗时: ${formatDuration(duration)}`);
    console.log(`[UnreadSync] 未读: 处理${totalUnreadProcessed}个聊天, 同步${totalUnreadSynced}条消息`);
    console.log(`[UnreadSync] 未回复: 处理${totalUnrepliedProcessed}个聊天, 同步${totalUnrepliedSynced}条消息`);
    console.log('[UnreadSync] ========================================');

    return {
        startTime: startTimeStr,
        endTime: endTimeStr,
        duration: formatDuration(duration),
        sessions: sessionIds.length,
        unread: { processed: totalUnreadProcessed, synced: totalUnreadSynced },
        unreplied: { processed: totalUnrepliedProcessed, synced: totalUnrepliedSynced },
        results: allResults
    };
}

// ============================================================
// 定时调度
// ============================================================

function scheduleNextScan() {
    const intervalMs = SYNC_INTERVAL_MIN * 60 * 1000;

    console.log(`[UnreadSync] Next scan in ${SYNC_INTERVAL_MIN} minutes`);

    scheduledTimer = setTimeout(async () => {
        try {
            await scanAllSessions();
        } catch (e) {
            console.error('[UnreadSync] Scan error:', e.message);
        } finally {
            scheduleNextScan();
        }
    }, intervalMs);
}

/**
 * 初始化模块
 */
function initialize(sm, sessionsObj) {
    sessionManager = sm;
    sessions = sessionsObj;

    if (!SYNC_ENABLED) {
        console.log('[UnreadSync] Disabled (UNREAD_SYNC_ENABLED=0)');
        return;
    }

    console.log('[UnreadSync] ========================================');
    console.log('[UnreadSync] 消息同步守护模块 V2.0 已启动');
    console.log(`[UnreadSync] 扫描间隔: ${SYNC_INTERVAL_MIN} 分钟`);
    console.log(`[UnreadSync] 未读阈值: ${SYNC_THRESHOLD_MIN} 分钟`);
    console.log(`[UnreadSync] 未回复同步: ${UNREPLIED_SYNC_ENABLED ? '启用' : '禁用'}`);
    if (UNREPLIED_SYNC_ENABLED) {
        console.log(`[UnreadSync] 未回复检查范围: ${SYNC_THRESHOLD_MIN}分钟 ~ ${UNREPLIED_SYNC_MAX_HOURS}小时`);
    }
    console.log(`[UnreadSync] 单次最大消息数: ${SYNC_MAX_MESSAGES}`);
    console.log(`[UnreadSync] 回退同步小时数: ${SYNC_FALLBACK_HOURS}`);
    console.log('[UnreadSync] ========================================');

    // 启动后延迟 2 分钟执行第一次扫描
    setTimeout(() => {
        console.log('[UnreadSync] Starting first scan...');
        scanAllSessions().catch(e => {
            console.error('[UnreadSync] First scan error:', e.message);
        }).finally(() => {
            scheduleNextScan();
        });
    }, 2 * 60 * 1000);
}

/**
 * 手动触发扫描
 */
async function triggerScan(sessionId = null) {
    if (sessionId) {
        return scanSession(sessionId);
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
    scanSession,
    syncChat,
    triggerScan,
    stop,
    // 配置常量导出
    SYNC_ENABLED,
    SYNC_INTERVAL_MIN,
    SYNC_THRESHOLD_MIN,
    SYNC_MAX_MESSAGES,
    SYNC_FALLBACK_HOURS,
    UNREPLIED_SYNC_ENABLED,
    UNREPLIED_SYNC_MAX_HOURS
};