/**
 * Startup Sync v3 - 简化版启动同步
 *
 * 特点：
 * - 直接在 preloadChatsSafe 成功后触发
 * - 不依赖复杂的 tracker 机制
 * - 每个 session 独立同步，互不干扰
 */

const axios = require('axios');

// 配置
const COLLECTOR_BASE = process.env.COLLECTOR_BASE || `http://127.0.0.1:${process.env.COLLECTOR_PORT || 7001}`;
const COLLECTOR_TOKEN = process.env.COLLECTOR_INGEST_TOKEN || process.env.API_TOKEN || '';

// 同步配置
const SYNC_ON_STARTUP = process.env.SYNC_ON_STARTUP === '1' || process.env.SYNC_ON_STARTUP === 'true';
const STARTUP_SYNC_CONTACTS = parseInt(process.env.STARTUP_SYNC_CONTACTS || '3');
const STARTUP_SYNC_HOURS = parseInt(process.env.STARTUP_SYNC_HOURS || '12');
const STARTUP_SYNC_CONCURRENCY = parseInt(process.env.STARTUP_SYNC_CONCURRENCY || '3');

// 记录已同步的 session，防止重复
const syncedSessions = new Set();

/**
 * 为单个 session 执行启动同步
 * 在 preloadChatsSafe 成功后调用
 *
 * @param {string} sessionId - Session ID
 * @param {string} sessionName - Session 名称
 * @param {Object} client - WhatsApp client
 * @param {Array} chats - 已加载的聊天列表
 */
async function syncSessionOnStartup(sessionId, sessionName, client, chats) {
    // 检查是否启用
    if (!SYNC_ON_STARTUP) {
        return;
    }

    // 防止重复同步
    if (syncedSessions.has(sessionId)) {
        console.log(`[StartupSync] Session ${sessionId} already synced, skipping`);
        return;
    }
    syncedSessions.add(sessionId);

    console.log(`[StartupSync] ========== Syncing session ${sessionId} (${sessionName}) ==========`);
    console.log(`[StartupSync] Config: contacts=${STARTUP_SYNC_CONTACTS}, hours=${STARTUP_SYNC_HOURS}`);

    const startTime = Date.now();

    try {
        // 筛选私聊
        const privateChats = chats
            .filter(c => !c.isGroup && c.id?._serialized && !c.id._serialized.includes('status'))
            .map(c => ({
                chatId: c.id._serialized,
                name: c.name || c.id.user || 'Unknown',
                lastMsgTimestamp: c.lastMessage?.timestamp || 0
            }))
            .filter(c => c.lastMsgTimestamp > 0)
            .sort((a, b) => b.lastMsgTimestamp - a.lastMsgTimestamp)
            .slice(0, STARTUP_SYNC_CONTACTS);

        console.log(`[StartupSync] Selected ${privateChats.length} private chats`);

        let synced = 0;
        let failed = 0;

        // 并发处理
        const chunks = [];
        for (let i = 0; i < privateChats.length; i += STARTUP_SYNC_CONCURRENCY) {
            chunks.push(privateChats.slice(i, i + STARTUP_SYNC_CONCURRENCY));
        }

        for (const chunk of chunks) {
            const results = await Promise.allSettled(
                chunk.map(chat => syncSingleChat(sessionId, sessionName, chat))
            );

            for (const result of results) {
                if (result.status === 'fulfilled' && result.value.success) {
                    synced += result.value.messageCount || 0;
                } else {
                    failed++;
                }
            }
        }

        const elapsed = Date.now() - startTime;
        console.log(`[StartupSync] ========== Session ${sessionId} completed in ${elapsed}ms ==========`);
        console.log(`[StartupSync] Summary: synced=${synced} messages, failed=${failed} contacts`);

    } catch (err) {
        console.error(`[StartupSync] Session ${sessionId} error:`, err.message);
    }
}

/**
 * 同步单个聊天
 */
async function syncSingleChat(sessionId, sessionName, chat) {
    const { chatId, name, lastMsgTimestamp } = chat;
    const cutoffTimestamp = lastMsgTimestamp - (STARTUP_SYNC_HOURS * 3600);

    // 解析电话号码
    let phoneNumber = null;
    let phoneLid = null;

    if (chatId.endsWith('@c.us')) {
        phoneNumber = chatId.replace('@c.us', '');
    } else if (chatId.includes('@lid')) {
        const match = chatId.match(/^(\d+)@lid/);
        if (match) phoneLid = match[1];
    }

    console.log(`[StartupSync] Syncing ${chatId}`);

    try {
        const syncResult = await axios.post(
            `${COLLECTOR_BASE}/sync-messages`,
            {
                chatId,
                sessionId,
                sessionName,
                phone: phoneNumber,
                phone_lid: phoneLid,
                contactName: name,
                after: new Date(cutoffTimestamp * 1000).toISOString(),
                before: new Date(lastMsgTimestamp * 1000).toISOString(),
                isStartupSync: true,
                replace: false,
                direction: 'both',
                batchSize: 50,
                useBatchCreate: true
            },
            {
                headers: COLLECTOR_TOKEN ? { 'x-api-token': COLLECTOR_TOKEN } : {},
                timeout: 300000  // 5分钟超时
            }
        );

        const messageCount = syncResult.data?.synced || syncResult.data?.created || 0;
        console.log(`[StartupSync] ${chatId}: synced ${messageCount} messages`);
        return { success: true, messageCount };

    } catch (err) {
        const errorMsg = err.response?.data?.error || err.message;
        console.error(`[StartupSync] ${chatId}: failed - ${errorMsg}`);
        return { success: false, error: errorMsg };
    }
}

/**
 * 重置同步状态（用于测试）
 */
function resetSyncState() {
    syncedSessions.clear();
}

// 空函数，保持兼容
function scheduleStartupSync() {}
function executeStartupSync() {}

module.exports = {
    syncSessionOnStartup,
    resetSyncState,
    scheduleStartupSync,
    executeStartupSync,
    SYNC_ON_STARTUP,
    STARTUP_SYNC_CONTACTS,
    STARTUP_SYNC_HOURS,
    STARTUP_SYNC_CONCURRENCY
};