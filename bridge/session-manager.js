/**
 * SessionManager - 动态会话管理模块 (增强版)
 *
 * 功能：
 * 1. 动态从 AdsPower API 获取 profile 列表和名称
 * 2. 管理会话生命周期（注册、状态追踪、心跳检测）
 * 3. 收集历史消息用于启动同步
 * 4. 提供统一的会话名称映射（替代硬编码）
 * 5. ★ 新增：自动健康检查和故障恢复
 * 6. ★ 新增：重试次数限制和异常状态管理
 *
 * 会话状态(sessionStatus):
 * - 'initializing': 初始化中
 * - 'ready': 正常运行
 * - 'disconnected': 断开连接（可恢复）
 * - 'restarting': 重启中
 * - 'error': 异常（重试次数耗尽，需人工干预）
 *
 * 环境变量：
 * - HEALTH_CHECK_INTERVAL_MS: 健康检查间隔（默认 300000 = 5分钟）
 * - HEALTH_CHECK_ENABLED: 是否启用健康检查（默认 1）
 * - MAX_RESTART_RETRIES: 最大重启次数（默认 3）
 * - RESTART_COOLDOWN_MS: 重启冷却时间（默认 60000 = 1分钟）
 * - AUTO_RESTART_ENABLED: 是否启用自动重启（默认 1）
 */

const axios = require('axios');
const EventEmitter = require('events');

// 会话状态常量
const SESSION_STATUS = {
    INITIALIZING: 'initializing',
    READY: 'ready',
    DISCONNECTED: 'disconnected',
    RESTARTING: 'restarting',
    ERROR: 'error'
};

class SessionManager extends EventEmitter {
    constructor(options = {}) {
        super();

        // AdsPower 配置
        this.adsBase = options.adsBase || process.env.ADSPOWER_BASE || 'http://127.0.0.1:50325';
        this.adsKey = options.adsKey || process.env.ADSPOWER_API_KEY || '';

        // 会话存储：Map<sessionId, SessionInfo>
        this.sessions = new Map();

        // 历史同步配置
        this.syncContactCount = Number(process.env.SYNC_CONTACT_COUNT || 20);
        this.syncHistoryHours = Number(process.env.SYNC_HISTORY_HOURS || 12);
        this.syncOnStartup = String(process.env.SYNC_ON_STARTUP || '1') === '1';

        // ★ 健康检查配置（增强版）
        this.healthCheckInterval = Number(process.env.HEALTH_CHECK_INTERVAL_MS || 300000); // 默认5分钟
        this.healthCheckEnabled = String(process.env.HEALTH_CHECK_ENABLED || '1') === '1';

        // ★ 自动恢复配置（新增）
        this.maxRestartRetries = Number(process.env.MAX_RESTART_RETRIES || 3);
        this.restartCooldown = Number(process.env.RESTART_COOLDOWN_MS || 60000); // 默认1分钟
        this.autoRestartEnabled = String(process.env.AUTO_RESTART_ENABLED || '1') === '1';

        // 内部状态
        this._heartbeatTimer = null;
        this._initialized = false;
        this._profileCache = new Map(); // profileId -> profileInfo
        this._initSessionFunc = null; // 外部注入的初始化函数

        // 兼容旧版本的 heartbeatInterval
        this.heartbeatInterval = this.healthCheckInterval;

        // 静态后备映射（当 AdsPower 不可用时）
        this.fallbackNames = {};
        try {
            const fallbackEnv = process.env.FALLBACK_SESSION_NAMES || '';
            if (fallbackEnv) {
                // 格式: "k17se6o0:Briana,k17sdxph:Abby"
                fallbackEnv.split(',').forEach(pair => {
                    const [id, name] = pair.split(':').map(s => s.trim());
                    if (id && name) this.fallbackNames[id] = name;
                });
            }
        } catch (_) {}
    }

    /**
     * ★ 注入会话初始化函数（由 whatsapp-manager 调用）
     * @param {Function} initFunc - async function(config) 用于初始化会话
     */
    setInitSessionFunction(initFunc) {
        this._initSessionFunc = initFunc;
        console.log('[SessionManager] Session init function registered');
    }

    /**
     * 获取 AdsPower API headers
     */
    _getHeaders() {
        return this.adsKey ? { 'X-AdsPower-API-Key': this.adsKey } : {};
    }

    /**
     * 从 AdsPower 获取所有 profiles
     * @returns {Promise<Array<{user_id, name, group_name, ...}>>}
     */
    async fetchAllProfiles() {
        try {
            const headers = this._getHeaders();
            const params = { page_size: 100 };

            const { data } = await axios.get(`${this.adsBase}/api/v1/user/list`, {
                headers,
                params,
                timeout: 15000
            });

            if (data.code !== 0) {
                console.warn('[SessionManager] AdsPower API error:', data.msg || data);
                return [];
            }

            const profiles = data.data?.list || [];

            // 缓存 profile 信息
            profiles.forEach(p => {
                this._profileCache.set(p.user_id, {
                    user_id: p.user_id,
                    name: p.name || p.user_id,
                    group_name: p.group_name || '',
                    remark: p.remark || '',
                    created_time: p.created_time,
                    serial_number: p.serial_number
                });
            });

            console.log(`[SessionManager] Fetched ${profiles.length} profiles from AdsPower`);
            return profiles;

        } catch (e) {
            console.error('[SessionManager] Failed to fetch profiles:', e.message);
            return [];
        }
    }

    /**
     * 检查 AdsPower 浏览器状态
     * @param {string} user_id
     * @returns {Promise<{status: string, ws?: string}>}
     */
    async checkBrowserStatus(user_id) {
        try {
            const headers = this._getHeaders();
            const { data } = await axios.get(
                `${this.adsBase}/api/v1/browser/active?user_id=${encodeURIComponent(user_id)}`,
                { headers, timeout: 10000 }
            );

            if (data.code === 0 && data.data?.status === 'Active') {
                return { status: 'active', ws: data.data.ws?.puppeteer };
            }
            return { status: 'inactive' };
        } catch (e) {
            return { status: 'error', error: e.message };
        }
    }

    /**
     * 启动 AdsPower 浏览器
     * @param {string} user_id
     * @returns {Promise<string>} WebSocket endpoint
     */
    async startBrowser(user_id) {
        const headers = this._getHeaders();

        // 先停止旧实例
        try {
            await axios.get(
                `${this.adsBase}/api/v1/browser/stop?user_id=${encodeURIComponent(user_id)}`,
                { headers, timeout: 10000 }
            );
            await new Promise(r => setTimeout(r, 800));
        } catch (_) {}

        // 启动新实例
        const { data } = await axios.get(
            `${this.adsBase}/api/v1/browser/start?user_id=${encodeURIComponent(user_id)}`,
            { headers, timeout: 20000 }
        );

        if (data.code !== 0) {
            throw new Error(`AdsPower start failed: ${JSON.stringify(data)}`);
        }

        return data.data.ws.puppeteer;
    }

    /**
     * ★ 停止 AdsPower 浏览器
     * @param {string} user_id
     */
    async stopBrowser(user_id) {
        try {
            const headers = this._getHeaders();
            await axios.get(
                `${this.adsBase}/api/v1/browser/stop?user_id=${encodeURIComponent(user_id)}`,
                { headers, timeout: 10000 }
            );
            console.log(`[SessionManager] Browser stopped for ${user_id}`);
        } catch (e) {
            console.warn(`[SessionManager] Failed to stop browser ${user_id}:`, e.message);
        }
    }

    /**
     * 初始化 SessionManager
     * @param {string} sessionsEnv - 可选的 SESSIONS 环境变量值
     * @returns {Promise<Array<{sessionId, sessionName, profileInfo}>>}
     */
    async initialize(sessionsEnv = '') {
        console.log('[SessionManager] Initializing...');

        // 解析 SESSIONS 环境变量
        const requestedIds = (sessionsEnv || process.env.SESSIONS || '')
            .split(',')
            .map(s => s.trim())
            .filter(Boolean);

        // 获取所有 AdsPower profiles
        const allProfiles = await this.fetchAllProfiles();

        // 确定要管理的会话
        let targetProfiles;
        if (requestedIds.length > 0) {
            // 只管理指定的 profile
            targetProfiles = allProfiles.filter(p => requestedIds.includes(p.user_id));

            // 检查是否有未找到的 ID
            const foundIds = targetProfiles.map(p => p.user_id);
            const missing = requestedIds.filter(id => !foundIds.includes(id));
            if (missing.length > 0) {
                console.warn(`[SessionManager] Warning: profiles not found in AdsPower: ${missing.join(', ')}`);
            }
        } else {
            // 管理所有 profile
            targetProfiles = allProfiles;
        }

        console.log(`[SessionManager] Will manage ${targetProfiles.length} sessions`);

        // 构建配置数组
        const configs = targetProfiles.map(p => ({
            sessionId: p.user_id,
            sessionName: p.name || p.user_id,
            profileInfo: this._profileCache.get(p.user_id) || { user_id: p.user_id, name: p.user_id }
        }));

        this._initialized = true;
        return configs;
    }

    /**
     * 注册会话
     * @param {string} sessionId
     * @param {Object} client - whatsapp-web.js Client 实例
     * @param {Object} options
     */
    registerSession(sessionId, client, options = {}) {
        const profileInfo = this._profileCache.get(sessionId) || { user_id: sessionId, name: sessionId };

        // ★ 检查是否是重新注册（保留重试计数）
        const existingSession = this.sessions.get(sessionId);
        const restartCount = existingSession?.restartCount || 0;

        const sessionInfo = {
            sessionId,
            sessionName: profileInfo.name || sessionId,
            sessionStatus: SESSION_STATUS.INITIALIZING,
            client,
            chats: [],
            lastHeartbeat: Date.now(),
            pendingHistorySync: null,
            profileInfo,
            // ★ 新增：恢复相关字段
            restartCount,
            lastRestartTime: existingSession?.lastRestartTime || null,
            lastError: null,
            ...options
        };

        this.sessions.set(sessionId, sessionInfo);
        console.log(`[SessionManager] Registered session: ${sessionId} (${sessionInfo.sessionName})`);

        return sessionInfo;
    }

    /**
     * 更新会话状态
     * @param {string} sessionId
     * @param {string} status
     */
    updateStatus(sessionId, status) {
        const session = this.sessions.get(sessionId);
        if (session) {
            const previousStatus = session.sessionStatus;
            session.sessionStatus = status;
            session.lastHeartbeat = Date.now();

            // ★ 如果从异常状态恢复到正常，重置重试计数
            if (status === SESSION_STATUS.READY && previousStatus !== SESSION_STATUS.READY) {
                session.restartCount = 0;
                session.lastError = null;
                console.log(`[SessionManager] Session ${sessionId} recovered, reset restart count`);
            }

            this.emit('statusChange', { sessionId, status, previousStatus });
        }
    }

    /**
     * 更新会话的 chats 缓存
     * @param {string} sessionId
     * @param {Array} chats
     */
    updateChats(sessionId, chats) {
        const session = this.sessions.get(sessionId);
        if (session) {
            session.chats = chats;
        }
    }

    /**
     * 获取会话名称（用于 buildDisplayName）
     * @param {string} sessionId
     * @returns {string|null}
     */
    getSessionName(sessionId) {
        // 优先从 SessionManager 获取
        const session = this.sessions.get(sessionId);
        if (session?.sessionName) {
            return session.sessionName;
        }

        // 其次从 profile 缓存获取
        const profile = this._profileCache.get(sessionId);
        if (profile?.name) {
            return profile.name;
        }

        // 最后尝试后备映射
        if (this.fallbackNames[sessionId]) {
            return this.fallbackNames[sessionId];
        }

        return null;
    }

    /**
     * 获取会话
     * @param {string} sessionId
     * @returns {Object|null}
     */
    getSession(sessionId) {
        return this.sessions.get(sessionId) || null;
    }

    /**
     * 获取所有会话的状态摘要
     * @returns {Object}
     */
    getStatusSummary() {
        const summary = {};
        for (const [sessionId, session] of this.sessions) {
            summary[sessionId] = {
                name: session.sessionName,
                status: session.sessionStatus,
                chatsCached: (session.chats || []).length,
                lastHeartbeat: session.lastHeartbeat,
                via: 'adspower-ws',
                // ★ 新增恢复状态信息
                restartCount: session.restartCount || 0,
                maxRetries: this.maxRestartRetries,
                lastError: session.lastError || null
            };
        }
        return summary;
    }

    /**
     * 获取所有待同步的历史数据
     * @returns {Array}
     */
    getAllPendingHistory() {
        const pending = [];
        for (const [sessionId, session] of this.sessions) {
            if (session.pendingHistorySync) {
                pending.push(session.pendingHistorySync);
            }
        }
        return pending;
    }

    /**
     * 清除已同步的历史数据
     * @param {string} sessionId
     */
    clearPendingHistory(sessionId) {
        const session = this.sessions.get(sessionId);
        if (session) {
            session.pendingHistorySync = null;
        }
    }

    /**
     * ★ 重启会话
     * @param {string} sessionId
     * @returns {Promise<{success: boolean, error?: string}>}
     */
    async restartSession(sessionId) {
        const session = this.sessions.get(sessionId);
        if (!session) {
            return { success: false, error: 'Session not found' };
        }

        // 检查冷却时间
        const now = Date.now();
        if (session.lastRestartTime && (now - session.lastRestartTime) < this.restartCooldown) {
            const waitTime = Math.ceil((this.restartCooldown - (now - session.lastRestartTime)) / 1000);
            console.log(`[SessionManager] Restart cooldown for ${sessionId}, wait ${waitTime}s`);
            return { success: false, error: `Cooldown active, wait ${waitTime}s` };
        }

        // 检查重试次数
        if (session.restartCount >= this.maxRestartRetries) {
            console.error(`[SessionManager] Max restart retries (${this.maxRestartRetries}) reached for ${sessionId}`);
            session.sessionStatus = SESSION_STATUS.ERROR;
            session.lastError = `Max restart retries (${this.maxRestartRetries}) exceeded`;
            this.emit('sessionError', {
                sessionId,
                error: session.lastError,
                restartCount: session.restartCount
            });
            return { success: false, error: session.lastError };
        }

        console.log(`[SessionManager] Restarting session ${sessionId} (attempt ${session.restartCount + 1}/${this.maxRestartRetries})...`);

        // 更新状态
        session.sessionStatus = SESSION_STATUS.RESTARTING;
        session.restartCount++;
        session.lastRestartTime = now;
        this.emit('sessionRestarting', { sessionId, attempt: session.restartCount });

        try {
            // 1. 销毁旧的 client
            if (session.client) {
                try {
                    await session.client.destroy();
                    console.log(`[SessionManager] Old client destroyed for ${sessionId}`);
                } catch (e) {
                    console.warn(`[SessionManager] Error destroying client for ${sessionId}:`, e.message);
                }
            }

            // 2. 停止浏览器
            await this.stopBrowser(sessionId);

            // 3. 等待一段时间
            await new Promise(r => setTimeout(r, 2000));

            // 4. 使用注入的初始化函数重新初始化
            if (this._initSessionFunc) {
                const config = {
                    sessionId,
                    sessionName: session.sessionName
                };
                await this._initSessionFunc(config);
                console.log(`[SessionManager] Session ${sessionId} restart initiated`);
                return { success: true };
            } else {
                throw new Error('No init function registered');
            }

        } catch (e) {
            console.error(`[SessionManager] Restart failed for ${sessionId}:`, e.message);
            session.lastError = e.message;
            session.sessionStatus = SESSION_STATUS.DISCONNECTED;
            this.emit('restartFailed', { sessionId, error: e.message });
            return { success: false, error: e.message };
        }
    }

    /**
     * ★ 手动重置会话状态（用于从 error 状态恢复）
     * @param {string} sessionId
     */
    resetSession(sessionId) {
        const session = this.sessions.get(sessionId);
        if (session) {
            session.restartCount = 0;
            session.lastError = null;
            session.sessionStatus = SESSION_STATUS.DISCONNECTED;
            console.log(`[SessionManager] Session ${sessionId} reset, can restart now`);
            return true;
        }
        return false;
    }

    /**
     * 收集历史消息（用于启动同步）
     * @param {string} sessionId
     * @returns {Promise<Array>}
     */
    async collectHistoryForSync(sessionId) {
        const session = this.sessions.get(sessionId);
        if (!session?.client) {
            console.warn(`[SessionManager] Cannot collect history: session ${sessionId} not ready`);
            return [];
        }

        console.log(`[SessionManager] Collecting history for ${sessionId}...`);

        try {
            // 获取聊天列表
            const chats = await session.client.getChats();

            // 只取私聊，按未读和时间排序
            const privateChats = chats
                .filter(c => !c.isGroup)
                .sort((a, b) => {
                    // 优先未读
                    const unreadDiff = (b.unreadCount || 0) - (a.unreadCount || 0);
                    if (unreadDiff !== 0) return unreadDiff;
                    // 其次按时间
                    return (b.timestamp || 0) - (a.timestamp || 0);
                })
                .slice(0, this.syncContactCount);

            console.log(`[SessionManager] Selected ${privateChats.length} chats for history sync`);

            // 计算时间范围
            const nowSec = Math.floor(Date.now() / 1000);
            const cutoffSec = nowSec - (this.syncHistoryHours * 3600);

            // 收集消息
            const results = [];

            for (const chat of privateChats) {
                try {
                    const messages = await chat.fetchMessages({ limit: 50 });

                    // 过滤时间范围内的消息
                    const filtered = messages.filter(m =>
                        m.timestamp && m.timestamp >= cutoffSec
                    );

                    if (filtered.length > 0) {
                        results.push({
                            chatId: chat.id?._serialized,
                            phone: chat.id?.user,
                            name: chat.name || chat.pushname || '',
                            messages: filtered.map(m => ({
                                id: m.id?._serialized,
                                body: m.body,
                                type: m.type,
                                fromMe: m.fromMe,
                                timestamp: m.timestamp,
                                hasMedia: m.hasMedia
                            }))
                        });
                    }
                } catch (e) {
                    console.warn(`[SessionManager] Error fetching messages for chat:`, e.message);
                }
            }

            const totalMessages = results.reduce((sum, c) => sum + c.messages.length, 0);
            console.log(`[SessionManager] Collected ${totalMessages} messages from ${results.length} contacts`);

            return results;

        } catch (e) {
            console.error(`[SessionManager] Error collecting history:`, e.message);
            return [];
        }
    }

    /**
     * ★ 执行心跳检查（增强版：带自动恢复）
     */
    async performHeartbeatCheck() {
        if (!this.healthCheckEnabled) return [];

        const results = [];

        for (const [sessionId, session] of this.sessions) {
            // ★ 跳过已经处于 error 或 restarting 状态的会话
            if (session.sessionStatus === SESSION_STATUS.ERROR) {
                results.push({
                    sessionId,
                    sessionName: session.sessionName,
                    previousStatus: session.sessionStatus,
                    isHealthy: false,
                    skipped: true,
                    reason: 'Session in error state, manual intervention required'
                });
                continue;
            }

            if (session.sessionStatus === SESSION_STATUS.RESTARTING) {
                results.push({
                    sessionId,
                    sessionName: session.sessionName,
                    previousStatus: session.sessionStatus,
                    isHealthy: false,
                    skipped: true,
                    reason: 'Session is restarting'
                });
                continue;
            }

            try {
                const checkResult = {
                    sessionId,
                    sessionName: session.sessionName,
                    previousStatus: session.sessionStatus,
                    checks: {},
                    restartCount: session.restartCount || 0
                };

                // 检查 AdsPower 浏览器状态
                const browserStatus = await this.checkBrowserStatus(sessionId);
                checkResult.checks.browser = browserStatus.status;

                // 检查 WA 连接状态
                if (session.client) {
                    try {
                        const waState = await session.client.getState();
                        checkResult.checks.waState = waState;

                        if (waState === 'CONNECTED') {
                            session.lastHeartbeat = Date.now();

                            // 可选：检查消息同步
                            if (this.syncOnStartup) {
                                await this.checkMessageSync(sessionId, session);
                            }
                        }
                    } catch (e) {
                        checkResult.checks.waState = 'error';
                        checkResult.checks.waError = e.message;
                    }
                } else {
                    checkResult.checks.waState = 'no_client';
                }

                // 判断健康状态
                const isHealthy = browserStatus.status === 'active' &&
                    checkResult.checks.waState === 'CONNECTED';

                checkResult.isHealthy = isHealthy;

                // ★ 不健康时的处理
                if (!isHealthy && session.sessionStatus === SESSION_STATUS.READY) {
                    console.warn(`[SessionManager] Session ${sessionId} unhealthy:`, checkResult.checks);

                    // 更新状态为断开
                    session.sessionStatus = SESSION_STATUS.DISCONNECTED;
                    session.lastError = `Browser: ${browserStatus.status}, WA: ${checkResult.checks.waState}`;

                    this.emit('sessionUnhealthy', { sessionId, checks: checkResult.checks });

                    // ★ 自动重启
                    if (this.autoRestartEnabled) {
                        console.log(`[SessionManager] Auto-restarting session ${sessionId}...`);
                        checkResult.autoRestart = true;

                        // 异步执行重启，不阻塞心跳检查
                        this.restartSession(sessionId).then(result => {
                            if (!result.success) {
                                console.error(`[SessionManager] Auto-restart failed for ${sessionId}: ${result.error}`);
                            }
                        }).catch(e => {
                            console.error(`[SessionManager] Auto-restart error for ${sessionId}:`, e.message);
                        });
                    }
                }

                results.push(checkResult);

            } catch (e) {
                console.error(`[SessionManager] Heartbeat error for ${sessionId}:`, e.message);
                results.push({
                    sessionId,
                    sessionName: session.sessionName,
                    isHealthy: false,
                    error: e.message
                });
            }
        }

        return results;
    }

    /**
     * 检查消息同步状态（检测其他设备发送的消息）
     * @param {string} sessionId
     * @param {Object} session
     */
    async checkMessageSync(sessionId, session) {
        try {
            const client = session.client;
            if (!client) return;

            // 获取最近的 chats
            const chats = await client.getChats();
            const privateChats = chats.filter(c => !c.isGroup).slice(0, 10);

            // 获取每个 chat 的最新消息
            const currentSnapshot = new Map();

            for (const chat of privateChats) {
                try {
                    const messages = await chat.fetchMessages({ limit: 1 });
                    if (messages.length > 0) {
                        const msg = messages[0];
                        currentSnapshot.set(chat.id._serialized, {
                            msgId: msg.id?._serialized,
                            fromMe: msg.fromMe,
                            timestamp: msg.timestamp
                        });
                    }
                } catch (_) {}
            }

            // 与上次快照比较
            const lastSnapshot = session._lastContactSnapshot || new Map();
            const needsSync = [];

            for (const [chatId, current] of currentSnapshot) {
                const last = lastSnapshot.get(chatId);
                if (!last) continue;

                // 如果最新消息不同，且是我发的（可能来自其他设备）
                if (current.msgId !== last.msgId && current.fromMe) {
                    needsSync.push({
                        chatId,
                        msgId: current.msgId,
                        timestamp: current.timestamp
                    });
                }
            }

            // 保存当前快照
            session._lastContactSnapshot = currentSnapshot;

            // 如果有需要同步的消息
            if (needsSync.length > 0) {
                console.log(`[SessionManager] Detected ${needsSync.length} potential messages from other devices`);
                this.emit('messagesNeedSync', { sessionId, messages: needsSync });
            }

        } catch (e) {
            console.warn(`[SessionManager] checkMessageSync error:`, e.message);
        }
    }

    /**
     * 启动心跳定时器
     */
    startHeartbeat() {
        if (this._heartbeatTimer) {
            clearInterval(this._heartbeatTimer);
        }

        if (!this.healthCheckEnabled) {
            console.log('[SessionManager] Heartbeat disabled');
            return;
        }

        console.log(`[SessionManager] Starting heartbeat (interval: ${this.healthCheckInterval}ms, auto-restart: ${this.autoRestartEnabled})`);

        this._heartbeatTimer = setInterval(async () => {
            try {
                await this.performHeartbeatCheck();
            } catch (e) {
                console.error('[SessionManager] Heartbeat error:', e.message);
            }
        }, this.healthCheckInterval);
    }

    /**
     * 停止心跳定时器
     */
    stopHeartbeat() {
        if (this._heartbeatTimer) {
            clearInterval(this._heartbeatTimer);
            this._heartbeatTimer = null;
            console.log('[SessionManager] Heartbeat stopped');
        }
    }

    /**
     * 关闭 SessionManager
     */
    async shutdown() {
        this.stopHeartbeat();

        for (const [sessionId, session] of this.sessions) {
            try {
                if (session.client) {
                    await session.client.destroy().catch(() => {});
                }
            } catch (_) {}
        }

        this.sessions.clear();
        this._profileCache.clear();
        this._initialized = false;

        console.log('[SessionManager] Shutdown complete');
    }
}

// 导出状态常量
SessionManager.STATUS = SESSION_STATUS;

// 创建单例
const sessionManager = new SessionManager();

module.exports = {
    SessionManager,
    sessionManager,
    SESSION_STATUS
};