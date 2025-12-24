/**
 * SessionManager - 动态会话管理模块 (增强版 + 启动同步)
 *
 * 功能：
 * 1. 动态从 AdsPower API 获取 profile 列表和名称
 * 2. 管理会话生命周期（注册、状态追踪、心跳检测）
 * 3. 收集历史消息用于启动同步 ★增强版★
 * 4. 提供统一的会话名称映射（替代硬编码）
 * 5. 自动健康检查和故障恢复
 * 6. 重试次数限制和异常状态管理
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
 * - SYNC_ON_STARTUP: 是否启用启动同步（默认 1）
 * - SYNC_CONTACT_COUNT: 同步的联系人数量（默认 20）
 * - SYNC_HISTORY_HOURS: 同步的历史小时数（默认 12）
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

        // ★ 历史同步配置（增强）
        this.syncContactCount = Number(process.env.SYNC_CONTACT_COUNT || 20);
        this.syncHistoryHours = Number(process.env.SYNC_HISTORY_HOURS || 12);
        this.syncOnStartup = String(process.env.SYNC_ON_STARTUP || '1') === '1';

        // 健康检查配置
        this.healthCheckInterval = Number(process.env.HEALTH_CHECK_INTERVAL_MS || 300000);
        this.healthCheckEnabled = String(process.env.HEALTH_CHECK_ENABLED || '1') === '1';

        // 自动恢复配置
        this.maxRestartRetries = Number(process.env.MAX_RESTART_RETRIES || 3);
        this.restartCooldown = Number(process.env.RESTART_COOLDOWN_MS || 60000);
        this.autoRestartEnabled = String(process.env.AUTO_RESTART_ENABLED || '1') === '1';

        // 内部状态
        this._heartbeatTimer = null;
        this._initialized = false;
        this._profileCache = new Map();
        this._initSessionFunc = null;

        // ★ 启动同步状态
        this._startupSyncComplete = false;
        this._startupSyncResults = null;

        // 兼容旧版本
        this.heartbeatInterval = this.healthCheckInterval;

        // 静态后备映射
        this.fallbackNames = {};
        try {
            const fallbackEnv = process.env.FALLBACK_SESSION_NAMES || '';
            if (fallbackEnv) {
                fallbackEnv.split(',').forEach(pair => {
                    const [id, name] = pair.split(':').map(s => s.trim());
                    if (id && name) this.fallbackNames[id] = name;
                });
            }
        } catch (_) {}
    }

    /**
     * 注入会话初始化函数
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
     */
    async startBrowser(user_id) {
        const headers = this._getHeaders();

        try {
            await axios.get(
                `${this.adsBase}/api/v1/browser/stop?user_id=${encodeURIComponent(user_id)}`,
                { headers, timeout: 10000 }
            );
            await new Promise(r => setTimeout(r, 800));
        } catch (_) {}

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
     * 停止 AdsPower 浏览器
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
     */
    async initialize(sessionsEnv = '') {
        console.log('[SessionManager] Initializing...');

        const requestedIds = (sessionsEnv || process.env.SESSIONS || '')
            .split(',')
            .map(s => s.trim())
            .filter(Boolean);

        const allProfiles = await this.fetchAllProfiles();

        let targetProfiles;
        if (requestedIds.length > 0) {
            targetProfiles = allProfiles.filter(p => requestedIds.includes(p.user_id));
            const foundIds = targetProfiles.map(p => p.user_id);
            const missing = requestedIds.filter(id => !foundIds.includes(id));
            if (missing.length > 0) {
                console.warn(`[SessionManager] Warning: profiles not found in AdsPower: ${missing.join(', ')}`);
            }
        } else {
            targetProfiles = allProfiles;
        }

        console.log(`[SessionManager] Will manage ${targetProfiles.length} sessions`);

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
     */
    registerSession(sessionId, client, options = {}) {
        const profileInfo = this._profileCache.get(sessionId) || { user_id: sessionId, name: sessionId };
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
     */
    updateStatus(sessionId, status) {
        const session = this.sessions.get(sessionId);
        if (session) {
            const previousStatus = session.sessionStatus;
            session.sessionStatus = status;
            session.lastHeartbeat = Date.now();

            if (status === SESSION_STATUS.READY && previousStatus !== SESSION_STATUS.READY) {
                session.restartCount = 0;
                session.lastError = null;
                session.readyAt = Date.now();  // ★ 记录变为 READY 的时间，用于宽限期判断
                console.log(`[SessionManager] Session ${sessionId} recovered, reset restart count`);
            }

            this.emit('statusChange', { sessionId, status, previousStatus });
        }
    }

    /**
     * 更新会话的 chats 缓存
     */
    updateChats(sessionId, chats) {
        const session = this.sessions.get(sessionId);
        if (session) {
            session.chats = chats;
        }
    }

    /**
     * 获取会话名称
     */
    getSessionName(sessionId) {
        const session = this.sessions.get(sessionId);
        if (session?.sessionName) {
            return session.sessionName;
        }
        const profile = this._profileCache.get(sessionId);
        if (profile?.name) {
            return profile.name;
        }
        if (this.fallbackNames[sessionId]) {
            return this.fallbackNames[sessionId];
        }
        return null;
    }

    /**
     * 获取会话
     */
    getSession(sessionId) {
        return this.sessions.get(sessionId) || null;
    }

    /**
     * 获取所有会话（包含 client 对象）
     * 用于启动同步等需要访问所有会话的场景
     */
    getAllSessions() {
        const result = {};
        for (const [sessionId, session] of this.sessions) {
            result[sessionId] = session;
        }
        return result;
    }

    /**
     * 获取所有会话的状态摘要
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
                restartCount: session.restartCount || 0,
                maxRetries: this.maxRestartRetries,
                lastError: session.lastError || null
            };
        }
        return summary;
    }

    /**
     * 获取所有待同步的历史数据
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
     */
    clearPendingHistory(sessionId) {
        const session = this.sessions.get(sessionId);
        if (session) {
            session.pendingHistorySync = null;
        }
    }

    /**
     * 重启会话
     */
    async restartSession(sessionId) {
        const session = this.sessions.get(sessionId);
        if (!session) {
            return { success: false, error: 'Session not found' };
        }

        const now = Date.now();
        if (session.lastRestartTime && (now - session.lastRestartTime) < this.restartCooldown) {
            const waitTime = Math.ceil((this.restartCooldown - (now - session.lastRestartTime)) / 1000);
            console.log(`[SessionManager] Restart cooldown for ${sessionId}, wait ${waitTime}s`);
            return { success: false, error: `Cooldown active, wait ${waitTime}s` };
        }

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

        session.sessionStatus = SESSION_STATUS.RESTARTING;
        session.restartCount++;
        session.lastRestartTime = now;
        this.emit('sessionRestarting', { sessionId, attempt: session.restartCount });

        try {
            if (session.client) {
                try {
                    await session.client.destroy();
                    console.log(`[SessionManager] Old client destroyed for ${sessionId}`);
                } catch (e) {
                    console.warn(`[SessionManager] Error destroying client for ${sessionId}:`, e.message);
                }
            }

            await this.stopBrowser(sessionId);
            await new Promise(r => setTimeout(r, 2000));

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
     * 手动重置会话状态
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
     * ★★★ 强制重启会话（确保先关闭浏览器）★★★
     *
     * 与 restartSession 的区别：
     * 1. 不检查冷却时间（由心跳触发时已经是异常状态）
     * 2. 确保浏览器完全关闭后再重启
     * 3. 用于心跳检测到异常时的自动恢复
     */
    async _forceRestartSession(sessionId) {
        const session = this.sessions.get(sessionId);
        if (!session) {
            return { success: false, error: 'Session not found' };
        }

        // 检查重启次数限制
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

        console.log(`[SessionManager] Force restarting session ${sessionId} (attempt ${session.restartCount + 1}/${this.maxRestartRetries})...`);

        session.sessionStatus = SESSION_STATUS.RESTARTING;
        session.restartCount++;
        session.lastRestartTime = Date.now();
        this.emit('sessionRestarting', { sessionId, attempt: session.restartCount });

        try {
            // 1. 销毁旧 client
            if (session.client) {
                try {
                    await session.client.destroy();
                    console.log(`[SessionManager] Old client destroyed for ${sessionId}`);
                } catch (e) {
                    console.warn(`[SessionManager] Error destroying client for ${sessionId}:`, e.message);
                }
                session.client = null;
            }

            // 2. ★★★ 关键：检查浏览器状态并强制关闭 ★★★
            const browserStatus = await this.checkBrowserStatus(sessionId);
            console.log(`[SessionManager] Browser status for ${sessionId}:`, browserStatus.status);

            if (browserStatus.status === 'active') {
                console.log(`[SessionManager] Browser still active, forcing stop for ${sessionId}...`);
                await this.stopBrowser(sessionId);
                await new Promise(r => setTimeout(r, 2000)); // 等待浏览器完全关闭
            } else {
                console.log(`[SessionManager] Browser already inactive for ${sessionId}`);
            }

            // 3. 再次确认浏览器已关闭
            const afterStopStatus = await this.checkBrowserStatus(sessionId);
            if (afterStopStatus.status === 'active') {
                console.warn(`[SessionManager] Browser still active after stop attempt, retrying...`);
                await this.stopBrowser(sessionId);
                await new Promise(r => setTimeout(r, 3000));
            }

            // 4. 调用初始化函数重新启动
            if (this._initSessionFunc) {
                const config = {
                    sessionId,
                    sessionName: session.sessionName
                };
                console.log(`[SessionManager] Initializing session ${sessionId} with config:`, config);
                await this._initSessionFunc(config);
                console.log(`[SessionManager] Session ${sessionId} force restart initiated`);
                return { success: true };
            } else {
                throw new Error('No init function registered');
            }

        } catch (e) {
            console.error(`[SessionManager] Force restart failed for ${sessionId}:`, e.message);
            session.lastError = e.message;
            session.sessionStatus = SESSION_STATUS.DISCONNECTED;
            this.emit('restartFailed', { sessionId, error: e.message });
            return { success: false, error: e.message };
        }
    }

    /**
     * ★ 增强版：收集历史消息（基于最后一条消息时间往前推）
     *
     * @param {string} sessionId - 会话ID
     * @returns {Promise<Array>} 联系人及消息数据
     */
    async collectHistoryForStartupSync(sessionId) {
        const session = this.sessions.get(sessionId);
        if (!session?.client) {
            console.warn(`[SessionManager] Cannot collect history: session ${sessionId} not ready`);
            return [];
        }

        console.log(`[SessionManager] ★ Collecting startup sync history for ${sessionId}...`);
        console.log(`[SessionManager] Config: contacts=${this.syncContactCount}, hours=${this.syncHistoryHours}`);

        try {
            const chats = await session.client.getChats();

            // 只取私聊，排除群组，按时间排序
            const privateChats = chats
                .filter(c => !c.isGroup && !/@g\.us$/i.test(c.id?._serialized || ''))
                .sort((a, b) => {
                    const aTime = a.timestamp || a.lastMessage?.timestamp || 0;
                    const bTime = b.timestamp || b.lastMessage?.timestamp || 0;
                    return bTime - aTime;
                })
                .slice(0, this.syncContactCount);

            console.log(`[SessionManager] Selected ${privateChats.length} private chats for sync`);

            const results = [];

            for (const chat of privateChats) {
                try {
                    const chatId = chat.id?._serialized || '';

                    // ★ 关键：先获取最新消息，确定时间范围
                    const recentMsgs = await chat.fetchMessages({ limit: 1 });
                    if (!recentMsgs || recentMsgs.length === 0) {
                        continue;
                    }

                    // 最后一条消息的时间戳
                    const lastMsgTimestamp = recentMsgs[0].timestamp;
                    if (!lastMsgTimestamp) {
                        continue;
                    }

                    // ★ 计算时间范围：从最后一条消息往前推 N 小时
                    const cutoffTimestamp = lastMsgTimestamp - (this.syncHistoryHours * 3600);

                    console.log(`[SessionManager] Chat ${chatId}: range ${new Date(cutoffTimestamp * 1000).toISOString()} ~ ${new Date(lastMsgTimestamp * 1000).toISOString()}`);

                    // 获取更多消息
                    const allMessages = await chat.fetchMessages({ limit: 200 });

                    // 过滤在时间范围内的消息
                    const filtered = allMessages.filter(m => {
                        const ts = m.timestamp;
                        return ts && ts >= cutoffTimestamp && ts <= lastMsgTimestamp;
                    });

                    if (filtered.length === 0) {
                        continue;
                    }

                    // 获取联系人信息
                    const contact = await chat.getContact().catch(() => null);
                    const contactName = contact?.pushname || contact?.name || chat.name || '';

                    // 提取电话号码
                    let phone = '';
                    let phone_lid = '';
                    if (/@c\.us$/i.test(chatId)) {
                        phone = chatId.replace(/@.*/, '').replace(/\D/g, '');
                    } else if (/@lid$/i.test(chatId)) {
                        phone_lid = chatId.replace(/@.*/, '').replace(/\D/g, '');
                    }

                    // 转换消息格式
                    const messages = filtered.map(m => ({
                        id: m.id?._serialized || m.id,
                        body: m.body || '',
                        type: m.type || 'chat',
                        fromMe: !!m.fromMe,
                        timestamp: m.timestamp,
                        hasMedia: !!m.hasMedia
                    }));

                    // 按时间升序排列
                    messages.sort((a, b) => a.timestamp - b.timestamp);

                    results.push({
                        chatId,
                        phone,
                        phone_lid,
                        name: contactName,
                        sessionId,
                        sessionName: session.sessionName,
                        messages,
                        lastMsgTimestamp,
                        cutoffTimestamp,
                        messageCount: messages.length
                    });

                    console.log(`[SessionManager] Chat ${phone || phone_lid || chatId}: ${messages.length} messages`);

                } catch (chatErr) {
                    console.warn(`[SessionManager] Error processing chat:`, chatErr.message);
                }
            }

            const totalMessages = results.reduce((sum, c) => sum + c.messageCount, 0);
            console.log(`[SessionManager] ★ Collected ${totalMessages} messages from ${results.length} contacts`);

            return results;

        } catch (e) {
            console.error(`[SessionManager] collectHistoryForStartupSync error:`, e.message);
            return [];
        }
    }

    /**
     * 旧版兼容：收集历史消息（基于当前时间往前推）
     */
    async collectHistoryForSync(sessionId) {
        const session = this.sessions.get(sessionId);
        if (!session?.client) {
            console.warn(`[SessionManager] Cannot collect history: session ${sessionId} not ready`);
            return [];
        }

        console.log(`[SessionManager] Collecting history for ${sessionId}...`);

        try {
            const chats = await session.client.getChats();
            const privateChats = chats
                .filter(c => !c.isGroup)
                .sort((a, b) => {
                    const unreadDiff = (b.unreadCount || 0) - (a.unreadCount || 0);
                    if (unreadDiff !== 0) return unreadDiff;
                    return (b.timestamp || 0) - (a.timestamp || 0);
                })
                .slice(0, this.syncContactCount);

            console.log(`[SessionManager] Selected ${privateChats.length} chats for history sync`);

            const nowSec = Math.floor(Date.now() / 1000);
            const cutoffSec = nowSec - (this.syncHistoryHours * 3600);

            const results = [];

            for (const chat of privateChats) {
                try {
                    const messages = await chat.fetchMessages({ limit: 50 });
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
     * 执行心跳检查
     */
    async performHeartbeatCheck() {
        if (!this.healthCheckEnabled) return [];

        const results = [];

        for (const [sessionId, session] of this.sessions) {
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

                const browserStatus = await this.checkBrowserStatus(sessionId);
                checkResult.checks.browser = browserStatus.status;

                if (session.client) {
                    try {
                        const waState = await session.client.getState();
                        checkResult.checks.waState = waState;

                        if (waState === 'CONNECTED') {
                            session.lastHeartbeat = Date.now();

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

                // ★★★ 修复：OPENING 状态视为正在连接，不触发重启 ★★★
                const waState = checkResult.checks.waState;
                const isConnectedOrOpening = waState === 'CONNECTED' || waState === 'OPENING';
                const isHealthy = browserStatus.status === 'active' && waState === 'CONNECTED';

                checkResult.isHealthy = isHealthy;

                // ★★★ 增强：检测到不健康时的处理逻辑 ★★★
                if (!isHealthy) {
                    // ★★★ 新增：宽限期检查 - READY 后 30 秒内不触发重启 ★★★
                    const GRACE_PERIOD_MS = 30000;
                    const timeSinceReady = session.readyAt ? (Date.now() - session.readyAt) : Infinity;
                    const inGracePeriod = timeSinceReady < GRACE_PERIOD_MS;

                    // ★★★ 新增：OPENING 状态不触发重启，等待连接完成 ★★★
                    if (isConnectedOrOpening && browserStatus.status === 'active') {
                        // OPENING 状态 - 正在连接中，不触发重启
                        if (waState === 'OPENING') {
                            console.log(`[SessionManager] Session ${sessionId} is OPENING, waiting for connection...`);
                        }
                        // 不做任何重启操作
                    }
                    // 宽限期内不触发重启
                    else if (inGracePeriod) {
                        console.log(`[SessionManager] Session ${sessionId} in grace period (${Math.round(timeSinceReady/1000)}s/${GRACE_PERIOD_MS/1000}s), skipping restart`);
                    }
                    // 只有在 READY 状态下且过了宽限期才触发重启
                    else if (session.sessionStatus === SESSION_STATUS.READY) {
                        console.warn(`[SessionManager] Session ${sessionId} unhealthy:`, checkResult.checks);

                        session.sessionStatus = SESSION_STATUS.DISCONNECTED;
                        session.lastError = `Browser: ${browserStatus.status}, WA: ${checkResult.checks.waState}`;

                        this.emit('sessionUnhealthy', { sessionId, checks: checkResult.checks });

                        if (this.autoRestartEnabled) {
                            console.log(`[SessionManager] Auto-restarting session ${sessionId}...`);
                            checkResult.autoRestart = true;

                            // ★★★ 关键：先确保浏览器关闭，再重启 ★★★
                            this._forceRestartSession(sessionId).then(result => {
                                if (!result.success) {
                                    console.error(`[SessionManager] Auto-restart failed for ${sessionId}: ${result.error}`);
                                }
                            }).catch(e => {
                                console.error(`[SessionManager] Auto-restart error for ${sessionId}:`, e.message);
                            });
                        }
                    }
                    // 如果已经是 DISCONNECTED 或 RESTARTING 状态，不再重复触发
                    else if (session.sessionStatus === SESSION_STATUS.DISCONNECTED) {
                        console.log(`[SessionManager] Session ${sessionId} still disconnected, waiting for restart...`);
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
     * 检查消息同步状态
     */
    async checkMessageSync(sessionId, session) {
        try {
            const client = session.client;
            if (!client) return;

            const chats = await client.getChats();
            const privateChats = chats.filter(c => !c.isGroup).slice(0, 10);

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

            const lastSnapshot = session._lastContactSnapshot || new Map();
            const needsSync = [];

            for (const [chatId, current] of currentSnapshot) {
                const last = lastSnapshot.get(chatId);
                if (!last) continue;

                if (current.msgId !== last.msgId && current.fromMe) {
                    needsSync.push({
                        chatId,
                        msgId: current.msgId,
                        timestamp: current.timestamp
                    });
                }
            }

            session._lastContactSnapshot = currentSnapshot;

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