/**
 * SessionManager - 动态会话管理模块 (V2 修复版)
 *
 * V2 修复内容：
 * 1. 添加重连锁（_restartLocks）防止并发重连
 * 2. 增强 safeRestartSession 方法，完整的关闭-重启流程
 * 3. 添加全局错误处理建议（需在入口文件实现）
 * 4. 隔离各 session 的错误，防止互相影响
 *
 * 会话状态(sessionStatus):
 * - 'initializing': 初始化中
 * - 'ready': 正常运行
 * - 'disconnected': 断开连接（可恢复）
 * - 'restarting': 重启中
 * - 'error': 异常（重试次数耗尽，需人工干预）
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

        // ★★★ V2 新增：重连锁，防止同一 session 并发重连 ★★★
        this._restartLocks = new Map();  // sessionId -> { locked: boolean, timestamp: number }

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

        // ★★★ V2 新增：重连锁超时时间（防止死锁）★★★
        this.restartLockTimeout = Number(process.env.RESTART_LOCK_TIMEOUT_MS || 120000); // 2分钟

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
     * ★★★ V2 新增：获取重连锁 ★★★
     * @returns {boolean} 是否成功获取锁
     */
    _acquireRestartLock(sessionId) {
        const existing = this._restartLocks.get(sessionId);
        const now = Date.now();

        if (existing && existing.locked) {
            // 检查是否超时
            if (now - existing.timestamp < this.restartLockTimeout) {
                console.log(`[SessionManager] Restart lock active for ${sessionId}, skipping (age: ${Math.round((now - existing.timestamp) / 1000)}s)`);
                return false;
            }
            // 锁已超时，强制释放
            console.warn(`[SessionManager] Restart lock timeout for ${sessionId}, forcing release`);
        }

        this._restartLocks.set(sessionId, { locked: true, timestamp: now });
        console.log(`[SessionManager] Acquired restart lock for ${sessionId}`);
        return true;
    }

    /**
     * ★★★ V2 新增：释放重连锁 ★★★
     */
    _releaseRestartLock(sessionId) {
        this._restartLocks.set(sessionId, { locked: false, timestamp: Date.now() });
        console.log(`[SessionManager] Released restart lock for ${sessionId}`);
    }

    /**
     * ★★★ V2 新增：检查是否有活跃的重连锁 ★★★
     */
    hasActiveRestartLock(sessionId) {
        const lock = this._restartLocks.get(sessionId);
        if (!lock || !lock.locked) return false;
        // 检查是否超时
        return (Date.now() - lock.timestamp) < this.restartLockTimeout;
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

        // ★★★ V2 改进：先确保完全停止 ★★★
        try {
            await axios.get(
                `${this.adsBase}/api/v1/browser/stop?user_id=${encodeURIComponent(user_id)}`,
                { headers, timeout: 10000 }
            );
            await new Promise(r => setTimeout(r, 1500)); // 增加等待时间
        } catch (_) {}

        const { data } = await axios.get(
            `${this.adsBase}/api/v1/browser/start?user_id=${encodeURIComponent(user_id)}`,
            { headers, timeout: 30000 }  // 增加超时时间
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
                lastError: session.lastError || null,
                hasRestartLock: this.hasActiveRestartLock(sessionId)  // ★ V2 新增
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
     * ★★★ V2 核心方法：安全重启会话 ★★★
     *
     * 这是所有重启操作的统一入口，包含：
     * 1. 重连锁检查
     * 2. 完整的关闭-重启流程
     * 3. 错误隔离
     *
     * @param {string} sessionId - 会话ID
     * @param {string} reason - 重启原因（用于日志）
     * @returns {Promise<{success: boolean, error?: string}>}
     */
    async safeRestartSession(sessionId, reason = 'unknown') {
        const session = this.sessions.get(sessionId);
        if (!session) {
            return { success: false, error: 'Session not found' };
        }

        // 1. 尝试获取重连锁
        if (!this._acquireRestartLock(sessionId)) {
            return { success: false, error: 'Restart already in progress' };
        }

        console.log(`[SessionManager] ========== Safe restart ${sessionId} (reason: ${reason}) ==========`);

        try {
            // 2. 检查重启次数
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

            // 3. 更新状态
            session.sessionStatus = SESSION_STATUS.RESTARTING;
            session.restartCount++;
            session.lastRestartTime = Date.now();
            this.emit('sessionRestarting', { sessionId, attempt: session.restartCount, reason });

            console.log(`[SessionManager] Restart attempt ${session.restartCount}/${this.maxRestartRetries} for ${sessionId}`);

            // 4. ★★★ 关键步骤：安全销毁旧 client ★★★
            if (session.client) {
                try {
                    console.log(`[SessionManager] Destroying old client for ${sessionId}...`);
                    await Promise.race([
                        session.client.destroy(),
                        new Promise((_, reject) => setTimeout(() => reject(new Error('destroy timeout')), 10000))
                    ]);
                    console.log(`[SessionManager] Old client destroyed for ${sessionId}`);
                } catch (e) {
                    console.warn(`[SessionManager] Error destroying client for ${sessionId}: ${e.message}`);
                    // 继续执行，不要因为销毁失败就停止
                }
                session.client = null;
            }

            // 5. ★★★ 关键步骤：确保 AdsPower 浏览器完全关闭 ★★★
            console.log(`[SessionManager] Stopping AdsPower browser for ${sessionId}...`);
            await this.stopBrowser(sessionId);
            await new Promise(r => setTimeout(r, 2000)); // 等待浏览器完全关闭

            // 6. 再次确认浏览器状态
            const statusAfterStop = await this.checkBrowserStatus(sessionId);
            if (statusAfterStop.status === 'active') {
                console.warn(`[SessionManager] Browser still active after stop, retrying...`);
                await this.stopBrowser(sessionId);
                await new Promise(r => setTimeout(r, 3000)); // 再等一下
            }

            // 7. ★★★ 关键步骤：调用初始化函数重新启动 ★★★
            if (this._initSessionFunc) {
                const config = {
                    sessionId,
                    sessionName: session.sessionName
                };
                console.log(`[SessionManager] Calling init function for ${sessionId}...`);
                await this._initSessionFunc(config);
                console.log(`[SessionManager] Session ${sessionId} restart initiated successfully`);
                return { success: true };
            } else {
                throw new Error('No init function registered');
            }

        } catch (e) {
            console.error(`[SessionManager] Safe restart failed for ${sessionId}: ${e.message}`);
            session.lastError = e.message;
            session.sessionStatus = SESSION_STATUS.DISCONNECTED;
            this.emit('restartFailed', { sessionId, error: e.message });
            return { success: false, error: e.message };

        } finally {
            // 8. ★★★ 无论成功失败，都要释放锁 ★★★
            this._releaseRestartLock(sessionId);
        }
    }

    /**
     * 手动重置会话状态（用于错误恢复）
     */
    resetSession(sessionId) {
        const session = this.sessions.get(sessionId);
        if (session) {
            session.restartCount = 0;
            session.lastError = null;
            session.sessionStatus = SESSION_STATUS.DISCONNECTED;
            // 同时释放可能存在的锁
            this._releaseRestartLock(sessionId);
            console.log(`[SessionManager] Session ${sessionId} reset, can restart now`);
            return true;
        }
        return false;
    }

    /**
     * ★★★ V2 保留：强制重启会话（内部方法，供心跳检测使用）★★★
     * 现在改为调用 safeRestartSession
     */
    async _forceRestartSession(sessionId) {
        return this.safeRestartSession(sessionId, 'heartbeat_unhealthy');
    }

    /**
     * ★ 增强版：收集历史消息
     */
    async collectHistoryForStartupSync(sessionId) {
        const session = this.sessions.get(sessionId);
        if (!session?.client) {
            console.warn(`[SessionManager] Cannot collect history: session ${sessionId} not ready`);
            return [];
        }

        console.log(`[SessionManager] Collecting history for ${sessionId}...`);

        try {
            const chats = await session.client.getChats();
            const privateChats = chats
                .filter(c => !c.isGroup && !/@g\.us$/i.test(c.id?._serialized || ''))
                .sort((a, b) => {
                    const aTime = a.timestamp || a.lastMessage?.timestamp || 0;
                    const bTime = b.timestamp || b.lastMessage?.timestamp || 0;
                    return bTime - aTime;
                })
                .slice(0, this.syncContactCount);

            console.log(`[SessionManager] Selected ${privateChats.length} chats for history sync`);

            const results = [];
            let totalMessages = 0;
            let totalContacts = 0;

            for (const chat of privateChats) {
                try {
                    const chatId = chat.id?._serialized || '';
                    const recentMsgs = await chat.fetchMessages({ limit: 1 });
                    if (!recentMsgs || recentMsgs.length === 0) continue;

                    const lastMsg = recentMsgs[0];
                    const lastMsgTime = lastMsg.timestamp || 0;
                    const cutoffTime = lastMsgTime - (this.syncHistoryHours * 3600);

                    const allMsgs = await chat.fetchMessages({ limit: 100 });
                    const relevantMsgs = allMsgs.filter(m => (m.timestamp || 0) >= cutoffTime);

                    if (relevantMsgs.length > 0) {
                        results.push({
                            chatId,
                            name: chat.name || '',
                            messages: relevantMsgs.map(m => ({
                                id: m.id?._serialized,
                                body: m.body,
                                fromMe: m.fromMe,
                                timestamp: m.timestamp,
                                type: m.type
                            }))
                        });
                        totalMessages += relevantMsgs.length;
                        totalContacts++;
                    }
                } catch (e) {
                    // 单个聊天失败不影响其他
                }
            }

            console.log(`[SessionManager] Collected ${totalMessages} messages from ${totalContacts} contacts`);
            return results;

        } catch (e) {
            console.error(`[SessionManager] collectHistory error:`, e.message);
            return [];
        }
    }

    /**
     * 执行心跳检查
     */
    async performHeartbeatCheck() {
        const results = [];

        for (const [sessionId, session] of this.sessions) {
            try {
                // 跳过正在重启或已出错的会话
                if (session.sessionStatus === SESSION_STATUS.RESTARTING) {
                    continue;
                }
                if (session.sessionStatus === SESSION_STATUS.ERROR) {
                    continue;
                }

                // ★★★ V2 新增：跳过有活跃重连锁的会话 ★★★
                if (this.hasActiveRestartLock(sessionId)) {
                    console.log(`[SessionManager] Session ${sessionId} has active restart lock, skipping heartbeat`);
                    continue;
                }

                const checkResult = {
                    sessionId,
                    sessionName: session.sessionName,
                    isHealthy: true,
                    checks: {}
                };

                // 检查 AdsPower 浏览器状态
                const browserStatus = await this.checkBrowserStatus(sessionId);
                checkResult.checks.browser = browserStatus.status;

                // 检查 WhatsApp 客户端状态
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

                // 健康判断
                const waState = checkResult.checks.waState;
                // ★★★ V3 修复：null 状态视为"正在连接"，给它机会 ★★★
                const isConnectedOrOpening = waState === 'CONNECTED' || waState === 'OPENING' || waState === null;
                const isHealthy = waState === 'CONNECTED';

                // ★★★ V3 修复：null 状态计数器，连续多次才判定为 unhealthy ★★★
                if (waState === null) {
                    session._nullStateCount = (session._nullStateCount || 0) + 1;
                    if (session._nullStateCount <= 5) {  // 给 5 次机会（约 15 秒）
                        console.log(`[SessionManager] Session ${sessionId}: waState is null (${session._nullStateCount}/5), waiting...`);
                        results.push({ ...checkResult, isHealthy: false, waiting: true });
                        continue;  // 跳过这次检查
                    }
                } else if (waState === 'CONNECTED') {
                    session._nullStateCount = 0;  // 重置计数器
                }

                // ★★★ V3 修复：忽略 AdsPower 的 inactive 误报 ★★★
                if (browserStatus.status !== 'active' && isConnectedOrOpening) {
                    if (!session._lastBrowserInactiveLog || Date.now() - session._lastBrowserInactiveLog > 60000) {
                        console.log(`[SessionManager] Session ${sessionId}: AdsPower reports browser inactive, but WA is ${waState || 'loading'} - ignoring`);
                        session._lastBrowserInactiveLog = Date.now();
                    }
                    // 如果 WA 是 CONNECTED，直接视为健康
                    if (waState === 'CONNECTED') {
                        checkResult.isHealthy = true;
                        results.push(checkResult);
                        continue;
                    }
                }

                checkResult.isHealthy = isHealthy;

                // 不健康时的处理
                if (!isHealthy) {
                    // ★★★ V3 修复：延长宽限期到 60 秒 ★★★
                    const GRACE_PERIOD_MS = 60000;
                    const timeSinceReady = session.readyAt ? (Date.now() - session.readyAt) : Infinity;
                    const inGracePeriod = timeSinceReady < GRACE_PERIOD_MS;

                    if (isConnectedOrOpening && browserStatus.status === 'active') {
                        if (waState === 'OPENING') {
                            console.log(`[SessionManager] Session ${sessionId} is OPENING, waiting for connection...`);
                        }
                    } else if (inGracePeriod) {
                        console.log(`[SessionManager] Session ${sessionId} in grace period (${Math.round(timeSinceReady/1000)}s/${GRACE_PERIOD_MS/1000}s), skipping restart`);
                    } else if (session.sessionStatus === SESSION_STATUS.READY) {
                        // ★★★ V3 修复：只有真正的问题状态才重启 ★★★
                        const reallyUnhealthy = waState === 'error' || waState === 'CONFLICT' ||
                            waState === 'UNPAIRED' || waState === 'UNLAUNCHED' ||
                            (session._nullStateCount && session._nullStateCount > 5);

                        if (!reallyUnhealthy) {
                            console.log(`[SessionManager] Session ${sessionId}: waState=${waState}, not critical - skipping restart`);
                        } else {
                            console.warn(`[SessionManager] Session ${sessionId} unhealthy:`, checkResult.checks);

                            session.sessionStatus = SESSION_STATUS.DISCONNECTED;
                            session.lastError = `Browser: ${browserStatus.status}, WA: ${checkResult.checks.waState}`;

                            this.emit('sessionUnhealthy', { sessionId, checks: checkResult.checks });

                            if (this.autoRestartEnabled) {
                                console.log(`[SessionManager] Auto-restarting session ${sessionId}...`);
                                checkResult.autoRestart = true;

                                // ★★★ V2 改进：使用 safeRestartSession ★★★
                                this.safeRestartSession(sessionId, 'heartbeat_unhealthy').then(result => {
                                    if (!result.success) {
                                        console.error(`[SessionManager] Auto-restart failed for ${sessionId}: ${result.error}`);
                                    }
                                }).catch(e => {
                                    console.error(`[SessionManager] Auto-restart error for ${sessionId}:`, e.message);
                                });
                            }
                        }
                    } else if (session.sessionStatus === SESSION_STATUS.DISCONNECTED) {
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
        this._restartLocks.clear();  // ★ V2 新增
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