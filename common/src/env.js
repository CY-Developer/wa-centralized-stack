// 引入dotenv来加载.env文件
require('dotenv').config();

// 确保所有环境变量的默认值，读取并配置
module.exports = {
    PROTOCOL_TIMEOUT_MS: process.env.PROTOCOL_TIMEOUT_MS || 120000, // 设置CDP协议超时
    MEDIA_DIR: process.env.MEDIA_DIR || './bridge/media', // 默认media目录
    AUTO_CLICK_USE_HERE: process.env.AUTO_CLICK_USE_HERE === '1', // 自动点击“Use here”弹窗
    CDP_CLOSE_DUP_TABS: process.env.CDP_CLOSE_DUP_TABS === '1', // 自动关多余的WA标签页
    RATE_PER_MIN: process.env.RATE_PER_MIN || 12, // 每分钟最大发送条数
    SAME_CHAT_COOLDOWN_MS: process.env.SAME_CHAT_COOLDOWN_MS || 6000, // 同会话冷却时间
    HUMAN_TYPING_MIN_MS: process.env.HUMAN_TYPING_MIN_MS || 400, // 输入最小时间（模拟）
    HUMAN_TYPING_MAX_MS: process.env.HUMAN_TYPING_MAX_MS || 1200, // 输入最大时间（模拟）
    SCAN_INTERVAL_MS: process.env.SCAN_INTERVAL_MS || 5000, // 扫描周期
    MAX_UNREAD_PER_SWEEP: process.env.MAX_UNREAD_PER_SWEEP || 3, // 每次扫描的最大未读数
    CAPTURE_LAST_N: process.env.CAPTURE_LAST_N || 30, // 每次会话抓取的最大消息数
    DIAG_MS: process.env.DIAG_MS || 5000, // 页面侧心跳打印间隔
    IMAGE_MODE: (process.env.IMAGE_MODE || 'IMAGE_INLINE').toUpperCase(), // 图片发送模式（INLINE / DOC）
    API_TOKEN: process.env.API_TOKEN || '', // API Token，用于管理端口安全
    REDIS_HOST: process.env.REDIS_HOST || 'localhost', // Redis服务器地址
    REDIS_PORT: process.env.REDIS_PORT || 6379, // Redis端口
    REDIS_PASSWORD: process.env.REDIS_PASSWORD || '', // Redis密码（如果有）
    REDIS_DB: process.env.REDIS_DB || 0, // Redis数据库索引
};
