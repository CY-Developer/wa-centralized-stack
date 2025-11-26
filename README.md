# WA 集中化栈（AdsPower + CDP + 可接 Chatwoot）

一个面向生产的最小骨架，用于基于 **AdsPower + Puppeteer/CDP** 的 **WhatsApp Web 消息聚合**，
并实现**读/写解耦**与**可接入 Chatwoot** 的集成。

- **collector/**：附着到 AdsPower 的浏览器 Profile，注入轻量的 content-script 进行**只读监听**，
  并把入站消息转发到中间层（或直接调用 Chatwoot API）。
- **sender/**：接收 Chatwoot **webhook**（出站消息），进入队列后以 Puppeteer 执行**拟人化发送**
  （打字、随机延迟），并用 **Bottleneck** 做限速、**BullMQ** 做队列。
- **common/**：一些小型的共享工具（logger、utils）。
- **tools/**：Chatwoot 集成的配置说明与 cURL 示例。
- **scripts/**：便捷启动脚本。

> 本仓库是一个**参考骨架**。需要根据自己的环境调整选择器与 Chatwoot API 端点。
> 代码刻意保持职责清晰且规模最小。

---

## 快速开始（Quick Start）

### 先决条件
- Node.js >= 18，npm 或 pnpm/yarn
- Redis（给 BullMQ 队列用）
- 本地已运行 AdsPower，且 **Profile 已登录** WhatsApp
- Chatwoot（自建或云端），已创建 API Channel

### 1) 配置环境
将 `.env.example` 复制为 `.env` 并填写变量。关键项：
- `REDIS_URL` – 队列使用的 Redis 连接
- `CHATWOOT_BASE_URL`、`CHATWOOT_API_TOKEN`、`CHATWOOT_ACCOUNT_ID`、`CHATWOOT_INBOX_IDENTIFIER`
- `PROFILES` – JSON 数组。每个条目定义一个 AdsPower Profile 的附着信息与路由，例如：
```json
[{
  "profileId": "ap_123",
  "accountLabel": "wa_1",
  "wsEndpoint": "ws://127.0.0.1:58888/devtools/browser/xxxx",
  "defaultDialCode": "+65"
}]
```
> `wsEndpoint` 可在通过 AdsPower Local API 启动某个 Profile 时获得。保持 AdsPower 的端口/鉴权一致。

### 2) 安装依赖
在项目根目录执行：
```bash
npm -w collector i
npm -w sender i
npm -w common i
```

### 3) 运行服务
分别打开两个终端：
```bash
# 1) collector（只读监听）
npm -w collector run start

# 2) sender（webhook + worker）
npm -w sender run start
```
- 配置一个 **Chatwoot webhook**，POST 到 `http://YOUR_SENDER_HOST:3001/chatwoot/webhook`。
- 确保 Chatwoot API Channel 使用正确的 **inbox identifier**；collector 会通过 Chatwoot API 推送入站消息。

### 安全 / 风控说明
- Collector 从不点击或输入。它优先通过 Store hook（首选）监听，DOM 观察器作为兜底。无流量时会**退避**并**冷却**（见代码注释）。
- Sender 以**账号维度串行队列**运行，设置 **minTime** 间隔与抖动。消息按字符逐字输入，带随机延迟，发送前包含已读回执延迟。

---

## 目录结构
```
collector/
  src/
    index.js               # 附着到 AdsPower，注入 hook，转发入站
    config.js              # 加载 env/PROFILES
    chatwoot.js            # 最小 Chatwoot 适配（建/查联系人、发入站消息）
    inject/wa_hook.js      # 注入脚本：尝试 hook window.Store；DOM 兜底；通过暴露函数把事件回给 Node
  package.json
sender/
  src/
    webhook.js             # Express Webhook（接 Chatwoot 出站）
    queue.js               # BullMQ 队列初始化
    worker.js              # 队列消费 -> 调用 sender
    sender.js              # Puppeteer 拟人动作（打开会话、打字、发送）
    config.js              # 加载 env 与映射
  package.json
common/
  src/
    logger.js              # Pino 日志
    utils.js               # 工具方法（sleep、jitter、手机号规范化）
  package.json
tools/
  chatwoot_setup.md        # 如何创建 inbox + webhook，及常见字段映射提示
  curl_examples.sh         # Chatwoot API 的 cURL 示例
scripts/
  start-collector.sh
  start-sender.sh
.env.example
package.json

```

---

## 免责声明（DISCLAIMER）
本代码仅用于教学/参考目的。必须确保使用方式符合 WhatsApp/Meta 及当地法律，
并遵守限速与用户同意。随着 WhatsApp Web 的演进，请相应调整选择器与流程。
"# wa-centralized-stack" 
