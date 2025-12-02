######## Core ########
BRIDGE_PORT=5010
BRIDGE_BASE=http://192.168.11.45:5010
SESSIONS=k16hf6nn,k16f9wid,k165wa1x,k165w8k8
#SESSIONS=k165wa1x,k165w8k8,k16hf6nn
BRIDGE_API_TOKEN=your-generated-token
# —— 令 whatsapp-manager 能把消息推给 192.168.11.45
COLLECTOR_BASE=http://192.168.11.45:7001
######## AdsPower ########
ADSPOWER_BASE=http://192.168.11.45:50325
ADSPOWER_API_KEY=
WA_BRIDGE_URL=http://192.168.11.45:5010
######## WebGuards (弹窗/多标签) ########
AUTO_TAKEOVER=1
AUTO_CLICK_USE_HERE=1
CDP_CLOSE_DUP_TABS=1
PROTOCOL_TIMEOUT_MS=120000

######## Send & Anti-abuse ########
RATE_PER_MIN=12
SAME_CHAT_COOLDOWN_MS=6000
# 关闭 wweb.js 的 Web 版本磁盘缓存（none|memory|local）
WWEB_CACHE=none
# 如需每次启动先清理磁盘缓存（可选）
WWEB_CACHE_CLEAN=0
######## Media ########
# 可不设，默认 192.168.11.45 目录下的 media/
MEDIA_DIR=./192.168.11.45/media

######## API Security ########
API_TOKEN=your-generated-token

######## Chatwoot / Queue ########
# Chatwoot 接入（按 tools/chatwoot_setup.md 配置）
CHATWOOT_BASE_URL=http://192.168.11.45:3000
CHATWOOT_API_TOKEN=83zGH18PbjijR22SuPVxouoz
CHATWOOT_ACCOUNT_ID=2
CHATWOOT_INBOX_ID=1
CHATWOOT_INBOX_IDENTIFIER=E7Vad1XQDSXsmbNiAe8CQph6
CHATWOOT_WEBHOOK_VERIFY=true
# Collector（接收 manager 入站，转发到 Chatwoot）
COLLECTOR_PORT=7001
COLLECTOR_INGEST_TOKEN=your-generated-token
PUBLIC_BASE_URL=http://192.168.11.45:7001
# Sender（接收 Chatwoot webhook，回发到 manager）
SENDER_PORT=3001
MANAGER_BASE=http://192.168.11.45:5010
SENDER_USE_QUEUE=1
$2a$11$RzuiGEsW4wJ4GS5MZo7jzOCmxChondGmIZs6/5jZwov39K6/bzu7u
$2a$12$tut20v2umlim87hLp34BbejQxHMlu4LJBXyhby4XdCp3Pbqvg2Atu
# Redis（幂等+队列）
REDIS_URL=redis://192.168.11.45:6379
# 关闭 Chatwoot 调用，仅做幂等+落盘，返回模拟ID（先联通链路）
CHATWOOT_DISABLED=0
DEFAULT_SESSION_ID=
CW_INTERNAL_BASE=http://192.168.11.45:3000