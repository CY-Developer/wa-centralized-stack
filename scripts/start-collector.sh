# 环境变量
BASE="http://localhost:3000"
ACC=1
TOKEN="L5fJbLV7SXtD7e73zPUJkd1q"

# 2.1 建联系人
curl -s -X POST "$BASE/api/v1/accounts/$ACC/contacts" \
  -H "Content-Type: application/json" \
  -H "api_access_token: $TOKEN" \
  -d '{"name":"Felix QA","phone_number":"+6281234567890"}' | tee /tmp/ct.json

CT_ID=$(jq -r '.id' /tmp/ct.json)

# 2.2 建会话（用 inbox 的**数字 id**）
INBOX_ID=WA_Bridge_inbox_id
curl -s -X POST "$BASE/api/v1/accounts/$ACC/conversations" \
  -H "Content-Type: application/json" \
  -H "api_access_token: $TOKEN" \
  -d "{\"contact_id\":$CT_ID,\"inbox_id\":$INBOX_ID,\"source_id\":\"wa:+6281234567890\"}" | tee /tmp/cv.json

CV_ID=$(jq -r '.id' /tmp/cv.json)

# 2.3 发一条“访客来消息”
curl -s -X POST "$BASE/api/v1/accounts/$ACC/conversations/$CV_ID/messages" \
  -H "Content-Type: application/json" \
  -H "api_access_token: $TOKEN" \
  -d '{"content":"Hello from bridge","message_type":0,"private":false}'
