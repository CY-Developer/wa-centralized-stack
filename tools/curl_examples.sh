#!/usr/bin/env bash
# Example cURL snippets for Chatwoot

BASE="${CHATWOOT_BASE_URL:-http://localhost:3000}"
TOKEN="${CHATWOOT_API_TOKEN}"
ACC="${CHATWOOT_ACCOUNT_ID}"

# Create contact
curl -s -X POST "$BASE/api/v1/accounts/$ACC/contacts" \  -H "Content-Type: application/json" -H "api_access_token: $TOKEN" \  -d '{"name":"Test User","phone_number":"+6512345678"}' | jq

# List contacts search
curl -s "$BASE/api/v1/accounts/$ACC/contacts/search?q=+6512345678" \  -H "api_access_token: $TOKEN" | jq
