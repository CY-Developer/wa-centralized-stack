# Chatwoot Setup (API Channel + Webhook)

1) **Create API Channel (Inbox)**
   - In Chatwoot admin -> Inboxes -> Add -> API.
   - Name it e.g. `WhatsApp (Browser)`.
   - Copy the **Inbox Identifier**; put it into `.env` as `CHATWOOT_INBOX_IDENTIFIER`.
   - Get an **API Access Token** (Profile -> Access Tokens) and set `CHATWOOT_API_TOKEN`.
   - Note your **Account ID** (URL usually contains it) to set `CHATWOOT_ACCOUNT_ID`.

2) **Configure Webhook**
   - Settings -> Integrations -> Webhook -> Add URL: `http://<sender-host>:3001/chatwoot/webhook`
   - Select events for messages (outgoing).

3) **Contact mapping**
   - The collector `ensureContact` uses phone as the key. Adjust as you need (email, custom id, ...).

4) **Test flow**
   - Send a message from your phone to the WhatsApp account in a running AdsPower profile.
   - The collector should forward it to Chatwoot and show in the Inbox.
   - Reply in Chatwoot -> webhook -> sender queue -> WA page types and sends.
