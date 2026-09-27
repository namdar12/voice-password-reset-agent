# Voice Password Reset Agent

Reset your password over a phone call. This is a voice AI agent that answers
incoming Twilio calls, holds a real-time spoken conversation via
**Amazon Nova Sonic** (speech-to-speech on AWS Bedrock), and walks the caller
through a password reset: it asks for their email, sends a verification code
via Gmail, waits for them to speak the code, verifies it, and resets the
password — all by voice.

## Architecture

```
Caller --phone--> Twilio --Media Streams (WebSocket)--> FastAPI (/media-stream)
                                                              |
                                                     BedrockStreamManager
                                                              |  bidirectional stream
                                                              v
                                                    Amazon Nova Sonic
                                                    (amazon.nova-sonic-v1:0)
                                                              |
                                              tools: gmailSendTool, passwordResetTool
                                                              |
                                              +--> Gmail (verification code)
                                              +--> password_tool (demo user store)
```

- `server.py` — FastAPI app. `/incoming-call` returns TwiML that streams call
audio to `/media-stream`; the WebSocket handler bridges Twilio audio
(8 kHz G.711 μ-law) to Nova Sonic (16 kHz PCM in, 24 kHz PCM out,
transcoded back to μ-law) and tracks the call in Redis.
- `amazon_nova.py` — `BedrockStreamManager`: manages the Nova Sonic
bidirectional stream, conversation phases, and tool dispatch (`gmailSendTool`
→ sends the code, `passwordResetTool` → resets the password).
- `gmail_tool.py` — sends verification codes via Gmail SMTP (app password).
- `gmail_service.py` — alternative Gmail API (OAuth) sender.
- `password_tool.py` — demo password store (salted SHA-256 in a local JSON
file; swap for a real user DB in production).
- `redis_service.py` — Redis-backed active-call state.
- `Implementation.md` — the original design doc for the whole flow.

## Setup

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

   `amazon_nova.py` also needs the Nova Sonic streaming SDK
(`aws_sdk_bedrock_runtime` + `smithy_aws_core`) — see the
[Amazon Nova Sonic docs](https://docs.aws.amazon.com/nova/latest/userguide/speech.html)
for install instructions.

2. Configure environment:

   ```bash
   cp .env.example .env   # then fill in your keys
   ```

3. Start Redis (e.g. `redis-server`), then run:

   ```bash
   uvicorn server:app --reload
   ```

4. Expose the server publicly (e.g. `ngrok http 8000`) and point your Twilio
number's **Voice** webhook at `https://<your-public-url>/incoming-call`.

5. Call the Twilio number and say your email address when asked.

## Configuration

| Variable | Required | Default | Description |
|---|---|---|---|
| `TWILIO_ACCOUNT_SID` / `TWILIO_AUTH_TOKEN` / `TWILIO_PHONE_NUMBER` | for calls | — | Twilio credentials and voice number |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` | yes | — | AWS credentials with Bedrock access |
| `AWS_REGION` | no | `us-east-1` | Bedrock region (Nova Sonic availability varies) |
| `GMAIL_USER` / `GMAIL_APP_PASSWORD` | yes | — | Gmail address + app-specific password for verification emails |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_DB` / `REDIS_PASSWORD` | no | `localhost:6379/0` | Redis for active-call state |
| `HOST` / `PORT` | no | `0.0.0.0:8000` | Server bind |

## Security notes

- Never commit `.env`, `credentials.json`, `token.pickle`, or
`user_passwords.json` — all are gitignored.
- The demo password store is a local JSON file; use a real database with a
proper password-hashing algorithm (e.g. bcrypt) in production.
- Avoid logging verification codes or passwords.
