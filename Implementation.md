Below is a high-level **Implementation Plan**. You can hand this to a junior engineer as an outline they can follow step by step. Most of the moving parts are already shown in the two code examples you provided (“amazon-nova speech” and “twilio using open ai”). We just need to adapt them to your use-case of verifying a user, sending an email with a code/phrase, and then letting them change their password.

---

## 1. Overall Flow Summary

1. **User calls your Twilio phone number**.
2. **Twilio** forwards the caller’s audio (via a Media Stream or `<Connect><Stream>` tag) to your backend.
3. **Your Backend** connects that audio to **Amazon Nova** (the LLM) for real-time speech recognition and conversation flow.
4. **The LLM** does the following:
   1. Asks for the user’s email address.
   2. Invokes a **Gmail Tool** (custom tool) to send a one-time code or phrase to that email.
   3. Waits for the user to say the code/phrase.
   4. Verifies the user-supplied code with the one it generated/sent.
   5. If correct, it calls a **Password Change Tool** (custom tool) to reset the password.
   6. Tells the user “Your password has been reset” and ends the call.
5. **Conversation ends** once the LLM says it is finished or the user hangs up.

---

## 2. Twilio: Set Up the Phone Number and Streaming

1. **Buy and Configure a Twilio Phone Number**

   - In the [Twilio Console](https://www.twilio.com/console), buy a new phone number (or repurpose an existing one).
   - Under the **Voice & Fax** config for that number, set the “A CALL COMES IN” webhook to your backend endpoint (e.g., `POST https://<your-server>/incoming-call`).

2. **Enable Media Streams (or `<Connect><Stream>`)**

   - In your Twilio function, you’ll return TwiML that instructs Twilio to start streaming audio to a WebSocket endpoint.
   - The minimal TwiML looks like:
     ```xml
     <Response>
       <Say>Welcome to the password reset service.</Say>
       <Connect>
         <Stream url="wss://<your-domain>/media-stream" />
       </Connect>
     </Response>
     ```
   - This sets up a **bidirectional** (or one-directional, depending on your configuration) audio path from Twilio to your server.

3. **Implement Your `/incoming-call` Handler**
   - Based on your Twilio example code:
     ```python
     @app.api_route("/incoming-call", methods=["GET", "POST"])
     async def handle_incoming_call(request: Request):
         response = VoiceResponse()
         response.say("Please wait while we connect your call ...")
         connect = Connect()
         connect.stream(url=f'wss://{request.url.hostname}/media-stream')
         response.append(connect)
         return HTMLResponse(content=str(response), media_type="application/xml")
     ```
   - This is essentially what you already have in your `<twilio>` sample.

---

## 3. Backend: Media Stream WebSocket Handling

1. **Accept the WebSocket** from Twilio (`/media-stream`)

   - Twilio will send real-time audio in G.711 μlaw or PCM fragments.
   - You can see in your `<twilio>` code example how you:
     1. `await websocket.accept()`
     2. Use `websockets.connect('wss://api.openai.com/v1/realtime?...')` in the example.
   - But instead of connecting to OpenAI’s Realtime GPT-4, you’ll adapt this to connect to **Amazon Nova**.

2. **Forward Audio** to the Amazon Nova streaming code

   - In the `<twilio>` example, there’s a loop that reads from the Twilio WebSocket (`async for message in websocket.iter_text()`) and parses out the media packets.
   - Each media packet is base64’d G.711 or PCM data.
   - You can re-encode or transcode as needed to match Amazon Nova’s expected format (your `<amazon Nova Speech>` example uses 16-bit, 16kHz PCM).
   - Send these frames to your `stream_manager.add_audio_chunk(...)` method or an equivalent in your adapted code.

3. **Send LLM’s Audio Response** back to the caller
   - Amazon Nova will generate audio output in 24kHz if you keep the default config. Twilio expects G.711 μlaw (8kHz). You have to do one of the following:
     - **Option A:** On-the-fly transcode 24kHz PCM → 8kHz μlaw for Twilio.
     - **Option B:** Reconfigure Nova to output 8kHz or 16kHz PCM, then encode to G.711.
   - This transcoding step is typically done by a library (FFmpeg, `pydub`, or a direct `sox`-like library).
   - In your example, the `<twilio>` code sends a `media` event with base64’d PCM back to Twilio. Twilio can handle PCM if you’ve set it up properly, but typically it’s G.711.

> **Tip:** The simplest approach might be to have Nova produce 8kHz μlaw directly (if it supports that) so you can skip the complicated transcoding steps.

---

## 4. Amazon Nova: Conversation and Tool Calls

**In your `<amazon Nova speech>` code,** notice the “Tools” concept. We can define two new tools:

1. **Gmail Sending Tool**:

   - **Tool Name**: `gmailSendTool`
   - **Description**: “Send a one-time verification code to a user’s email.”
   - **Input Schema**: A JSON with keys like `{ "userEmail": "string", "verificationCode": "string" }`.
   - The tool’s Python function uses a Gmail API or SMTP library to send the code.

2. **Password Reset Tool**:
   - **Tool Name**: `passwordResetTool`
   - **Description**: “Change the user’s password in the database.”
   - **Input Schema**: e.g. `{ "userEmail": "string", "newPassword": "string" }`.

### 4.1. Insert Tools into the Prompt

In your `BedrockStreamManager.start_prompt()` method (or wherever you list the available tools), add them:

```python
prompt_start_event = {
    "event": {
        "promptStart": {
            "promptName": self.prompt_name,
            "textOutputConfiguration": {...},
            "audioOutputConfiguration": {...},
            "toolUseOutputConfiguration": {...},
            "toolConfiguration": {
                "tools": [
                    {
                        "toolSpec": {
                            "name": "gmailSendTool",
                            "description": "Sends a verification code to the user's email",
                            "inputSchema": {
                                "json": json.dumps({
                                    "type": "object",
                                    "properties": {
                                        "userEmail": {"type": "string"},
                                        "verificationCode": {"type": "string"},
                                    },
                                    "required": ["userEmail", "verificationCode"]
                                })
                            }
                        }
                    },
                    {
                        "toolSpec": {
                            "name": "passwordResetTool",
                            "description": "Resets the user's password",
                            "inputSchema": {
                                "json": json.dumps({
                                    "type": "object",
                                    "properties": {
                                        "userEmail": {"type": "string"},
                                        "newPassword": {"type": "string"},
                                    },
                                    "required": ["userEmail", "newPassword"]
                                })
                            }
                        }
                    }
                ]
            }
        }
    }
}
```

_(This is similar to how you have the `getDateAndTimeTool` in the sample. You’d just add two more entries for your new tools.)_

### 4.2. Handling Tool Use in `processToolUse()`

Your existing method `processToolUse(self, toolName, toolUseContent)` is where you branch logic based on which tool the LLM is calling. So you’d extend it:

```python
async def processToolUse(self, toolName, toolUseContent):
    tool = toolName.lower()
    if tool == "gmailsendtool":
        return await self.handle_gmail_send(toolUseContent)
    elif tool == "passwordresettool":
        return await self.handle_password_reset(toolUseContent)
    ...
```

And define the two new methods:

```python
async def handle_gmail_send(self, toolUseContent):
    # toolUseContent["content"] is a JSON string
    # parse it
    data = json.loads(toolUseContent["content"])
    user_email = data["userEmail"]
    verification_code = data["verificationCode"]

    # 1. Actually send the email via Gmail API or SMTP library
    #   e.g. using `smtplib` or `google-api-python-client`.
    #   Example (pseudo-code):
    # send_email(user_email, verification_code)

    return {
        "result": f"Code {verification_code} sent to {user_email}"
    }

async def handle_password_reset(self, toolUseContent):
    data = json.loads(toolUseContent["content"])
    user_email = data["userEmail"]
    new_password = data["newPassword"]

    # 1. Update the user’s password in your DB
    #   e.g. user = db.find_user_by_email(user_email)
    #        user.password = hash(new_password)
    #        db.save(user)

    return {
        "result": f"Password changed for {user_email}"
    }
```

> **Note:** You’ll need real code that sends an email and changes a password in your system. This is just the structure.

---

## 5. The LLM Prompt/Conversation: Ensuring the Right Flow

**What the LLM must do** (in natural language, plus system instructions):

1. Greet user; ask for email.
2. Generate a random code in the LLM (like `MAGIC123`) or you can have your backend do it.
3. Use `gmailSendTool` with `{"userEmail": "theUserEmail@domain", "verificationCode": "MAGIC123"}`.
4. Prompt the user: “Please say the code we emailed you.”
5. Compare user response with “MAGIC123” (the LLM can do this on its own if it is aware of what it requested) or you can store that code in the system and let the LLM call a “verifyTool” or reason about it.
6. If correct, ask user for new password or generate a random one.
7. Call `passwordResetTool` with the user’s email and new password.
8. Confirm success.
9. Hang up.

**Implementation detail**: Because we rely on the LLM to “remember” the code it generated, you can let your backend generate the code, store it in a variable, and pass it into the LLM via a “hidden” system message, or keep it in memory. Then the LLM either calls a “verifyTool” or you add logic in `processToolUse` to confirm correctness. (lets keep it in memory)

- LLM: “What is your email?”
- LLM sees user says: `someone@domain.com`.
- LLM picks random code: `987654`.
- LLM calls `gmailSendTool`.
- LLM asks user for the code.
- LLM listens for user’s response. If user says “987654,” LLM calls `passwordResetTool`.

---

## 6. End the Call

1. **Twilio**: Once the LLM says “Password reset. Goodbye,” you can have the LLM or your backend send a `sessionEnd` event and then simply stop sending media to Twilio.
2. Twilio sees no more media and ends the call.
3. If you want a clean Twilio disconnect, you can also instruct Twilio to `<Hangup>` after a final prompt.

---

## 7. Implementation Steps Recap

1. **Set Up Twilio**

   - Buy a number, set up Voice webhook → your server’s `/incoming-call`.
   - Return TwiML that `<Connect><Stream>` to `wss://<your-domain>/media-stream`.

2. **Implement `/media-stream`** in your server (using FastAPI or Express or anything):

   - `await websocket.accept()` to accept Twilio’s WebSocket.
   - For each incoming chunk from Twilio:
     - Extract base64’d audio data → decode/convert → feed into your Amazon Nova interface code.
   - For each chunk from Amazon Nova’s audio output:
     - Convert to G.711 μlaw 8kHz (if needed) → send it back to Twilio in a `{"event":"media","media":{"payload":...}}` JSON message.

3. **Adapt the Amazon Nova streaming code** (from your `<amazon Nova speech>` example):

   - Instead of reading from your microphone, read from Twilio’s WebSocket audio frames.
   - Instead of playing audio back on your speakers, push the audio frames out to Twilio’s WebSocket.
   - Add the **two new Tools** (`gmailSendTool` and `passwordResetTool`).
   - In the “system prompt” or initial instructions to Nova, explicitly tell it the steps it needs to do (get user’s email, send code, ask for code, verify, reset password).

4. **Implement the Tools**

   - `gmailSendTool` that uses an SMTP library or the Gmail API to send the code.
   - `passwordResetTool` that updates your user’s password in the database.

5. **Test the Full Flow**

   - Call the Twilio number.
   - Provide your email verbally when asked.
   - Check your email for the code.
   - Speak the code.
   - Check if the conversation proceeds to password reset.
   - See if the final “Goodbye” triggers a call hangup.

6. **Deploy**
   - Host your server somewhere accessible over HTTPS (e.g., AWS, Google Cloud, Heroku, or your own server).
   - Make sure Twilio can reach it.
   - Confirm environment variables (AWS credentials, email credentials, etc.) are set.

---

## 8. Notes and Gotchas

- **Transcoding**: The biggest complexity is matching audio formats between Twilio and Amazon Nova. Twilio typically sends 8kHz G.711, whereas your code example uses 16kHz PCM. You’ll need to use a small library or method to do the sample rate conversion (and encoding/decoding).
- **LLM Memory**: Make sure the LLM is set up to remember the code or store the code in your backend. If the LLM forgets the code it emailed, it won’t be able to verify.
- **Security**: Use HTTPS and secure your environment variables. Store logs carefully; you don’t want to log real user passwords or codes in plain text.
- **Handling Partial or Inaccurate Speech**: The user might say their code incorrectly. The LLM might mis-hear them. Consider building some repetition logic or a fallback approach if the code is incorrectly repeated multiple times.

---

## 9. Putting It All Together

Below is a simplified pseudo-directory layout for clarity:

```
project/
  ├─ server.py          # Your FastAPI (or Flask) server with Twilio endpoints
  ├─ amazon_nova.py     # The adapted Amazon Nova streaming code
  ├─ gmail_tool.py      # Custom code for sending emails
  ├─ password_tool.py   # Custom code for changing passwords
  ├─ .env               # Twilio, AWS, and Email credentials
  └─ requirements.txt   # Python dependencies
```

Implementation steps in code:

1. **In `server.py`**:

   - Define `@app.post("/incoming-call")` that returns TwiML using the `<Connect><Stream>` verb.
   - Define `@app.websocket("/media-stream")` to handle streaming logic.
   - Inside that WebSocket handler, connect to your `BedrockStreamManager`, pass audio both ways.

2. **In `amazon_nova.py`**:

   - Copy-paste your `<amazon Nova speech>` code.
   - Remove the microphone reading and speaker playback code.
   - Add your new tools in `start_prompt()`.
   - Expand `processToolUse()` to call your `gmail_tool.send_code(...)` or `password_tool.reset(...)`.

3. **In `gmail_tool.py`**:

   - Provide a function that sends an email via Gmail (or any SMTP).

4. **In `password_tool.py`**:

   - Provide a function that changes the user’s password in your system.

5. **Run & Test**.

That’s the broad plan. Each step is fairly modular, and your junior engineer can tackle each piece individually. Once all pieces are plugged in, the user’s voice will route from Twilio → your server → Amazon Nova → Tools → Amazon Nova → your server → Twilio → user’s ear, enabling the password reset flow.

---

### Good luck!

With the above plan, a junior engineer should have a clear picture of how to wire everything together. The key is to **focus on hooking up the Twilio audio** to the **Amazon Nova LLM** (and vice versa), **defining the conversation** in the LLM’s system prompt, and **creating the two custom tools** (email & password reset). Once the flow works end-to-end, test thoroughly for edge cases (mispronounced codes, invalid emails, etc.).
