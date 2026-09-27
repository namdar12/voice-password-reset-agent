import asyncio
import audioop
import base64
import json
import os
import traceback
from typing import Dict

try:
    import numpy

    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    print("Warning: numpy not available, test tone generation disabled")

from dotenv import load_dotenv
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import HTMLResponse
from twilio.twiml.voice_response import Connect, VoiceResponse

from amazon_nova import BedrockStreamManager
from redis_service import redis_service

# Load environment variables
load_dotenv()

app = FastAPI()

# Redis key prefix for active calls
REDIS_CALL_PREFIX = "active_call:"


@app.get("/")
async def root():
    return {"message": "Voice Password Reset Service"}


@app.api_route("/incoming-call", methods=["GET", "POST"])
async def handle_incoming_call(request: Request):
    """Handle incoming Twilio calls and set up media streaming."""
    response = VoiceResponse()
    response.say("Please wait while we connect your call...")

    # Set up media streaming
    connect = Connect()
    connect.stream(url=f"wss://{request.base_url.hostname}/media-stream")
    response.append(connect)

    return HTMLResponse(content=str(response), media_type="application/xml")


@app.websocket("/media-stream")
async def media_stream(websocket: WebSocket):
    print("WebSocket connection attempt received")
    await websocket.accept()
    print("WebSocket connection accepted")
    br = None
    send_task = None
    recv_task = None
    call_id = None

    try:
        print("Starting Bedrock Stream Manager")
        br = BedrockStreamManager()
        print("Initializing conversation with Nova")
        await br.start_conversation()
        print("Conversation started successfully")

        # Generate a unique call ID
        call_id = f"{REDIS_CALL_PREFIX}{websocket.client.host}:{websocket.client.port}"

        # Store the call information in Redis
        call_data = {
            "host": websocket.client.host,
            "port": websocket.client.port,
            "status": "active",
        }
        redis_service.set_key(call_id, call_data, expire=3600)  # Expire after 1 hour

        async def twilio_to_nova():
            print("Starting Twilio to Nova audio processing")
            await asyncio.sleep(2)  # Give Nova time to initialize
            print("Nova initialization complete, ready to process audio")

            while br.is_active:
                try:
                    print("Waiting for Twilio audio message...")
                    msg = await websocket.receive_text()
                    print(f"Received message from Twilio: {msg[:100]}...")
                    evt = json.loads(msg)

                    if evt["event"] == "media" and br.is_active:
                        print("Processing media event")
                        raw = base64.b64decode(evt["media"]["payload"])
                        pcm8 = audioop.ulaw2lin(raw, 2)
                        pcm16, _ = audioop.ratecv(pcm8, 2, 1, 8000, 16000, None)

                        # Check audio phase before sending
                        if br.audio_phase == "live":
                            print(f"Sending audio chunk to Nova: {len(pcm16)} bytes")
                            await br.send_audio_chunk(pcm16)
                        else:
                            print(f"Buffering audio chunk in greeting phase")
                    elif evt["event"] == "stop":
                        print("Received stop event from Twilio")
                        break
                except asyncio.CancelledError:
                    print("Twilio to Nova task cancelled")
                    break
                except Exception as e:
                    print(f"Error processing Twilio audio: {e}")
                    traceback.print_exc()
                    break

        async def nova_to_twilio():
            print("Starting Nova to Twilio audio forwarding task")
            test_audio_sent = False
            start_time = asyncio.get_event_loop().time()
            force_live_attempted = False
            consecutive_timeouts = 0
            max_consecutive_timeouts = 3

            while br.is_active:
                try:
                    print("Waiting for audio from Nova...")
                    # Use the new method with timeout
                    pcm24 = await br.nova_to_twilio(timeout=10)  # Shorter timeout

                    if pcm24 is None:
                        consecutive_timeouts += 1
                        print(
                            f"No audio received from Nova (timeout {consecutive_timeouts}/{max_consecutive_timeouts})"
                        )

                        if consecutive_timeouts >= max_consecutive_timeouts:
                            print(
                                "Too many consecutive timeouts, attempting to recover..."
                            )
                            await br.force_to_live_phase()
                            consecutive_timeouts = 0
                            continue

                        # Check if we've been waiting too long in greeting phase
                        current_time = asyncio.get_event_loop().time()
                        elapsed = current_time - start_time

                        # After 5 seconds with no audio response, force to live phase
                        if (
                            elapsed > 5
                            and br.audio_phase == "greeting"
                            and not force_live_attempted
                        ):
                            print(
                                f"No audio after {elapsed:.1f} seconds. Force switching to live phase"
                            )
                            await br.force_to_live_phase()
                            force_live_attempted = True

                        # Only print state every 5 seconds to avoid spamming logs
                        if not test_audio_sent:
                            # Send a test message to check if Twilio is receiving audio
                            try:
                                print("Sending test audio packet to Twilio")
                                # Generate a basic tone or use a dummy sound
                                if NUMPY_AVAILABLE:
                                    test_audio = generate_test_tone(440, 1.0, 8000, 1)
                                else:
                                    # Simple dummy sound if numpy not available
                                    test_audio = bytes(
                                        [128] * 8000
                                    )  # 1 second of silence
                                test_payload = base64.b64encode(test_audio).decode()
                                await websocket.send_text(
                                    json.dumps(
                                        {
                                            "event": "media",
                                            "media": {"payload": test_payload},
                                        }
                                    )
                                )
                                print("Test audio sent to Twilio")
                                test_audio_sent = True
                                br.print_state()
                            except Exception as e:
                                print(f"Failed to send test audio: {e}")
                        await asyncio.sleep(1)
                        continue

                    # Reset timeout counter on successful audio
                    consecutive_timeouts = 0

                    print(f"Processing audio response: {len(pcm24)} bytes")
                    pcm8, _ = audioop.ratecv(pcm24, 2, 1, 24000, 8000, None)
                    ulaw = audioop.lin2ulaw(pcm8, 2)
                    payload = base64.b64encode(ulaw).decode()
                    await websocket.send_text(
                        json.dumps({"event": "media", "media": {"payload": payload}})
                    )
                    print("Sent audio back to Twilio")
                except asyncio.CancelledError:
                    print("Nova to Twilio task cancelled")
                    break
                except Exception as e:
                    print(f"Error sending audio to Twilio: {e}")
                    traceback.print_exc()
                    await asyncio.sleep(1)  # Avoid tight loop on errors

        # Run both loops concurrently
        send_task = asyncio.create_task(nova_to_twilio())
        recv_task = asyncio.create_task(twilio_to_nova())

        done, pending = await asyncio.wait(
            [send_task, recv_task], return_when=asyncio.FIRST_COMPLETED
        )

        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    except Exception as e:
        print(f"WebSocket error: {e}")
        traceback.print_exc()
    finally:
        if br:
            await br.end_conversation()
        if send_task and not send_task.done():
            send_task.cancel()
        if recv_task and not recv_task.done():
            recv_task.cancel()
        if call_id:
            # Remove the call from Redis when it ends
            redis_service.delete_key(call_id)
        await websocket.close()


def generate_test_tone(frequency, duration, sample_rate, amplitude):
    """Generate a simple test tone."""
    if not NUMPY_AVAILABLE:
        # Fallback if numpy not available
        return bytes([128] * int(sample_rate * duration))

    samples = int(duration * sample_rate)
    omega = 2 * 3.14159 * frequency / sample_rate
    xvalues = numpy.arange(samples)
    sine_wave = amplitude * numpy.sin(omega * xvalues)
    # Convert to 8-bit unsigned PCM
    audio_data = numpy.array(sine_wave * 127 + 128, dtype=numpy.uint8).tobytes()
    return audio_data


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "server:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", 8000)),
        reload=True,
    )
