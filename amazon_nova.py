# NOTE: This file requires the aws_sdk_bedrock_runtime SDK and smithy_aws_core (see Amazon Nova Sonic docs for install instructions)
import asyncio
import base64
import json
import os
import traceback
import uuid
from typing import Any, Dict, Optional

# Amazon Nova Sonic streaming SDK imports
from aws_sdk_bedrock_runtime.client import (
    BedrockRuntimeClient,
    InvokeModelWithBidirectionalStreamOperationInput,
)
from aws_sdk_bedrock_runtime.config import (
    Config,
    HTTPAuthSchemeResolver,
    SigV4AuthScheme,
)
from aws_sdk_bedrock_runtime.models import (
    BidirectionalInputPayloadPart,
    InvokeModelWithBidirectionalStreamInputChunk,
    ModelStreamErrorException,
)
from dotenv import load_dotenv
from smithy_aws_core.credentials_resolvers.environment import (
    EnvironmentCredentialsResolver,
)

# Load environment variables
load_dotenv()


class BedrockStreamManager:
    def __init__(self, region: str = None):
        self.region = region or os.getenv("AWS_REGION", "us-east-1")
        self.model_id = "amazon.nova-sonic-v1:0"
        self.client = None
        self.stream = None
        self.is_active = False
        self.prompt_name = None
        self.audio_content_name = None
        self.audio_output_queue = asyncio.Queue()
        self.verification_code = None
        self.user_email = None
        self.conversation_state = {}
        self.response_task = None
        self.has_audio_input = False
        self.prompt_ended = False
        self.open_content_streams = set()
        self.should_process_audio = True
        self.current_prompt_name = None
        self.audio_chunk_count = 0
        self.last_audio_time = None
        self.speech_detection_task = None
        self.initial_response_sent = False
        self.waiting_for_response = False
        self.audio_phase = "greeting"  # "greeting" or "live"
        self.connection_retries = 0
        self.max_retries = 3
        self.retry_delay = 1.0  # seconds
        print(f"Initialized BedrockStreamManager with audio_phase: {self.audio_phase}")

    def _initialize_client(self):
        config = Config(
            endpoint_uri=f"https://bedrock-runtime.{self.region}.amazonaws.com",
            region=self.region,
            aws_credentials_identity_resolver=EnvironmentCredentialsResolver(),
            http_auth_scheme_resolver=HTTPAuthSchemeResolver(),
            http_auth_schemes={"aws.auth#sigv4": SigV4AuthScheme()},
        )
        self.client = BedrockRuntimeClient(config=config)

    async def send_initial_text_prompt(self):
        """Send the initial text-only prompt with system instructions."""
        self.prompt_name = str(uuid.uuid4())
        self.prompt_ended = False
        self.waiting_for_response = True  # Block real audio early

        print(f"Creating initial text prompt: {self.prompt_name}")
        self.print_state()

        # Start prompt with both text and audio configurations
        prompt_start = {
            "event": {
                "promptStart": {
                    "promptName": self.prompt_name,
                    "textOutputConfiguration": {"mediaType": "text/plain"},
                    "audioOutputConfiguration": {
                        "mediaType": "audio/lpcm",
                        "sampleRateHertz": 24000,
                        "sampleSizeBits": 16,
                        "channelCount": 1,
                        "voiceId": "matthew",
                        "encoding": "base64",
                        "audioType": "SPEECH",
                    },
                    "toolConfiguration": {
                        "tools": [
                            {
                                "toolSpec": {
                                    "name": "gmailSendTool",
                                    "description": "Sends a verification code to the user's email",
                                    "inputSchema": {
                                        "json": json.dumps(
                                            {
                                                "type": "object",
                                                "properties": {
                                                    "userEmail": {"type": "string"},
                                                    "verificationCode": {
                                                        "type": "string"
                                                    },
                                                },
                                                "required": [
                                                    "userEmail",
                                                    "verificationCode",
                                                ],
                                            }
                                        )
                                    },
                                }
                            },
                            {
                                "toolSpec": {
                                    "name": "passwordResetTool",
                                    "description": "Resets the user's password",
                                    "inputSchema": {
                                        "json": json.dumps(
                                            {
                                                "type": "object",
                                                "properties": {
                                                    "userEmail": {"type": "string"},
                                                    "newPassword": {"type": "string"},
                                                },
                                                "required": [
                                                    "userEmail",
                                                    "newPassword",
                                                ],
                                            }
                                        )
                                    },
                                }
                            },
                        ]
                    },
                }
            }
        }
        print("Sending initial promptStart")
        await self.send_event(prompt_start)
        await asyncio.sleep(0.5)  # Wait for prompt to be registered

        # First send system text (MUST be first content)
        print("Sending system text first")
        system_content_name = str(uuid.uuid4())
        system_content_start = {
            "event": {
                "contentStart": {
                    "promptName": self.prompt_name,
                    "contentName": system_content_name,
                    "type": "TEXT",
                    "interactive": True,
                    "role": "SYSTEM",
                    "textInputConfiguration": {"mediaType": "text/plain"},
                }
            }
        }
        print("Sending system content start")
        await self.send_event(system_content_start)

        system_text_input = {
            "event": {
                "textInput": {
                    "promptName": self.prompt_name,
                    "contentName": system_content_name,
                    "content": "You are a helpful password reset assistant. Start by greeting the user warmly and ask for their email address to reset their password. Make sure to speak clearly and be conversational.",
                }
            }
        }
        print("Sending system instruction")
        await self.send_event(system_text_input)

        system_content_end = {
            "event": {
                "contentEnd": {
                    "promptName": self.prompt_name,
                    "contentName": system_content_name,
                }
            }
        }
        await self.send_event(system_content_end)
        await asyncio.sleep(0.5)  # Ensure system content is processed
        print("System content complete, adding audio content")

        # Now add token audio stream as the second content
        print("Adding token audio after system content")
        self.audio_content_name = str(uuid.uuid4())
        audio_start = {
            "event": {
                "contentStart": {
                    "promptName": self.prompt_name,
                    "contentName": self.audio_content_name,
                    "type": "AUDIO",
                    "interactive": False,  # It's just a placeholder
                    "role": "USER",
                    "audioInputConfiguration": {
                        "mediaType": "audio/lpcm",
                        "sampleRateHertz": 16000,
                        "sampleSizeBits": 16,
                        "channelCount": 1,
                        "audioType": "SPEECH",
                        "encoding": "base64",
                    },
                }
            }
        }
        await self.send_event(audio_start)

        # 100 ms of silence (16 kHz * 16-bit mono => 16000*0.1*2 = 3200 bytes)
        print("Sending token silence audio")
        silent_chunk = base64.b64encode(b"\x00" * 3200).decode("utf-8")
        await self.send_event(
            {
                "event": {
                    "audioInput": {
                        "promptName": self.prompt_name,
                        "contentName": self.audio_content_name,
                        "content": silent_chunk,
                    }
                }
            }
        )

        print("Closing token audio content")
        await self.send_event(
            {
                "event": {
                    "contentEnd": {
                        "promptName": self.prompt_name,
                        "contentName": self.audio_content_name,
                    }
                }
            }
        )
        await asyncio.sleep(0.5)  # Ensure audio content is processed

        # Close the initial prompt
        print("Closing initial text prompt")
        prompt_end = {"event": {"promptEnd": {"promptName": self.prompt_name}}}
        await self.send_event(prompt_end)
        self.prompt_ended = True
        self.initial_response_sent = True
        print("Initial prompt closed, now waiting for response")
        self.print_state()
        # waiting_for_response already set to True

    def print_state(self):
        """Print the current state of the stream manager for debugging."""
        state = {
            "is_active": self.is_active,
            "prompt_name": self.prompt_name,
            "audio_content_name": self.audio_content_name,
            "waiting_for_response": self.waiting_for_response,
            "initial_response_sent": self.initial_response_sent,
            "audio_phase": self.audio_phase,
            "prompt_ended": self.prompt_ended,
            "audio_output_queue_size": self.audio_output_queue.qsize(),
            "open_content_streams": list(self.open_content_streams),
        }
        print(f"STREAM STATE: {json.dumps(state, indent=2)}")

    async def create_audio_prompt(self):
        """Create a new prompt for audio input."""
        self.prompt_name = str(uuid.uuid4())
        self.audio_content_name = str(uuid.uuid4())
        self.prompt_ended = False
        self.waiting_for_response = False
        self.has_audio_input = False
        self.audio_chunk_count = 0
        self.last_audio_time = None

        print(f"Creating audio prompt: {self.prompt_name}")
        self.print_state()

        # Start prompt with same configurations
        prompt_start = {
            "event": {
                "promptStart": {
                    "promptName": self.prompt_name,
                    "textOutputConfiguration": {"mediaType": "text/plain"},
                    "audioOutputConfiguration": {
                        "mediaType": "audio/lpcm",
                        "sampleRateHertz": 24000,
                        "sampleSizeBits": 16,
                        "channelCount": 1,
                        "voiceId": "matthew",
                        "encoding": "base64",
                        "audioType": "SPEECH",
                    },
                    "toolConfiguration": {
                        "tools": [
                            {
                                "toolSpec": {
                                    "name": "gmailSendTool",
                                    "description": "Sends a verification code to the user's email",
                                    "inputSchema": {
                                        "json": json.dumps(
                                            {
                                                "type": "object",
                                                "properties": {
                                                    "userEmail": {"type": "string"},
                                                    "verificationCode": {
                                                        "type": "string"
                                                    },
                                                },
                                                "required": [
                                                    "userEmail",
                                                    "verificationCode",
                                                ],
                                            }
                                        )
                                    },
                                }
                            },
                            {
                                "toolSpec": {
                                    "name": "passwordResetTool",
                                    "description": "Resets the user's password",
                                    "inputSchema": {
                                        "json": json.dumps(
                                            {
                                                "type": "object",
                                                "properties": {
                                                    "userEmail": {"type": "string"},
                                                    "newPassword": {"type": "string"},
                                                },
                                                "required": [
                                                    "userEmail",
                                                    "newPassword",
                                                ],
                                            }
                                        )
                                    },
                                }
                            },
                        ]
                    },
                }
            }
        }
        print("Sending audio promptStart")
        await self.send_event(prompt_start)

        # Immediately open the audio channel
        audio_content_start = {
            "event": {
                "contentStart": {
                    "promptName": self.prompt_name,
                    "contentName": self.audio_content_name,
                    "type": "AUDIO",
                    "interactive": True,
                    "role": "USER",
                    "audioInputConfiguration": {
                        "mediaType": "audio/lpcm",
                        "sampleRateHertz": 16000,
                        "sampleSizeBits": 16,
                        "channelCount": 1,
                        "audioType": "SPEECH",
                        "encoding": "base64",
                    },
                }
            }
        }
        print("Setting up audio input stream")
        await self.send_event(audio_content_start)
        self.open_content_streams.add(self.audio_content_name)
        print("Audio input stream ready")
        self.print_state()

    async def start_conversation(self):
        """Start a new conversation with Nova, with retry logic."""
        while self.connection_retries < self.max_retries:
            try:
                if not self.client:
                    self._initialize_client()

                # Start the bidirectional stream
                self.stream = await self.client.invoke_model_with_bidirectional_stream(
                    InvokeModelWithBidirectionalStreamOperationInput(
                        model_id=self.model_id
                    )
                )
                self.is_active = True
                self.connection_retries = 0  # Reset retry counter on success

                # Start the response processing task
                self.response_task = asyncio.create_task(self.process_responses())

                # Send initial prompts
                await self.send_initial_text_prompt()
                return True

            except Exception as e:
                self.connection_retries += 1
                print(f"Connection attempt {self.connection_retries} failed: {e}")
                if self.connection_retries < self.max_retries:
                    await asyncio.sleep(self.retry_delay)
                else:
                    print("Max retries reached, giving up")
                    return False

    async def send_event(self, event_json: dict):
        """Send an event to Nova with retry logic."""
        if not self.is_active or not self.stream:
            print("Cannot send event: stream not active")
            return False

        retries = 0
        max_retries = 3
        while retries < max_retries:
            try:
                evt_str = json.dumps(event_json)
                chunk = InvokeModelWithBidirectionalStreamInputChunk(
                    value=BidirectionalInputPayloadPart(bytes_=evt_str.encode("utf-8"))
                )
                await self.stream.input_stream.send(chunk)
                return True
            except Exception as e:
                retries += 1
                print(f"Error sending event (attempt {retries}/{max_retries}): {e}")
                if retries < max_retries:
                    await asyncio.sleep(0.5)
                else:
                    print("Max retries reached for sending event")
                    return False

    async def send_audio_chunk(self, audio_chunk: bytes):
        """Send an audio chunk to Nova with retry logic."""
        if not self.is_active or not self.stream:
            print("Cannot send audio: stream not active")
            return False

        retries = 0
        max_retries = 3
        while retries < max_retries:
            try:
                b64data = base64.b64encode(audio_chunk).decode("utf-8")
                audio_event = {
                    "event": {
                        "audioInput": {
                            "promptName": self.prompt_name,
                            "contentName": self.audio_content_name,
                            "content": b64data,
                        }
                    }
                }
                await self.send_event(audio_event)
                self.audio_chunk_count += 1
                self.last_audio_time = asyncio.get_event_loop().time()
                return True
            except Exception as e:
                retries += 1
                print(
                    f"Error sending audio chunk (attempt {retries}/{max_retries}): {e}"
                )
                if retries < max_retries:
                    await asyncio.sleep(0.5)
                else:
                    print("Max retries reached for sending audio chunk")
                    return False

    async def detect_speech_end(self):
        """Detect when user stops speaking and close the prompt."""
        while self.is_active and not self.prompt_ended:
            await asyncio.sleep(0.1)  # Check every 100ms

            if self.last_audio_time:
                time_since_last_audio = (
                    asyncio.get_event_loop().time() - self.last_audio_time
                )

                # If we have enough audio and silence detected
                if self.audio_chunk_count > 10 and time_since_last_audio > 1.0:
                    print(f"Speech end detected after {self.audio_chunk_count} chunks")
                    await self.complete_current_prompt()
                    break

    async def complete_current_prompt(self):
        """Complete the current prompt and trigger model response."""
        if self.prompt_ended:
            return

        try:
            # Close audio content stream if open
            if self.audio_content_name in self.open_content_streams:
                content_end = {
                    "event": {
                        "contentEnd": {
                            "promptName": self.prompt_name,
                            "contentName": self.audio_content_name,
                        }
                    }
                }
                print("Closing audio content stream")
                await self.send_event(content_end)
                self.open_content_streams.remove(self.audio_content_name)

            # Send promptEnd
            prompt_end = {"event": {"promptEnd": {"promptName": self.prompt_name}}}
            print("Sending promptEnd to trigger response")
            await self.send_event(prompt_end)
            self.prompt_ended = True
            self.waiting_for_response = True

        except Exception as e:
            print(f"Error completing prompt: {e}")

    async def process_responses(self):
        """Process responses from Nova, handling audio output and tool use."""
        if not self.is_active:
            print("Warning: process_responses called when stream is not active")
            return

        print("Starting to process Nova responses")
        try:
            print("Attempting to await output stream...")
            result = await self.stream.await_output()
            print(f"await_output result: {result}")
            _, output_stream = result
            print("Successfully got output stream")

            async for output in output_stream:
                if not self.is_active:
                    print("Stream became inactive during processing")
                    break

                print(f"Received output type: {type(output)}")

                if output.value and output.value.bytes_:
                    try:
                        response_data = output.value.bytes_.decode("utf-8")
                        print(f"Raw response data: {response_data[:100]}...")
                        json_data = json.loads(response_data)
                        print(f"Processed response type: {list(json_data.keys())}")

                        # Handle different response types
                        if "event" in json_data:
                            event = json_data["event"]
                            event_type = list(event.keys())[0] if event else "unknown"
                            print(f"Event type: {event_type}")

                            # Handle turn completion
                            if "turnComplete" in event:
                                print("Turn completed - ready for next prompt")
                                self.prompt_ended = True
                                self.waiting_for_response = False

                            # Handle audio output
                            elif "audioOutput" in event:
                                print("Received audio output from Nova")
                                audio_content = event["audioOutput"]["content"]
                                audio_bytes = base64.b64decode(audio_content)
                                print(
                                    f"Audio output received: {len(audio_bytes)} bytes"
                                )
                                await self.audio_output_queue.put(audio_bytes)
                                print("Audio output queued for playback")

                            # Handle text output
                            elif "textOutput" in event:
                                text_content = event["textOutput"]["content"]
                                print(f"Assistant: {text_content}")

                            # Handle tool use
                            elif "toolUse" in event:
                                print("Tool use requested")
                                self.toolUseContent = event["toolUse"]
                                self.toolName = event["toolUse"]["toolName"]
                                self.toolUseId = event["toolUse"]["toolUseId"]
                                print(f"Tool use requested: {self.toolName}")

                                # Process tool use and send result
                                tool_result = await self.process_tool_use(
                                    self.toolName, self.toolUseContent
                                )
                                await self.send_tool_result_event(
                                    self.toolUseId, tool_result
                                )

                    except json.JSONDecodeError as e:
                        print(f"Error decoding response JSON: {e}")
                        print(f"Raw response: {response_data}")
                        continue
                    except Exception as e:
                        print(f"Error processing response: {e}")
                        traceback.print_exc()
                        continue

        except asyncio.CancelledError:
            print("Response processing was cancelled")
        except ModelStreamErrorException as e:
            print(f"Model stream error: {e}")
            traceback.print_exc()
            self.is_active = False
            raise
        except Exception as e:
            print(f"Critical error in process_responses: {e}")
            print(f"Error type: {type(e)}")
            print(f"Error details: {e}")
            traceback.print_exc()
            self.is_active = False
            raise

    async def send_tool_result_event(self, tool_use_id: str, result: dict):
        # Send toolResult event back to the model
        tool_result_event = {
            "event": {
                "toolResult": {"toolUseId": tool_use_id, "content": json.dumps(result)}
            }
        }
        await self.send_event(tool_result_event)

    async def process_tool_use(self, tool_name: str, tool_use: dict) -> dict:
        # tool_use['content'] is a JSON string
        content = json.loads(tool_use.get("content", "{}"))
        if tool_name == "gmailSendTool":
            self.verification_code = content["verificationCode"]
            self.user_email = content["userEmail"]
            from gmail_tool import send_verification_code

            await send_verification_code(
                email=content["userEmail"], code=content["verificationCode"]
            )
            return {
                "result": f"Code {content['verificationCode']} sent to {content['userEmail']}"
            }
        elif tool_name == "passwordResetTool":
            from password_tool import reset_password

            await reset_password(
                email=content["userEmail"], new_password=content["newPassword"]
            )
            return {"result": f"Password changed for {content['userEmail']}"}
        return {"result": "Unknown tool"}

    async def end_conversation(self):
        """End the conversation with proper cleanup."""
        print("Ending conversation")
        self.is_active = False

        if self.response_task and not self.response_task.done():
            self.response_task.cancel()
            try:
                await self.response_task
            except asyncio.CancelledError:
                pass

        if self.stream:
            try:
                await self.stream.input_stream.close()
            except Exception as e:
                print(f"Error closing stream: {e}")

        # Clear queues
        while not self.audio_output_queue.empty():
            try:
                self.audio_output_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

        self.stream = None
        self.client = None
        print("Conversation ended")

    async def nova_to_twilio(self, timeout=30):
        """Get audio from Nova with improved timeout handling."""
        try:
            return await asyncio.wait_for(
                self.audio_output_queue.get(), timeout=timeout
            )
        except asyncio.TimeoutError:
            print(f"No audio received from Nova within {timeout} seconds")
            return None
        except Exception as e:
            print(f"Error getting audio from Nova: {e}")
            return None

    async def _emergency_fallback(self, timeout):
        """Emergency fallback to force transition to live phase if no turnComplete received."""
        try:
            await asyncio.sleep(timeout)
            if self.audio_phase == "greeting":
                print(
                    f"EMERGENCY FALLBACK: No turnComplete received after {timeout} seconds"
                )
                print(f"EMERGENCY FALLBACK: Forcing transition to live phase")
                await self.force_to_live_phase()
        except asyncio.CancelledError:
            # Task was cancelled, which is expected
            pass
        except Exception as e:
            print(f"Error in emergency fallback: {e}")
            traceback.print_exc()

    async def force_to_live_phase(self):
        """Force transition to live phase with proper cleanup."""
        if self.audio_phase == "greeting":
            print("Forcing transition to live phase")
            try:
                # Complete any existing prompt
                if self.prompt_name and not self.prompt_ended:
                    await self.complete_current_prompt()

                # Create new prompt for live phase
                await self.create_audio_prompt()
                self.audio_phase = "live"
                print("Successfully transitioned to live phase")
            except Exception as e:
                print(f"Error forcing to live phase: {e}")
                # Try to recover by recreating the connection
                await self.end_conversation()
                await self.start_conversation()
