import requests
import json
import time
import logging
from typing import Dict, Any, Optional, List, Generator, Union

logger = logging.getLogger(__name__)

class ClaudeClient:
    """Client for interacting with Anthropic Claude API."""
    
    def __init__(self, api_key: str, api_base: Optional[str] = None):
        self.api_key = api_key
        self.api_base = api_base or "https://api.anthropic.com/v1"
        self.session = requests.Session()
        self.session.headers.update({
            "x-api-key": self.api_key,
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01"
        })
    
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a request to the Claude API."""
        url = f"{self.api_base}/{endpoint}"
        start_time = time.time()
        
        try:
            if method.upper() == "GET":
                response = self.session.get(url, params=data)
            else:
                response = self.session.request(
                    method=method,
                    url=url,
                    json=data
                )
            response.raise_for_status()
            return {
                "status_code": response.status_code,
                "response": response.json(),
                "duration_ms": int((time.time() - start_time) * 1000)
            }
        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request to Claude API: {str(e)}")
            error_data = {}
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                except:
                    error_data = {"message": str(e)}
                
            return {
                "status_code": getattr(e.response, 'status_code', 500) if hasattr(e, 'response') else 500,
                "error": error_data,
                "duration_ms": int((time.time() - start_time) * 1000)
            }
    
    def _convert_messages_to_prompt(self, messages: List[Dict[str, str]]) -> str:
        """Convert OpenAI-style messages to Claude prompt format."""
        prompt = ""
        for message in messages:
            role = message.get("role", "").lower()
            content = message.get("content", "")
            
            if role == "system":
                prompt += f"{content}\n\n"
            elif role == "user":
                prompt += f"Human: {content}\n\n"
            elif role == "assistant":
                prompt += f"Assistant: {content}\n\n"
            else:
                # For unsupported roles, we'll just append the content
                prompt += f"{content}\n\n"
                
        # Add the final "Assistant: " to prompt the model to respond
        prompt += "Assistant: "
        return prompt
    
    def chat_completion(self, messages: List[Dict[str, str]], model: str = "claude-2.1", 
                      temperature: float = 0.7, max_tokens: Optional[int] = None, 
                      stream: bool = False, **kwargs) -> Dict[str, Any]:
        """Create a chat completion using Claude."""
        # Map model names if needed
        model_mapping = {
            "claude-3-opus": "claude-3-opus-20240229",
            "claude-3-sonnet": "claude-3-sonnet-20240229",
            "claude-3-haiku": "claude-3-haiku-20240307"
        }
        
        model = model_mapping.get(model, model)
        
        # Convert OpenAI-style messages to Claude format
        prompt = self._convert_messages_to_prompt(messages)
        
        data = {
            "model": model,
            "prompt": prompt,
            "temperature": temperature,
            "stream": stream
        }
        
        if max_tokens is not None:
            data["max_tokens_to_sample"] = max_tokens
            
        # Add any additional parameters
        for key, value in kwargs.items():
            if key == "top_p":
                data["top_p"] = value
            elif key == "stop":
                data["stop_sequences"] = value
            else:
                data[key] = value
            
        if stream:
            return self._stream_chat_completion(data)
        else:
            response = self._make_request("POST", "complete", data)
            
            # Convert Claude response format to OpenAI format
            if response.get("status_code") == 200 and "response" in response:
                claude_response = response["response"]
                openai_format = {
                    "id": f"chatcmpl-{int(time.time())}",
                    "object": "chat.completion",
                    "created": int(time.time()),
                    "model": model,
                    "choices": [{
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": claude_response.get("completion", "")
                        },
                        "finish_reason": "stop"
                    }],
                    "usage": {
                        "prompt_tokens": self.estimate_tokens(prompt),
                        "completion_tokens": self.estimate_tokens(claude_response.get("completion", "")),
                        "total_tokens": self.estimate_tokens(prompt) + self.estimate_tokens(claude_response.get("completion", ""))
                    }
                }
                response["response"] = openai_format
                
            return response
    
    def _stream_chat_completion(self, data: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        """Stream a chat completion."""
        url = f"{self.api_base}/complete"
        start_time = time.time()
        
        try:
            response = self.session.post(url, json=data, stream=True)
            response.raise_for_status()
            
            for line in response.iter_lines():
                if line:
                    line = line.decode('utf-8')
                    if line.startswith('data: '):
                        line = line[6:]  # Remove 'data: ' prefix
                        if line == "[DONE]":
                            break
                        try:
                            claude_chunk = json.loads(line)
                            
                            # Convert Claude streaming format to OpenAI streaming format
                            openai_chunk = {
                                "id": f"chatcmpl-{int(time.time())}",
                                "object": "chat.completion.chunk",
                                "created": int(time.time()),
                                "model": data.get("model", ""),
                                "choices": [{
                                    "index": 0,
                                    "delta": {
                                        "content": claude_chunk.get("completion", "")
                                    },
                                    "finish_reason": None if not claude_chunk.get("stop_reason") else "stop"
                                }]
                            }
                            
                            yield {
                                "status_code": 200,
                                "chunk": openai_chunk,
                                "duration_ms": int((time.time() - start_time) * 1000)
                            }
                        except json.JSONDecodeError:
                            logger.error(f"Error decoding JSON from stream: {line}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error streaming from Claude API: {str(e)}")
            error_data = {}
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                except:
                    error_data = {"message": str(e)}
                
            yield {
                "status_code": getattr(e.response, 'status_code', 500) if hasattr(e, 'response') else 500,
                "error": error_data,
                "duration_ms": int((time.time() - start_time) * 1000)
            }
    
    def estimate_tokens(self, text: str) -> int:
        """Estimate the number of tokens in the text."""
        # Very rough approximation: ~4 chars per token
        return len(text) // 4 + 1
