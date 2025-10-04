import requests
import json
import time
import logging
from typing import Dict, Any, Optional, List, Generator, Union

logger = logging.getLogger(__name__)

class OpenAIClient:
    """Client for interacting with OpenAI API."""
    
    def __init__(self, api_key: str, api_base: Optional[str] = None):
        self.api_key = api_key
        self.api_base = api_base or "https://api.openai.com/v1"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        })
    
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a request to the OpenAI API."""
        url = f"{self.api_base}/{endpoint}"
        start_time = time.time()
        
        try:
            print(data,url)
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
            logger.error(f"Error making request to OpenAI API: {str(e)}")
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
    
    def chat_completion(self, messages: List[Dict[str, str]], model: str = "gpt-3.5-turbo", 
                      temperature: float = 0.7, max_tokens: Optional[int] = None, 
                      stream: bool = False, **kwargs) -> Dict[str, Any]:
        """Create a chat completion."""
        data = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "stream": stream
        }
        
        if max_tokens is not None:
            data["max_tokens"] = max_tokens
            
        # Add any additional parameters
        for key, value in kwargs.items():
            data[key] = value
            
        if stream:
            return self._stream_chat_completion(data)
        else:
            return self._make_request("POST", "chat/completions", data)
    
    def _stream_chat_completion(self, data: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        """Stream a chat completion."""
        url = f"{self.api_base}/chat/completions"
        start_time = time.time()
        
        try:
            print(data)
            response = self.session.post(url, json=data, stream=True)
            response.raise_for_status()
            print(response)
            for line in response.iter_lines():
                if line:
                    line = line.decode('utf-8')
                    if line.startswith('data: '):
                        line = line[6:]  # Remove 'data: ' prefix
                        if line == "[DONE]":
                            break
                        try:
                            chunk = json.loads(line)
                            yield {
                                "status_code": 200,
                                "chunk": chunk,
                                "duration_ms": int((time.time() - start_time) * 1000)
                            }
                        except json.JSONDecodeError:
                            logger.error(f"Error decoding JSON from stream: {line}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error streaming from OpenAI API: {str(e)}")
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
    
    def text_completion(self, prompt: str, model: str = "text-davinci-003", 
                      temperature: float = 0.7, max_tokens: Optional[int] = None, 
                      stream: bool = False, **kwargs) -> Dict[str, Any]:
        """Create a text completion."""
        data = {
            "model": model,
            "prompt": prompt,
            "temperature": temperature,
            "stream": stream
        }
        
        if max_tokens is not None:
            data["max_tokens"] = max_tokens
            
        # Add any additional parameters
        for key, value in kwargs.items():
            data[key] = value
            
        return self._make_request("POST", "completions", data)
    
    def list_models(self) -> Dict[str, Any]:
        """List available models."""
        return self._make_request("GET", "models")
    
    def estimate_tokens(self, text: str) -> int:
        """Estimate the number of tokens in the text."""
        # Very rough approximation: ~4 chars per token
        return len(text) // 4 + 1
