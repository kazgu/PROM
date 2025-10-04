import logging
import time
from typing import Dict, Any, List, Optional, Tuple, Generator, Union
from django.utils import timezone

from api_proxy.services.mongodb_adapter import (
    external_api_config_adapter, model_mapping_adapter, 
    api_request_adapter, api_key_adapter
)
from api_proxy.services.openai import OpenAIClient
from api_proxy.services.claude import ClaudeClient

logger = logging.getLogger(__name__)

class ModelRouter:
    """Routes API requests to the appropriate LLM provider."""
    
    def __init__(self):
        self.provider_clients = {}
    
    def _get_provider_client(self, provider: Dict[str, Any]) -> Union[OpenAIClient, ClaudeClient, None]:
        """Get or create a client for the specified provider."""
        provider_id = str(provider['id'])
        
        if provider_id in self.provider_clients:
            return self.provider_clients[provider_id]
        
        if provider['api_type'] == 'openai':
            client = OpenAIClient(
                api_key=provider['api_key'],
                api_base=provider.get('api_base')
            )
        elif provider['api_type'] == 'claude':
            client = ClaudeClient(
                api_key=provider['api_key'],
                api_base=provider.get('api_base')
            )
        else:
            logger.error(f"Unsupported provider type: {provider['api_type']}")
            return None
        
        self.provider_clients[provider_id] = client
        return client
    
    def get_provider_for_model(self, model_name: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Find a provider for the requested model."""
        try:
            # First, try to find an exact mapping
            mappings = model_mapping_adapter.filter(
                local_name=model_name,
                is_active=True
            )
            
            # Get all active providers
            active_providers = external_api_config_adapter.filter(is_active=True)
            
            # Create a lookup dictionary for providers by ID
            provider_lookup = {provider['id']: provider for provider in active_providers}
            
            # Filter mappings to only include those with active providers
            valid_mappings = []
            for mapping in mappings:
                provider_id = mapping.get('provider_id')
                if provider_id in provider_lookup:
                    valid_mappings.append((mapping, provider_lookup[provider_id]))
            
            # Sort by provider priority
            valid_mappings.sort(key=lambda x: x[1].get('priority', 100))
            
            if valid_mappings:
                mapping, provider = valid_mappings[0]
                return provider, mapping.get('provider_model_name')
            
            # If no mapping found, check if any provider directly supports this model
            for provider in active_providers:
                if provider['api_type'] == 'openai':
                    return provider, model_name
                elif provider['api_type'] == 'claude' and model_name.startswith('claude-'):
                    return provider, model_name
            
            # No suitable provider found
            logger.warning(f"No provider found for model: {model_name}")
            return None, None
            
        except Exception as e:
            logger.error(f"Error finding provider for model {model_name}: {str(e)}")
            return None, None
    
    def route_chat_completion(self, api_key: Dict[str, Any], request_data: Dict[str, Any], 
                            client_ip: Optional[str] = None) -> Dict[str, Any]:
        """Route a chat completion request to the appropriate provider."""
        model_name = request_data.get('model', 'gpt-3.5-turbo')
        stream = request_data.get('stream', False)
        
        # Create APIRequest record
        api_request = api_request_adapter.create(
            api_key_id=api_key['id'],
            endpoint='chat/completions',
            method='POST',
            request_data=request_data,
            ip_address=client_ip,
            timestamp=timezone.now()
        )
        
        start_time = time.time()
        
        try:
            # Find provider for the requested model
            provider, provider_model = self.get_provider_for_model(model_name)
            
            if not provider:
                error_response = {
                    "status_code": 400,
                    "error": {
                        "message": f"Model '{model_name}' is not supported.",
                        "type": "invalid_request_error"
                    }
                }
                self._update_api_request(api_request, error_response, None, 0)
                return error_response
            
            # Get client for the provider
            client = self._get_provider_client(provider)
            if not client:
                error_response = {
                    "status_code": 500,
                    "error": {
                        "message": "Failed to initialize provider client.",
                        "type": "server_error"
                    }
                }
                self._update_api_request(api_request, error_response, provider, 0)
                return error_response
            
            # Update the model name to the provider's model name
            request_data_copy = request_data.copy()
            request_data_copy['model'] = provider_model
            
            # Make the request to the provider
            if stream:
                return self._handle_streaming_response(
                    client, api_request, provider, provider_model, request_data_copy
                )
            else:
                response = client.chat_completion(**request_data_copy)
                duration_ms = int((time.time() - start_time) * 1000)
                
                # Calculate tokens
                tokens_used = 0
                if response.get('status_code') == 200 and 'response' in response:
                    tokens_used = response['response'].get('usage', {}).get('total_tokens', 0)
                
                # Update the API request with the response
                self._update_api_request(api_request, response, provider, tokens_used)
                
                return response
                
        except Exception as e:
            logger.error(f"Error routing chat completion: {str(e)}")
            duration_ms = int((time.time() - start_time) * 1000)
            error_response = {
                "status_code": 500,
                "error": {
                    "message": f"An error occurred: {str(e)}",
                    "type": "server_error"
                },
                "duration_ms": duration_ms
            }
            
            self._update_api_request(api_request, error_response, provider if 'provider' in locals() else None, 0)
            return error_response
    
    def _handle_streaming_response(self, client, api_request, provider, provider_model, request_data):
        """Handle a streaming response from a provider."""
        # This is a generator function that returns a generator of response chunks
        def stream_response():
            tokens_used = 0
            start_time = time.time()
            
            try:
                for chunk in client.chat_completion(**request_data):
                    yield chunk
                    
                    # Roughly estimate tokens from the chunk
                    if 'chunk' in chunk and 'choices' in chunk['chunk']:
                        for choice in chunk['chunk']['choices']:
                            if 'delta' in choice and 'content' in choice['delta']:
                                content = choice['delta']['content']
                                tokens_used += client.estimate_tokens(content)
                
                # Update the API request with the completed streaming response
                duration_ms = int((time.time() - start_time) * 1000)
                self._update_api_request(api_request, {
                    "status_code": 200,
                    "response": {
                        "model": provider_model,
                        "object": "chat.completion",
                        "usage": {
                            "total_tokens": tokens_used
                        }
                    },
                    "duration_ms": duration_ms
                }, provider, tokens_used)
                
            except Exception as e:
                logger.error(f"Error in streaming response: {str(e)}")
                duration_ms = int((time.time() - start_time) * 1000)
                error_chunk = {
                    "status_code": 500,
                    "error": {
                        "message": f"An error occurred during streaming: {str(e)}",
                        "type": "server_error"
                    },
                    "duration_ms": duration_ms
                }
                
                yield error_chunk
                self._update_api_request(api_request, error_chunk, provider, tokens_used)
        
        return stream_response()
    
    def _update_api_request(self, api_request: Dict[str, Any], response: Dict[str, Any], 
                          provider: Optional[Dict[str, Any]], tokens_used: int) -> None:
        """Update the API request record with the response data."""
        try:
            update_data = {
                'status_code': response.get('status_code', 500),
                'tokens_used': tokens_used,
                'duration_ms': response.get('duration_ms', 0)
            }
            
            if 'error' in response:
                update_data['error'] = str(response['error'])
                update_data['response_data'] = {'error': response['error']}
            else:
                update_data['response_data'] = {'usage': response.get('response', {}).get('usage', {})}
            
            if provider:
                update_data['provider_used_id'] = provider['id']
                model_used = response.get('response', {}).get('model', '')
                update_data['model_used'] = model_used
            
            # Update the API request
            api_request_adapter.update(api_request['id'], **update_data)
            
            # Update the API key's request count and last_used timestamp
            if 'api_key_id' in api_request:
                api_key_id = api_request['api_key_id']
                api_key = api_key_adapter.get(id=api_key_id)
                
                # Increment request count
                request_count = api_key.get('request_count', 0) + 1
                
                api_key_adapter.update(
                    api_key_id,
                    request_count=request_count,
                    last_used=timezone.now()
                )
        
        except Exception as e:
            logger.error(f"Error updating API request record: {str(e)}")
