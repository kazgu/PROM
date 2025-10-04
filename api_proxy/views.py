import json
import logging
import threading
import uuid
from datetime import datetime, timedelta
from django.http import JsonResponse, StreamingHttpResponse, Http404
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.conf import settings
from django.utils import timezone
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status

# Import MongoDB adapters instead of Django models
from api_proxy.services.mongodb_adapter import (
    api_key_adapter, external_api_config_adapter, 
    model_mapping_adapter, model_routing_adapter, 
    api_request_adapter
)
from api_proxy.services.router import ModelRouter
from knowledge_graph.services.extractor import TripleExtractor

logger = logging.getLogger(__name__)
model_router = ModelRouter()
triple_extractor = TripleExtractor()

def get_api_key_from_request(request):
    """Extract and validate the API key from the request."""
    auth_header = request.META.get('HTTP_AUTHORIZATION', '')
    
    if not auth_header.startswith('Bearer '):
        return None
    
    key = auth_header.split(' ')[1].strip()
    
    try:
        # Use MongoDB adapter instead of Django ORM
        api_key = api_key_adapter.filter(key=key, is_active=True)
        if api_key and len(api_key) > 0:
            return api_key[0]
        return None
    except Exception as e:
        logger.error(f"Error getting API key: {str(e)}")
        return None

def get_client_ip(request):
    """Get the client IP address from the request."""
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip

@csrf_exempt
@require_http_methods(["POST"])
def chat_completions(request):
    """OpenAI-compatible chat completions endpoint.""" 
    api_key = get_api_key_from_request(request)
    
    if not api_key:
        return JsonResponse({
            "error": {
                "message": "Invalid API key",
                "type": "invalid_request_error",
                "param": None,
                "code": "invalid_api_key"
            }
        }, status=401)
    
    try:
        request_data = json.loads(request.body)
        client_ip = get_client_ip(request)
        
        # Check if this is a streaming request
        stream = request_data.get('stream', False)
        
        # Route the request to the appropriate provider
        if stream:
            # Handle streaming response
            def generate_response():
                # Collect the complete response for triple extraction
                full_content = ""
                api_request_id = None
                
                for chunk in model_router.route_chat_completion(api_key, request_data, client_ip):
                    if chunk.get('status_code') != 200:
                        # In case of error during streaming
                        yield f"data: {json.dumps(chunk.get('error'))}\n\n"
                        break
                    
                    # Send the chunk in the Server-Sent Events format
                    yield f"data: {json.dumps(chunk.get('chunk'))}\n\n"
                    
                    # Collect content for triple extraction
                    if 'chunk' in chunk and 'choices' in chunk['chunk']:
                        for choice in chunk['chunk']['choices']:
                            if 'delta' in choice and 'content' in choice['delta']:
                                full_content += choice['delta']['content']
                            
                            # Try to get the API request ID if available
                            if api_request_id is None and 'id' in chunk['chunk']:
                                api_request_id = chunk['chunk']['id']
                
                yield "data: [DONE]\n\n"
                
                # After streaming is complete, extract triples from the collected content
                if full_content:
                    # Create a conversation with the original messages and the collected response
                    messages = request_data.get('messages', []).copy()
                    messages.append({
                        'role': 'assistant',
                        'content': full_content
                    })
                    
                    # Extract triples in a background thread
                    threading.Thread(
                        target=extract_triples_from_conversation,
                        args=(messages, api_request_id, api_key)
                    ).start()
            
            response = StreamingHttpResponse(
                generate_response(),
                content_type='text/event-stream'
            )
            return response
        else:
            # Handle regular response
            print(api_key, request_data, client_ip)
            result = model_router.route_chat_completion(api_key, request_data, client_ip)
            
            if result.get('status_code') != 200:
                return JsonResponse(result.get('error', {"message": "Unknown error"}), 
                                   status=result.get('status_code', 500))
            
            # After successful completion, extract triples asynchronously
            if result.get('status_code') == 200 and 'response' in result:
                api_request_id = result.get('response', {}).get('id')
                messages = request_data.get('messages', [])
                
                # Add the assistant response to the messages
                assistant_message = {
                    'role': 'assistant',
                    'content': result['response']['choices'][0]['message']['content']
                }
                messages.append(assistant_message)
                
                # Extract triples in a background thread
                threading.Thread(
                    target=extract_triples_from_conversation,
                    args=(messages, api_request_id, api_key)
                ).start()
            
            return JsonResponse(result.get('response'))
            
    except json.JSONDecodeError:
        return JsonResponse({
            "error": {
                "message": "Invalid request body",
                "type": "invalid_request_error"
            }
        }, status=400)
    except Exception as e:
        logger.exception("Error processing chat completion request")
        return JsonResponse({
            "error": {
                "message": f"An error occurred: {str(e)}",
                "type": "server_error"
            }
        }, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def completions(request):
    """OpenAI-compatible text completions endpoint."""
    # Similar implementation to chat_completions but for text completions
    # For simplicity, we can convert text completion requests to chat completion format
    api_key = get_api_key_from_request(request)
    
    if not api_key:
        return JsonResponse({
            "error": {
                "message": "Invalid API key",
                "type": "invalid_request_error",
                "param": None,
                "code": "invalid_api_key"
            }
        }, status=401)
    
    try:
        request_data = json.loads(request.body)
        client_ip = get_client_ip(request)
        
        # Convert text completion to chat completion format
        prompt = request_data.pop('prompt', '')
        messages = [{"role": "user", "content": prompt}]
        
        chat_request_data = request_data.copy()
        chat_request_data['messages'] = messages
        
        # Route the request as a chat completion
        result = model_router.route_chat_completion(api_key, chat_request_data, client_ip)
        
        if result.get('status_code') != 200:
            return JsonResponse(result.get('error', {"message": "Unknown error"}), 
                               status=result.get('status_code', 500))
                               
        # After successful completion, extract triples asynchronously
        if result.get('status_code') == 200 and 'response' in result:
            api_request_id = result.get('response', {}).get('id')
            # For text completions, create a simple conversation with user prompt and assistant response
            messages = [
                {'role': 'user', 'content': prompt},
                {'role': 'assistant', 'content': result['response']['choices'][0]['text']}
            ]
            
            # Extract triples in a background thread
            threading.Thread(
                target=extract_triples_from_conversation,
                args=(messages, api_request_id, api_key)
            ).start()
        
        # Convert the chat completion response back to a text completion format
        chat_response = result.get('response', {})
        choices = chat_response.get('choices', [])
        
        text_completion_response = {
            "id": chat_response.get('id', ''),
            "object": "text_completion",
            "created": chat_response.get('created', int(timezone.now().timestamp())),
            "model": chat_response.get('model', ''),
            "choices": [{
                "text": choice.get('message', {}).get('content', ''),
                "index": choice.get('index', 0),
                "logprobs": None,
                "finish_reason": choice.get('finish_reason', 'stop')
            } for choice in choices],
            "usage": chat_response.get('usage', {})
        }
        
        return JsonResponse(text_completion_response)
        
    except json.JSONDecodeError:
        return JsonResponse({
            "error": {
                "message": "Invalid request body",
                "type": "invalid_request_error"
            }
        }, status=400)
    except Exception as e:
        logger.exception("Error processing text completion request")
        return JsonResponse({
            "error": {
                "message": f"An error occurred: {str(e)}",
                "type": "server_error"
            }
        }, status=500)

@csrf_exempt
@require_http_methods(["GET"])
def list_models(request):
    """OpenAI-compatible models endpoint."""
    # For GET requests to the models endpoint, we don't require authentication
    # This is consistent with how the OpenAI API works
    
    # Query available models from database
    models_data = []
    
    # First, get all active model mappings
    mappings = model_mapping_adapter.filter(is_active=True)
    active_providers = external_api_config_adapter.filter(is_active=True)
    active_provider_ids = [p['id'] for p in active_providers]
    
    # Filter mappings to only include those with active providers
    active_mappings = [m for m in mappings if m.get('provider_id') in active_provider_ids]
    
    for mapping in active_mappings:
        # Get provider details
        try:
            provider = external_api_config_adapter.get(id=mapping['provider_id'])
            # Use creation timestamp of provider as model 'created' timestamp
            created_timestamp = int(datetime.fromisoformat(str(provider['created_at'])).timestamp())
            owned_by = provider['name'] if provider['api_type'] == 'other' else provider['api_type']
            
            models_data.append({
                "id": mapping['local_name'],
                "object": "model",
                "created": created_timestamp,
                "owned_by": owned_by
            })
        except Http404:
            logger.warning(f"Provider {mapping['provider_id']} not found for mapping {mapping['id']}")
    
    # Next, add standard models supported by providers
    standard_models = {
        'openai': [
            {"id": "gpt-3.5-turbo", "created": 1677610602, "owned_by": "openai"},
            {"id": "gpt-4", "created": 1687882411, "owned_by": "openai"}
        ],
        'claude': [
            {"id": "claude-2.1", "created": 1699083251, "owned_by": "anthropic"},
        ]
    }
    
    # Only add standard models if we have an active provider for them
    for provider in active_providers:
        if provider['api_type'] in standard_models:
            for model in standard_models[provider['api_type']]:
                # Check if this model is already included from mappings
                if not any(m['id'] == model['id'] for m in models_data):
                    models_data.append({
                        "id": model['id'],
                        "object": "model",
                        "created": model['created'],
                        "owned_by": model['owned_by']
                    })
    
    return JsonResponse({
        "object": "list",
        "data": models_data
    })

def extract_triples_from_conversation(messages, api_request_id=None, api_key=None):
    """Extract knowledge triples from a conversation in a background thread."""
    try:
        # Generate a valid UUID for the extraction session if api_request_id is not a valid UUID
        extraction_id = api_request_id
        if api_request_id:
            try:
                uuid.UUID(str(api_request_id))
            except (ValueError, TypeError, AttributeError):
                # If not a valid UUID, generate a new one based on the API request ID
                extraction_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, str(api_request_id)))
                logger.info(f"Generated new UUID {extraction_id} for extraction from non-UUID request ID: {api_request_id}")
        
        # If api_key is not provided, try to get it from the API request
        if not api_key and api_request_id:
            try:
                # Use MongoDB adapter instead of Django ORM
                api_request = api_request_adapter.get(id=api_request_id)
                if 'api_key_id' in api_request:
                    api_key_id = api_request['api_key_id']
                    api_key = api_key_adapter.get(id=api_key_id)
                    logger.info(f"Using API key {api_key['id']} for triple extraction")
            except Http404:
                logger.warning(f"Could not find API request with ID {api_request_id}")
            except Exception as e:
                logger.warning(f"Error getting API request: {str(e)}")
        
        logger.info(f"Extracting triples from conversation with extraction ID: {extraction_id}")
        triples = triple_extractor.extract_from_conversation(messages, extraction_id, api_key)
        logger.info(f"Extracted {len(triples)} triples from conversation")
    except Exception as e:
        logger.error(f"Error extracting triples: {str(e)}")

# API Key management views for authenticated users
@api_view(['POST'])
@permission_classes([IsAuthenticated])
def create_api_key(request):
    """Create a new API key for the authenticated user."""
    name = request.data.get('name', f"API Key - {timezone.now().strftime('%Y-%m-%d %H:%M')}")
    
    # Use MongoDB adapter instead of Django ORM
    api_key = api_key_adapter.create(name=name)
    
    return Response({
        "id": api_key['id'],
        "key": api_key['key'],  # Only shown once upon creation
        "name": api_key['name'],
        "created_at": api_key['created_at']
    }, status=status.HTTP_201_CREATED)

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def list_api_keys(request):
    """List all API keys for the authenticated user."""
    # Use MongoDB adapter instead of Django ORM
    api_keys = api_key_adapter.all()
    
    return Response([{
        "id": key['id'],
        "name": key['name'],
        "key": key['key'],
        "created_at": key['created_at'],
        "last_used": key.get('last_used'),
        "request_count": key.get('request_count', 0),
        "is_active": key.get('is_active', True)
    } for key in api_keys])

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_api_key(request, key_id):
    """Get details for a specific API key."""
    try:
        # Use MongoDB adapter instead of Django ORM
        api_key = api_key_adapter.get(id=key_id)
        
        # Get API requests for this key
        api_requests = api_request_adapter.filter(api_key_id=key_id)
        
        # Calculate total tokens and estimated cost
        total_tokens = sum(req.get('tokens_used', 0) for req in api_requests)
        
        # Calculate estimated cost based on tokens
        # Average cost per 1000 tokens: $0.002
        estimated_cost = (total_tokens / 1000) * 0.002
         
        return Response({
            "id": api_key['id'],
            "name": api_key['name'],
            "key_full": api_key['key'], 
            "key": api_key['key'][:12] + "..." if api_key['key'] else None,  # Show only beginning of key for security
            "created_at": api_key['created_at'],
            "expires_at": None,  # Add expiration if implemented
            "last_used": api_key.get('last_used'),
            "request_count": api_key.get('request_count', 0),
            "is_active": api_key.get('is_active', True),
            "allowed_models": api_key.get('allowed_models', []),
            "total_tokens": total_tokens,
            "estimated_cost": estimated_cost,
            "request_limit": None  # Add request limit if implemented
        })  
    except Http404:
        return Response({"error": "API key not found"}, status=status.HTTP_404_NOT_FOUND)

@api_view(['DELETE'])
@permission_classes([IsAuthenticated])
def revoke_api_key(request, key_id):
    """Revoke an API key."""
    try:
        # Use MongoDB adapter instead of Django ORM
        api_key = api_key_adapter.get(id=key_id)
        api_key_adapter.update(key_id, is_active=False)
        
        return Response(status=status.HTTP_204_NO_CONTENT)
    except Http404:
        return Response({"error": "API key not found"}, status=status.HTTP_404_NOT_FOUND)

# External API config (model) management views
@api_view(['GET'])
@permission_classes([IsAuthenticated])
def list_external_api_configs(request):
    """List all external API configurations."""
    # Use MongoDB adapter instead of Django ORM
    configs = external_api_config_adapter.all()
    
    # Sort by name
    configs = sorted(configs, key=lambda x: x.get('name', ''))
    
    return Response([{
        "id": config['id'],
        "name": config['name'],
        "api_type": config['api_type'],
        "is_active": config.get('is_active', True),
        "priority": config.get('priority', 100),
        "created_at": config.get('created_at'),
        "updated_at": config.get('updated_at'),
        # Don't return the actual API key for security
        "has_api_key": bool(config.get('api_key')),
        "api_base": config.get('api_base')
    } for config in configs])

@api_view(['POST'])
@permission_classes([IsAuthenticated])
def create_external_api_config(request):
    """Create a new external API configuration."""
    try:
        name = request.data.get('name')
        api_type = request.data.get('api_type')
        api_key = request.data.get('api_key')
        api_base = request.data.get('api_base')
        priority = request.data.get('priority', 100)
        is_active = request.data.get('is_active', True)
        
        # Parse additional config from JSON if provided
        additional_config = request.data.get('additional_config', '{}')
        if isinstance(additional_config, str):
            try:
                config_dict = json.loads(additional_config)
            except json.JSONDecodeError:
                config_dict = {}
        else:
            config_dict = additional_config
        
        # Validate required fields
        if not name or not api_type or not api_key:
            return Response({
                "error": "Missing required fields: name, api_type, and api_key are required"
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Create the API config using MongoDB adapter
        api_config = external_api_config_adapter.create(
            name=name,
            api_type=api_type,
            api_key=api_key,
            api_base=api_base,
            priority=priority,
            is_active=is_active,
            config=config_dict
        )
        
        return Response({
            "id": api_config['id'],
            "name": api_config['name'],
            "api_type": api_config['api_type'],
            "is_active": api_config.get('is_active', True),
            "priority": api_config.get('priority', 100),
            "created_at": api_config['created_at']
        }, status=status.HTTP_201_CREATED)
        
    except Exception as e:
        logger.exception("Error creating external API config")
        return Response({
            "error": f"An error occurred: {str(e)}"
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_external_api_config(request, config_id):
    """Get a specific external API configuration."""
    try:
        # Use MongoDB adapter instead of Django ORM
        config = external_api_config_adapter.get(id=config_id)
        
        return Response({
            "id": config['id'],
            "name": config['name'],
            "api_type": config['api_type'],
            "is_active": config.get('is_active', True),
            "priority": config.get('priority', 100),
            "created_at": config.get('created_at'),
            "updated_at": config.get('updated_at'),
            # Don't return the actual API key for security
            "has_api_key": bool(config.get('api_key')),
            "api_base": config.get('api_base'),
            "config": config.get('config', {})
        })
    except Http404:
        return Response({"error": "API configuration not found"}, status=status.HTTP_404_NOT_FOUND)

@api_view(['PUT'])
@permission_classes([IsAuthenticated])
def update_external_api_config(request, config_id):
    """Update an external API configuration."""
    try:
        # Use MongoDB adapter instead of Django ORM
        config = external_api_config_adapter.get(id=config_id)
        
        # Prepare update data
        update_data = {}
        
        # Update fields if provided
        if 'name' in request.data:
            update_data['name'] = request.data['name']
        
        if 'api_type' in request.data:
            update_data['api_type'] = request.data['api_type']
        
        if 'api_key' in request.data and request.data['api_key']:
            update_data['api_key'] = request.data['api_key']
        
        if 'api_base' in request.data:
            update_data['api_base'] = request.data['api_base']
        
        if 'priority' in request.data:
            update_data['priority'] = request.data['priority']
        
        if 'is_active' in request.data:
            update_data['is_active'] = request.data['is_active']
        
        # Parse additional config from JSON if provided
        if 'additional_config' in request.data:
            additional_config = request.data.get('additional_config', '{}')
            if isinstance(additional_config, str):
                try:
                    config_dict = json.loads(additional_config)
                    update_data['config'] = config_dict
                except json.JSONDecodeError:
                    pass
            else:
                update_data['config'] = additional_config
        
        # Update the config
        updated_config = external_api_config_adapter.update(config_id, **update_data)
        
        return Response({
            "id": updated_config['id'],
            "name": updated_config['name'],
            "api_type": updated_config['api_type'],
            "is_active": updated_config.get('is_active', True),
            "priority": updated_config.get('priority', 100),
            "updated_at": updated_config.get('updated_at')
        })
    except Http404:
        return Response({"error": "API configuration not found"}, status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        logger.exception("Error updating external API config")
        return Response({
            "error": f"An error occurred: {str(e)}"
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['DELETE'])
@permission_classes([IsAuthenticated])
def delete_external_api_config(request, config_id):
    """Delete an external API configuration."""
    try:
        # Use MongoDB adapter instead of Django ORM
        config = external_api_config_adapter.get(id=config_id)
        external_api_config_adapter.delete(config_id)
        
        return Response(status=status.HTTP_204_NO_CONTENT)
    except Http404:
        return Response({"error": "API configuration not found"}, status=status.HTTP_404_NOT_FOUND)

# Model routing rules management views
@api_view(['GET'])
@permission_classes([IsAuthenticated])
def list_model_routing_rules(request):
    """List all model routing rules."""
    # Use MongoDB adapter instead of Django ORM
    rules = model_routing_adapter.all()
    
    # Sort by priority and created_at
    rules = sorted(rules, key=lambda x: (x.get('priority', 10), x.get('created_at', '')))
    
    # For each rule, get the target model details
    result = []
    for rule in rules:
        try:
            # Get target model details
            target_model_id = rule.get('target_model_id')
            target_model = external_api_config_adapter.get(id=target_model_id)
            
            result.append({
                "id": rule['id'],
                "name": rule['name'],
                "condition_type": rule['condition_type'],
                "condition_value": rule['condition_value'],
                "target_model": target_model_id,
                "target_model_name": target_model['name'],
                "priority": rule.get('priority', 10),
                "is_active": rule.get('is_active', True),
                "created_at": rule.get('created_at'),
                "updated_at": rule.get('updated_at')
            })
        except Http404:
            # Skip rules with missing target models
            logger.warning(f"Target model {target_model_id} not found for rule {rule['id']}")
            continue
    
    return Response(result)

@api_view(['POST'])
@permission_classes([IsAuthenticated])
def create_model_routing_rule(request):
    """Create a new model routing rule."""
    try:
        name = request.data.get('name')
        condition_type = request.data.get('condition_type')
        condition_value = request.data.get('condition_value')
        target_model_id = request.data.get('target_model')
        priority = request.data.get('priority', 10)
        is_active = request.data.get('is_active', True)
        
        # Validate required fields
        if not all([name, condition_type, condition_value, target_model_id]):
            return Response({
                "error": "Missing required fields"
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Parse condition value from JSON if provided as string
        if isinstance(condition_value, str):
            try:
                condition_value = json.loads(condition_value)
            except json.JSONDecodeError:
                return Response({
                    "error": "Invalid condition value format"
                }, status=status.HTTP_400_BAD_REQUEST)
        
        # Get target model
        try:
            # Use MongoDB adapter instead of Django ORM
            target_model = external_api_config_adapter.get(id=target_model_id)
        except Http404:
            return Response({
                "error": "Target model not found"
            }, status=status.HTTP_404_NOT_FOUND)
        
        # Create routing rule using MongoDB adapter
        rule = model_routing_adapter.create(
            name=name,
            condition_type=condition_type,
            condition_value=condition_value,
            target_model_id=target_model_id,
            priority=priority,
            is_active=is_active
        )
        
        return Response({
            "id": rule['id'],
            "name": rule['name'],
            "condition_type": rule['condition_type'],
            "condition_value": rule['condition_value'],
            "target_model": target_model_id,
            "target_model_name": target_model['name'],
            "priority": rule.get('priority', 10),
            "is_active": rule.get('is_active', True),
            "created_at": rule.get('created_at')
        }, status=status.HTTP_201_CREATED)
        
    except Exception as e:
        logger.exception("Error creating model routing rule")
        return Response({
            "error": f"An error occurred: {str(e)}"
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_model_routing_rule(request, rule_id):
    """Get a specific model routing rule."""
    try:
        # Use MongoDB adapter instead of Django ORM
        rule = model_routing_adapter.get(id=rule_id)
        
        # Get target model details
        target_model_id = rule.get('target_model_id')
        target_model = external_api_config_adapter.get(id=target_model_id)
        
        return Response({
            "id": rule['id'],
            "name": rule['name'],
            "condition_type": rule['condition_type'],
            "condition_value": rule['condition_value'],
            "target_model": target_model_id,
            "target_model_name": target_model['name'],
            "priority": rule.get('priority', 10),
            "is_active": rule.get('is_active', True),
            "created_at": rule.get('created_at'),
            "updated_at": rule.get('updated_at')
        })
    except Http404:
        return Response({"error": "Routing rule not found"}, status=status.HTTP_404_NOT_FOUND)

@api_view(['PUT'])
@permission_classes([IsAuthenticated])
def update_model_routing_rule(request, rule_id):
    """Update a model routing rule."""
    try:
        # Use MongoDB adapter instead of Django ORM
        rule = model_routing_adapter.get(id=rule_id)
        
        # Prepare update data
        update_data = {}
        
        # Update fields if provided
        if 'name' in request.data:
            update_data['name'] = request.data['name']
        
        if 'condition_type' in request.data:
            update_data['condition_type'] = request.data['condition_type']
        
        if 'condition_value' in request.data:
            condition_value = request.data['condition_value']
            if isinstance(condition_value, str):
                try:
                    condition_value = json.loads(condition_value)
                except json.JSONDecodeError:
                    return Response({
                        "error": "Invalid condition value format"
                    }, status=status.HTTP_400_BAD_REQUEST)
            update_data['condition_value'] = condition_value
        
        if 'target_model' in request.data:
            target_model_id = request.data['target_model']
            try:
                # Verify target model exists
                target_model = external_api_config_adapter.get(id=target_model_id)
                update_data['target_model_id'] = target_model_id
            except Http404:
                return Response({
                    "error": "Target model not found"
                }, status=status.HTTP_404_NOT_FOUND)
        
        if 'priority' in request.data:
            update_data['priority'] = request.data['priority']
        
        if 'is_active' in request.data:
            update_data['is_active'] = request.data['is_active']
        
        # Update the rule
        updated_rule = model_routing_adapter.update(rule_id, **update_data)
        
        # Get target model details for response
        target_model_id = updated_rule.get('target_model_id')
        target_model = external_api_config_adapter.get(id=target_model_id)
        
        return Response({
            "id": updated_rule['id'],
            "name": updated_rule['name'],
            "condition_type": updated_rule['condition_type'],
            "condition_value": updated_rule['condition_value'],
            "target_model": target_model_id,
            "target_model_name": target_model['name'],
            "priority": updated_rule.get('priority', 10),
            "is_active": updated_rule.get('is_active', True),
            "updated_at": updated_rule.get('updated_at')
        })
    except Http404:
        return Response({"error": "Routing rule not found"}, status=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        logger.exception("Error updating model routing rule")
        return Response({
            "error": f"An error occurred: {str(e)}"
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['DELETE'])
@permission_classes([IsAuthenticated])
def delete_model_routing_rule(request, rule_id):
    """Delete a model routing rule."""
    try:
        # Use MongoDB adapter instead of Django ORM
        rule = model_routing_adapter.get(id=rule_id)
        model_routing_adapter.delete(rule_id)
        
        return Response(status=status.HTTP_204_NO_CONTENT)
    except Http404:
        return Response({"error": "Routing rule not found"}, status=status.HTTP_404_NOT_FOUND)

# Usage statistics API
@api_view(['GET'])
@permission_classes([IsAuthenticated])
def usage_statistics(request):
    """Get detailed usage statistics with filtering options."""
    # Parse filter parameters
    api_key_id = request.query_params.get('api_key')
    model = request.query_params.get('model')
    start_date = request.query_params.get('start_date')
    end_date = request.query_params.get('end_date')
    group_by = request.query_params.get('group_by', 'day')  # day, week, month
    
    # Build filter criteria for MongoDB
    filter_criteria = {}
    
    if api_key_id:
        filter_criteria['api_key_id'] = api_key_id
    
    if model:
        filter_criteria['model_used'] = model
    
    if start_date:
        filter_criteria['timestamp'] = {'$gte': start_date}
    
    if end_date:
        if 'timestamp' in filter_criteria:
            filter_criteria['timestamp']['$lte'] = end_date
        else:
            filter_criteria['timestamp'] = {'$lte': end_date}
    
    # Get all API requests matching the filter criteria
    api_requests = api_request_adapter.filter(**filter_criteria)
    
    # Get statistics by API key
    api_key_stats = []
    
    if api_key_id:
        # Detailed stats for a single API key
        try:
            api_key = api_key_adapter.get(id=api_key_id)
            
            # Filter requests for this API key
            key_requests = [req for req in api_requests if req.get('api_key_id') == api_key_id]
            
            # Calculate total tokens and requests
            total_tokens = sum(req.get('tokens_used', 0) for req in key_requests)
            total_requests = len(key_requests)
            
            # Calculate estimated cost
            estimated_cost = (total_tokens / 1000) * 0.002
            
            # Get model breakdown
            model_usage = {}
            for req in key_requests:
                model = req.get('model_used')
                if model:
                    if model not in model_usage:
                        model_usage[model] = {'request_count': 0, 'token_count': 0}
                    model_usage[model]['request_count'] += 1
                    model_usage[model]['token_count'] += req.get('tokens_used', 0)
            
            # Convert to list and sort by request count
            model_breakdown = [
                {'model_used': model, 'request_count': stats['request_count'], 'token_count': stats['token_count']}
                for model, stats in model_usage.items()
            ]
            model_breakdown.sort(key=lambda x: x['request_count'], reverse=True)
            
            api_key_stats.append({
                'id': api_key['id'],
                'name': api_key['name'],
                'total_requests': total_requests,
                'total_tokens': total_tokens,
                'estimated_cost': estimated_cost,
                'model_breakdown': model_breakdown
            })
        except Http404:
            logger.warning(f"API key {api_key_id} not found")
    else:
        # Stats for all API keys
        api_keys = api_key_adapter.all()
        
        for api_key in api_keys:
            key_id = api_key['id']
            
            # Filter requests for this API key
            key_requests = [req for req in api_requests if req.get('api_key_id') == key_id]
            
            # Skip if no requests
            if not key_requests:
                continue
            
            # Calculate total tokens and requests
            total_tokens = sum(req.get('tokens_used', 0) for req in key_requests)
            total_requests = len(key_requests)
            
            # Calculate estimated cost
            estimated_cost = (total_tokens / 1000) * 0.002
            
            api_key_stats.append({
                'id': key_id,
                'name': api_key['name'],
                'total_requests': total_requests,
                'total_tokens': total_tokens,
                'estimated_cost': estimated_cost
            })
    
    # Get time series data
    time_series = []
    
    if api_requests:
        # Group requests by date
        date_groups = {}
        
        for req in api_requests:
            timestamp = req.get('timestamp')
            if not timestamp:
                continue
                
            # Parse timestamp
            if isinstance(timestamp, str):
                try:
                    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except ValueError:
                    continue
            
            # Truncate timestamp based on group_by
            if group_by == 'week':
                # Get the start of the week (Monday)
                date_key = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
                date_key = date_key - datetime.timedelta(days=date_key.weekday())
            elif group_by == 'month':
                # Get the start of the month
                date_key = timestamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            else:  # default to day
                # Get the start of the day
                date_key = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
            
            date_key_str = date_key.isoformat()
            
            if date_key_str not in date_groups:
                date_groups[date_key_str] = {'request_count': 0, 'token_count': 0}
            
            date_groups[date_key_str]['request_count'] += 1
            date_groups[date_key_str]['token_count'] += req.get('tokens_used', 0)
        
        # Convert to list and sort by date
        time_series = [
            {'date': date, 'request_count': stats['request_count'], 'token_count': stats['token_count']}
            for date, stats in date_groups.items()
        ]
        time_series.sort(key=lambda x: x['date'])
    
    # Get model usage stats
    model_stats = []
    
    if api_requests:
        # Group requests by model
        model_groups = {}
        
        for req in api_requests:
            model = req.get('model_used')
            if not model:
                continue
            
            if model not in model_groups:
                model_groups[model] = {'request_count': 0, 'token_count': 0}
            
            model_groups[model]['request_count'] += 1
            model_groups[model]['token_count'] += req.get('tokens_used', 0)
        
        # Convert to list and sort by request count
        model_stats = [
            {'model_used': model, 'request_count': stats['request_count'], 'token_count': stats['token_count']}
            for model, stats in model_groups.items()
        ]
        model_stats.sort(key=lambda x: x['request_count'], reverse=True)
    
    # Calculate totals
    total_requests = len(api_requests)
    total_tokens = sum(req.get('tokens_used', 0) for req in api_requests)
    estimated_total_cost = (total_tokens / 1000) * 0.002
    
    # Prepare response
    return Response({
        'api_key_stats': api_key_stats,
        'time_series': time_series,
        'model_stats': model_stats,
        'total_requests': total_requests,
        'total_tokens': total_tokens,
        'estimated_total_cost': estimated_total_cost
    })

# Request logs API
@api_view(['GET'])
@permission_classes([IsAuthenticated])
def list_api_requests(request):
    """List API request logs with filtering options."""
    # Parse filter parameters
    api_key_id = request.query_params.get('api_key')
    model = request.query_params.get('model')
    status_code = request.query_params.get('status')
    start_date = request.query_params.get('start_date')
    end_date = request.query_params.get('end_date')
    search = request.query_params.get('search')
    
    # Build filter criteria for MongoDB
    filter_criteria = {}
    
    if api_key_id:
        filter_criteria['api_key_id'] = api_key_id
    
    if model:
        filter_criteria['model_used'] = model
    
    if status_code:
        if status_code == 'success':
            filter_criteria['status_code'] = {'$gte': 200, '$lt': 300}
        elif status_code == 'error':
            filter_criteria['status_code'] = {'$gte': 400}
    
    if start_date:
        if 'timestamp' not in filter_criteria:
            filter_criteria['timestamp'] = {}
        filter_criteria['timestamp']['$gte'] = start_date
    
    if end_date:
        if 'timestamp' not in filter_criteria:
            filter_criteria['timestamp'] = {}
        filter_criteria['timestamp']['$lte'] = end_date
    
    if search:
        # Search in request data (this might be inefficient on large datasets)
        filter_criteria['request_data'] = {'$regex': search}
    
    # Get all requests matching the filter criteria
    all_requests = api_request_adapter.filter(**filter_criteria)
    
    # Sort by timestamp (descending)
    all_requests = sorted(all_requests, key=lambda x: x.get('timestamp', ''), reverse=True)
    
    # Pagination
    page = int(request.query_params.get('page', 1))
    page_size = int(request.query_params.get('page_size', 20))
    
    start = (page - 1) * page_size
    end = start + page_size
    
    total_count = len(all_requests)
    requests_page = all_requests[start:end] if start < total_count else []
    
    # Prepare response
    results = []
    for req in requests_page:
        # Get API key details
        api_key = None
        api_key_name = None
        if 'api_key_id' in req:
            try:
                api_key_obj = api_key_adapter.get(id=req['api_key_id'])
                api_key = api_key_obj.get('key')
                api_key_name = api_key_obj.get('name')
            except Http404:
                pass
        
        # Get provider details
        provider_id = req.get('provider_used_id')
        provider_name = None
        if provider_id:
            try:
                provider = external_api_config_adapter.get(id=provider_id)
                provider_name = provider.get('name')
            except Http404:
                pass
        
        results.append({
            "id": req.get('id'),
            "timestamp": req.get('timestamp'),
            "api_key": api_key,
            "api_key_name": api_key_name,
            "endpoint": req.get('endpoint'),
            "model_used": req.get('model_used'),
            "provider_used": provider_id,
            "provider_name": provider_name,
            "tokens_used": req.get('tokens_used', 0),
            "duration_ms": req.get('duration_ms', 0),
            "status_code": req.get('status_code'),
            "has_error": bool(req.get('error'))
        })
    
    return Response({
        "total": total_count,
        "page": page,
        "page_size": page_size,
        "results": results
    })

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_api_request_details(request, request_id):
    """Get detailed information about a specific API request."""
    try:
        # Use MongoDB adapter instead of Django ORM
        api_request = api_request_adapter.get(id=request_id)
        
        # Get API key details
        api_key = None
        api_key_name = None
        if 'api_key_id' in api_request:
            try:
                api_key_obj = api_key_adapter.get(id=api_request['api_key_id'])
                api_key = api_key_obj.get('key')
                api_key_name = api_key_obj.get('name')
            except Http404:
                pass
        
        # Get provider details
        provider_id = api_request.get('provider_used_id')
        provider_name = None
        if provider_id:
            try:
                provider = external_api_config_adapter.get(id=provider_id)
                provider_name = provider.get('name')
            except Http404:
                pass
        
        return Response({
            "id": api_request.get('id'),
            "timestamp": api_request.get('timestamp'),
            "api_key": api_key,
            "api_key_name": api_key_name,
            "endpoint": api_request.get('endpoint'),
            "method": api_request.get('method'),
            "request_data": api_request.get('request_data'),
            "response_data": api_request.get('response_data'),
            "status_code": api_request.get('status_code'),
            "model_used": api_request.get('model_used'),
            "provider_used": provider_id,
            "provider_name": provider_name,
            "tokens_used": api_request.get('tokens_used', 0),
            "duration_ms": api_request.get('duration_ms', 0),
            "ip_address": api_request.get('ip_address'),
            "error": api_request.get('error')
        })
    except Http404:
        return Response({"error": "API request not found"}, status=status.HTTP_404_NOT_FOUND)
