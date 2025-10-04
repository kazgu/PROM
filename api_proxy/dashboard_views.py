import logging
import csv
import io
from datetime import datetime, timedelta
from django.utils import timezone
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from api_proxy.services.mongodb_adapter import (
    api_request_adapter, external_api_config_adapter, api_key_adapter
)
from knowledge_graph.services.analytics import GraphAnalytics

logger = logging.getLogger(__name__)

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def dashboard_stats(request):
    """
    Get statistics for the dashboard.
    Returns usage data, model usage breakdown, and other stats.
    """
    # Calculate time periods
    now = timezone.now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    past_days = []
    
    # Generate past 7 days for API usage chart
    for i in range(6, -1, -1):
        day = now - timedelta(days=i)
        day_start = day.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        past_days.append((day_start, day_end, day.date()))
    
    # Get API usage per day
    api_usage = []
    for day_start, day_end, date in past_days:
        # Count requests for this day
        requests = api_request_adapter.filter(
            timestamp__gte=day_start.isoformat(),
            timestamp__lt=day_end.isoformat()
        )
        request_count = len(requests)
        
        api_usage.append({
            'date': date.isoformat(),
            'count': request_count
        })
    
    # Get model usage stats
    all_requests = api_request_adapter.all()
    model_counts = {}
    
    for req in all_requests:
        model = req.get('model_used')
        if model:
            if model not in model_counts:
                model_counts[model] = 0
            model_counts[model] += 1
    
    # Sort by count and get top 6
    model_usage = []
    for model, count in sorted(model_counts.items(), key=lambda x: x[1], reverse=True)[:6]:
        model_usage.append({
            'model_name': model,
            'request_count': count
        })
    
    # Get other stats
    today_requests = len(api_request_adapter.filter(
        timestamp__gte=today_start.isoformat()
    ))
    
    # Get month requests
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_requests = len(api_request_adapter.filter(
        timestamp__gte=month_start.isoformat()
    ))
    
    # Get token usage
    total_tokens = sum(req.get('tokens_used', 0) for req in all_requests)
    
    # Calculate estimated cost (assuming average cost of $0.002 per 1000 tokens)
    estimated_cost = (total_tokens / 1000) * 0.002
    
    # Get graph stats from knowledge graph
    analytics = GraphAnalytics()
    graph_stats = analytics.get_graph_statistics()
    
    # Format response
    response = {
        'api_usage': api_usage,
        'model_usage': model_usage,
        'today_requests': today_requests,
        'month_requests': month_requests,
        'total_tokens': total_tokens,
        'estimated_cost': estimated_cost,
        'graph_stats': graph_stats
    }
    
    return Response(response)

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def recent_activity(request):
    """
    Get recent activity for the dashboard.
    Returns a list of recent API requests and actions.
    """
    limit = int(request.query_params.get('limit', 10))
    
    # Get recent API requests
    all_requests = api_request_adapter.all()
    
    # Sort by timestamp (descending)
    recent_requests = sorted(
        all_requests, 
        key=lambda x: x.get('timestamp', ''), 
        reverse=True
    )[:limit]
    
    activities = []
    for req in recent_requests:
        activity_type = 'error' if req.get('error') else 'api_request'
        
        # Create a readable description
        endpoint = req.get('endpoint', '')
        if endpoint.endswith('completions'):
            action = f"API 请求: {endpoint}"
            description = f"模型: {req.get('model_used') or '未知'}, 状态: {req.get('status_code') or 'N/A'}"
        else:
            action = f"API 请求: {endpoint}"
            description = f"状态: {req.get('status_code') or 'N/A'}"
        
        activities.append({
            'timestamp': req.get('timestamp'),
            'type': activity_type,
            'action': action,
            'description': description
        })
    
    return Response(activities)

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def system_status(request):
    """
    Get system service status.
    Returns the status of various system components.
    """
    status = {
        'neo4j': check_neo4j_status(),
        'redis': check_redis_status(),
        'openai': check_openai_status(),
        'claude': check_claude_status(),
        'mongodb': check_mongodb_status()
    }
    
    return Response(status)

def check_neo4j_status():
    """Check Neo4j connection status."""
    try:
        from knowledge_graph.services.graph_db import Neo4jGraphDB
        client = Neo4jGraphDB()
        # Test connection by running a simple query
        with client.driver.session() as session:
            result = session.run("RETURN 1 as test")
            if result.single()['test'] == 1:
                return 'ok'
            return 'error'
    except Exception as e:
        logger.error(f"Neo4j status check failed: {str(e)}")
        return 'error'

def check_redis_status():
    """Check Redis connection status."""
    try:
        from django.core.cache import cache
        cache.set('status_test', 'ok', timeout=10)
        status = cache.get('status_test')
        return 'ok' if status == 'ok' else 'error'
    except Exception as e:
        logger.error(f"Redis status check failed: {str(e)}")
        return 'error'

def check_mongodb_status():
    """Check MongoDB connection status."""
    try:
        from knowledge_graph.services.mongodb_service import MongoDBService
        mongo_service = MongoDBService()
        # Test connection by getting a collection
        collection = mongo_service.get_collection('entities')
        # Try to find one document
        collection.find_one({})
        return 'ok'
    except Exception as e:
        logger.error(f"MongoDB status check failed: {str(e)}")
        return 'error'

def check_openai_status():
    """Check OpenAI API status."""
    try:
        # Check if we have active OpenAI config
        openai_configs = external_api_config_adapter.filter(
            api_type='openai', 
            is_active=True
        )
        
        if not openai_configs:
            return 'unknown'  # No configs to check
        
        # In a real implementation, you might make a test request
        # to the OpenAI API here
        return 'ok'
    except Exception as e:
        logger.error(f"OpenAI status check failed: {str(e)}")
        return 'error'

def check_claude_status():
    """Check Claude API status."""
    try:
        # Check if we have active Claude config
        claude_configs = external_api_config_adapter.filter(
            api_type='claude', 
            is_active=True
        )
        
        if not claude_configs:
            return 'unknown'  # No configs to check
        
        # In a real implementation, you might make a test request
        # to the Claude API here
        return 'ok'
    except Exception as e:
        logger.error(f"Claude status check failed: {str(e)}")
        return 'error'

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def detailed_usage_stats(request):
    """
    Get detailed usage statistics for the dashboard.
    Supports filtering by date range and includes various breakdowns.
    Returns data for charts and stats visualization.
    """
    # Parse date range parameters
    start_date_str = request.query_params.get('start_date')
    end_date_str = request.query_params.get('end_date')
    format_as_csv = request.query_params.get('format') == 'csv'
    
    # Default to last 7 days if no dates provided
    now = timezone.now()
    if end_date_str:
        try:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(
                hour=23, minute=59, second=59, microsecond=999999
            )
            end_date = timezone.make_aware(end_date)
        except ValueError:
            end_date = now
    else:
        end_date = now
    
    if start_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            start_date = timezone.make_aware(start_date)
        except ValueError:
            start_date = end_date - timedelta(days=7)
    else:
        start_date = end_date - timedelta(days=7)
    
    # Get API requests within date range
    all_requests = api_request_adapter.all()
    
    # Filter requests by date range
    requests = []
    for req in all_requests:
        timestamp_str = req.get('timestamp')
        if not timestamp_str:
            continue
        
        try:
            if isinstance(timestamp_str, str):
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                if start_date <= timestamp <= end_date:
                    requests.append(req)
            else:
                # Assume it's already a datetime object
                if start_date <= timestamp_str <= end_date:
                    requests.append(req)
        except (ValueError, TypeError):
            continue
    
    if format_as_csv:
        # Create CSV file for export
        return create_usage_csv(requests, start_date, end_date)
    
    # Calculate daily usage statistics
    daily_usage = get_daily_usage(requests, start_date, end_date)
    
    # Get usage by model
    usage_by_model = get_usage_by_model(requests)
    
    # Get usage by endpoint
    usage_by_endpoint = get_usage_by_endpoint(requests)
    
    # Get token usage by day
    token_usage = get_token_usage_by_day(requests, start_date, end_date)
    
    # Calculate summary statistics
    summary = calculate_summary_statistics(requests, start_date, end_date)
    
    # Format response
    response = {
        'usage_by_day': daily_usage,
        'usage_by_model': usage_by_model,
        'usage_by_endpoint': usage_by_endpoint,
        'token_usage': token_usage,
        'summary': summary
    }
    
    return Response(response)

def get_daily_usage(requests, start_date, end_date):
    """
    Calculate daily usage statistics.
    """
    # Generate all days in range
    days = []
    current_date = start_date
    while current_date <= end_date:
        days.append(current_date.date())
        current_date += timedelta(days=1)
    
    # Count requests per day
    daily_counts = {}
    for req in requests:
        timestamp_str = req.get('timestamp')
        if not timestamp_str:
            continue
        
        try:
            if isinstance(timestamp_str, str):
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                date = timestamp.date()
            else:
                # Assume it's already a datetime object
                date = timestamp_str.date()
            
            if date not in daily_counts:
                daily_counts[date] = 0
            daily_counts[date] += 1
        except (ValueError, TypeError):
            continue
    
    # Build result with all days
    result = []
    for day in days:
        result.append({
            'date': day.isoformat(),
            'request_count': daily_counts.get(day, 0)
        })
    
    return result

def get_usage_by_model(requests):
    """
    Calculate usage statistics by model.
    """
    model_usage = {}
    
    for req in requests:
        model = req.get('model_used')
        if not model:
            continue
        
        if model not in model_usage:
            model_usage[model] = {
                'model_used': model,
                'request_count': 0,
                'token_count': 0
            }
        
        model_usage[model]['request_count'] += 1
        model_usage[model]['token_count'] += req.get('tokens_used', 0)
    
    # Convert to list and sort by request count
    result = list(model_usage.values())
    result.sort(key=lambda x: x['request_count'], reverse=True)
    
    return result

def get_usage_by_endpoint(requests):
    """
    Calculate usage statistics by endpoint.
    """
    endpoint_usage = {}
    
    for req in requests:
        endpoint = req.get('endpoint')
        if not endpoint:
            continue
        
        if endpoint not in endpoint_usage:
            endpoint_usage[endpoint] = {
                'endpoint': endpoint,
                'request_count': 0,
                'token_count': 0
            }
        
        endpoint_usage[endpoint]['request_count'] += 1
        endpoint_usage[endpoint]['token_count'] += req.get('tokens_used', 0)
    
    # Convert to list and sort by request count
    result = list(endpoint_usage.values())
    result.sort(key=lambda x: x['request_count'], reverse=True)
    
    return result

def get_token_usage_by_day(requests, start_date, end_date):
    """
    Calculate token usage statistics by day.
    """
    # Generate all days in range
    days = []
    current_date = start_date
    while current_date <= end_date:
        days.append(current_date.date())
        current_date += timedelta(days=1)
    
    # Count tokens per day
    daily_tokens = {}
    for req in requests:
        timestamp_str = req.get('timestamp')
        tokens = req.get('tokens_used', 0)
        
        if not timestamp_str:
            continue
        
        try:
            if isinstance(timestamp_str, str):
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                date = timestamp.date()
            else:
                # Assume it's already a datetime object
                date = timestamp_str.date()
            
            if date not in daily_tokens:
                daily_tokens[date] = 0
            daily_tokens[date] += tokens
        except (ValueError, TypeError):
            continue
    
    # Build result with all days
    result = []
    for day in days:
        tokens = daily_tokens.get(day, 0)
        # Simulate input/output token split
        result.append({
            'date': day.isoformat(),
            'tokens': tokens,
            'input_tokens': int(tokens * 0.4),  # Simulate 40% input tokens
            'output_tokens': int(tokens * 0.6)  # Simulate 60% output tokens
        })
    
    return result

def calculate_summary_statistics(requests, start_date, end_date):
    """
    Calculate summary statistics for the dashboard.
    """
    # Total requests
    total_requests = len(requests)
    
    # Total tokens
    total_tokens = sum(req.get('tokens_used', 0) for req in requests)
    
    # Date range in days
    date_range_days = (end_date - start_date).days + 1
    
    # Average requests per day
    avg_requests_per_day = total_requests / date_range_days if date_range_days > 0 else 0
    
    # Success rate: requests with status code 200-299 / total requests
    successful_requests = sum(
        1 for req in requests 
        if req.get('status_code', 0) >= 200 and req.get('status_code', 0) < 300
    )
    
    success_rate = (successful_requests / total_requests * 100) if total_requests > 0 else 0
    
    # Average response time
    durations = [req.get('duration_ms', 0) for req in requests if req.get('duration_ms', 0) > 0]
    avg_response_time = sum(durations) / len(durations) if durations else 0
    
    # Calculate estimated cost (assuming average cost of $0.002 per 1000 tokens)
    estimated_cost = (total_tokens / 1000) * 0.002
    
    return {
        'total_requests': total_requests,
        'total_tokens': total_tokens,
        'avg_requests_per_day': round(avg_requests_per_day, 1),
        'success_rate': success_rate,
        'avg_response_time': avg_response_time,
        'estimated_cost': estimated_cost
    }

def create_usage_csv(requests, start_date, end_date):
    """
    Create a CSV file for exporting usage statistics.
    """
    # Create a CSV file in memory
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    
    # Write header
    writer.writerow([
        'Date', 'Time', 'API Key', 'Model', 'Endpoint', 
        'Status Code', 'Tokens Used', 'Duration (ms)', 'Error'
    ])
    
    # Write data rows
    for req in sorted(requests, key=lambda x: x.get('timestamp', '')):
        timestamp_str = req.get('timestamp')
        if not timestamp_str:
            continue
        
        try:
            if isinstance(timestamp_str, str):
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                # Assume it's already a datetime object
                timestamp = timestamp_str
            
            # Get API key name
            api_key_name = 'N/A'
            api_key_id = req.get('api_key_id')
            if api_key_id:
                try:
                    api_key = api_key_adapter.get(id=api_key_id)
                    api_key_name = api_key.get('name', 'N/A')
                except Exception:
                    pass
            
            writer.writerow([
                timestamp.strftime('%Y-%m-%d'),
                timestamp.strftime('%H:%M:%S'),
                api_key_name,
                req.get('model_used', 'N/A'),
                req.get('endpoint', 'N/A'),
                req.get('status_code', 'N/A'),
                req.get('tokens_used', 0),
                req.get('duration_ms', 'N/A'),
                'Yes' if req.get('error') else 'No'
            ])
        except (ValueError, TypeError, AttributeError):
            continue
    
    # Prepare response
    csv_data = csv_buffer.getvalue()
    
    return Response({
        'csv_data': csv_data
    })
