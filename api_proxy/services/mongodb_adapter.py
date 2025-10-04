"""
MongoDB adapter for api_proxy app.

This module provides adapter classes that mimic Django model behavior but use MongoDB as the backend.
"""
import logging
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional, Union, Tuple

from django.core.paginator import Paginator
from django.db.models import Q
from django.http import Http404

from knowledge_graph.services.mongodb_service import MongoDBService

logger = logging.getLogger(__name__)

class MongoDBAdapter:
    """Base adapter class for MongoDB."""
    
    def __init__(self, collection_name: str):
        """Initialize the adapter with a collection name."""
        self.collection_name = collection_name
        self._mongo_service = None
    
    @property
    def mongo_service(self) -> MongoDBService:
        """Lazy load the MongoDB service."""
        if self._mongo_service is None:
            self._mongo_service = MongoDBService()
        return self._mongo_service
    
    @property
    def collection(self):
        """Get the MongoDB collection."""
        return self.mongo_service.get_collection(self.collection_name)
    
    def close(self):
        """Close the MongoDB connection."""
        if self._mongo_service is not None:
            self._mongo_service.close()
            self._mongo_service = None


class APIKeyAdapter(MongoDBAdapter):
    """Adapter for APIKey model."""
    
    def __init__(self):
        """Initialize the adapter."""
        super().__init__('api_keys')
    
    def create(self, **kwargs) -> Dict[str, Any]:
        """Create a new API key."""
        # Generate ID if not provided
        if 'id' not in kwargs:
            kwargs['id'] = str(uuid.uuid4())
        
        # Generate API key if not provided
        if 'key' not in kwargs:
            # Generate a secure API key with prefix
            import secrets
            key = f"pk-{secrets.token_hex(32)}"
            kwargs['key'] = key
        
        # Add timestamps
        now = datetime.now()
        kwargs['created_at'] = now
        
        # Initialize request count
        if 'request_count' not in kwargs:
            kwargs['request_count'] = 0
        
        # Ensure allowed_models is a list
        if 'allowed_models' not in kwargs:
            kwargs['allowed_models'] = []
        
        # Ensure is_active is set
        if 'is_active' not in kwargs:
            kwargs['is_active'] = True
        
        # Insert into MongoDB
        api_key_id = self.mongo_service.create_api_key(kwargs)
        
        # Return the created API key
        return self.get(id=api_key_id)
    
    def get(self, **kwargs) -> Dict[str, Any]:
        """Get an API key by filters."""
        # Handle Django-style ID lookup
        if 'pk' in kwargs:
            kwargs['id'] = kwargs.pop('pk')
        
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Get from MongoDB
        api_key = self.collection.find_one(query)
        
        if not api_key:
            raise Http404(f"API key not found with query: {kwargs}")
        
        return api_key
    
    def get_by_key(self, key: str) -> Dict[str, Any]:
        """Get an API key by the key value."""
        return self.get(key=key)
    
    def filter(self, **kwargs) -> List[Dict[str, Any]]:
        """Filter API keys by criteria."""
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Apply sorting
        sort_by = kwargs.pop('order_by', '-created_at') if 'order_by' in kwargs else '-created_at'
        sort_dir = 1  # Ascending
        
        # Handle descending sort
        if sort_by.startswith('-'):
            sort_by = sort_by[1:]
            sort_dir = -1
        
        # Get from MongoDB
        cursor = self.collection.find(query).sort(sort_by, sort_dir)
        
        return list(cursor)
    
    def all(self) -> List[Dict[str, Any]]:
        """Get all API keys."""
        return list(self.collection.find().sort('created_at', -1))
    
    def update(self, api_key_id: str, **kwargs) -> Dict[str, Any]:
        """Update an API key."""
        # Update timestamp for last_used if provided
        if 'last_used' in kwargs and kwargs['last_used'] is True:
            kwargs['last_used'] = datetime.now()
        
        # Increment request count if provided
        if 'increment_request_count' in kwargs and kwargs['increment_request_count'] is True:
            # Remove the flag
            del kwargs['increment_request_count']
            
            # Get current request count
            api_key = self.get(id=api_key_id)
            current_count = api_key.get('request_count', 0)
            
            # Increment by 1
            kwargs['request_count'] = current_count + 1
        
        # Remove ID from update data if present
        if 'id' in kwargs:
            del kwargs['id']
        
        # Remove key from update data if present (key should not be updated)
        if 'key' in kwargs:
            del kwargs['key']
        
        # Update in MongoDB
        self.mongo_service.update_api_key(api_key_id, kwargs)
        
        # Return the updated API key
        return self.get(id=api_key_id)
    
    def delete(self, api_key_id: str) -> bool:
        """Delete an API key."""
        return self.mongo_service.delete_api_key(api_key_id)
    
    def count(self, **kwargs) -> int:
        """Count API keys with filters."""
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Count in MongoDB
        return self.collection.count_documents(query)
    
    def _build_query(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Build a MongoDB query from Django-style filters."""
        query = {}
        
        for key, value in filters.items():
            # Handle Q objects
            if isinstance(value, Q):
                # This is a simplified conversion and may not handle all Q object cases
                for child in value.children:
                    if isinstance(child, tuple):
                        field, val = child
                        query[field] = val
            
            # Handle special Django-style lookups
            elif '__' in key:
                field, lookup = key.split('__', 1)
                
                if lookup == 'exact':
                    query[field] = value
                elif lookup == 'iexact':
                    query[field] = {'$regex': f'^{value}$', '$options': 'i'}
                elif lookup == 'contains':
                    query[field] = {'$regex': value, '$options': ''}
                elif lookup == 'icontains':
                    query[field] = {'$regex': value, '$options': 'i'}
                elif lookup == 'in':
                    query[field] = {'$in': value}
                elif lookup == 'gt':
                    query[field] = {'$gt': value}
                elif lookup == 'gte':
                    query[field] = {'$gte': value}
                elif lookup == 'lt':
                    query[field] = {'$lt': value}
                elif lookup == 'lte':
                    query[field] = {'$lte': value}
                elif lookup == 'startswith':
                    query[field] = {'$regex': f'^{value}', '$options': ''}
                elif lookup == 'istartswith':
                    query[field] = {'$regex': f'^{value}', '$options': 'i'}
                elif lookup == 'endswith':
                    query[field] = {'$regex': f'{value}$', '$options': ''}
                elif lookup == 'iendswith':
                    query[field] = {'$regex': f'{value}$', '$options': 'i'}
                elif lookup == 'isnull':
                    if value:
                        query[field] = None
                    else:
                        query[field] = {'$ne': None}
            else:
                query[key] = value
        
        return query


class ExternalAPIConfigAdapter(MongoDBAdapter):
    """Adapter for ExternalAPIConfig model."""
    
    def __init__(self):
        """Initialize the adapter."""
        super().__init__('external_api_configs')
    
    def create(self, **kwargs) -> Dict[str, Any]:
        """Create a new external API config."""
        # Generate ID if not provided
        if 'id' not in kwargs:
            kwargs['id'] = str(uuid.uuid4())
        
        # Add timestamps
        now = datetime.now()
        kwargs['created_at'] = now
        kwargs['updated_at'] = now
        
        # Ensure config is a dict
        if 'config' not in kwargs:
            kwargs['config'] = {}
        
        # Ensure is_active is set
        if 'is_active' not in kwargs:
            kwargs['is_active'] = True
        
        # Ensure priority is set
        if 'priority' not in kwargs:
            kwargs['priority'] = 100
        
        # Insert into MongoDB
        config_id = self.mongo_service.create_external_api_config(kwargs)
        
        # Return the created config
        return self.get(id=config_id)
    
    def get(self, **kwargs) -> Dict[str, Any]:
        """Get an external API config by filters."""
        # Handle Django-style ID lookup
        if 'pk' in kwargs:
            kwargs['id'] = kwargs.pop('pk')
        
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Get from MongoDB
        config = self.collection.find_one(query)
        
        if not config:
            raise Http404(f"External API config not found with query: {kwargs}")
        
        return config
    
    def filter(self, **kwargs) -> List[Dict[str, Any]]:
        """Filter external API configs by criteria."""
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Apply sorting
        sort_by = kwargs.pop('order_by', 'name') if 'order_by' in kwargs else 'name'
        sort_dir = 1  # Ascending
        
        # Handle descending sort
        if sort_by.startswith('-'):
            sort_by = sort_by[1:]
            sort_dir = -1
        
        # Get from MongoDB
        cursor = self.collection.find(query).sort(sort_by, sort_dir)
        
        return list(cursor)
    
    def all(self) -> List[Dict[str, Any]]:
        """Get all external API configs."""
        return list(self.collection.find().sort('name', 1))
    
    def update(self, config_id: str, **kwargs) -> Dict[str, Any]:
        """Update an external API config."""
        # Update timestamp
        kwargs['updated_at'] = datetime.now()
        
        # Remove ID from update data if present
        if 'id' in kwargs:
            del kwargs['id']
        
        # Update in MongoDB
        self.mongo_service.update_external_api_config(config_id, kwargs)
        
        # Return the updated config
        return self.get(id=config_id)
    
    def delete(self, config_id: str) -> bool:
        """Delete an external API config."""
        return self.mongo_service.delete_external_api_config(config_id)
    
    def count(self, **kwargs) -> int:
        """Count external API configs with filters."""
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Count in MongoDB
        return self.collection.count_documents(query)
    
    def _build_query(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Build a MongoDB query from Django-style filters."""
        query = {}
        
        for key, value in filters.items():
            # Handle Q objects
            if isinstance(value, Q):
                # This is a simplified conversion and may not handle all Q object cases
                for child in value.children:
                    if isinstance(child, tuple):
                        field, val = child
                        query[field] = val
            
            # Handle special Django-style lookups
            elif '__' in key:
                field, lookup = key.split('__', 1)
                
                if lookup == 'exact':
                    query[field] = value
                elif lookup == 'iexact':
                    query[field] = {'$regex': f'^{value}$', '$options': 'i'}
                elif lookup == 'contains':
                    query[field] = {'$regex': value, '$options': ''}
                elif lookup == 'icontains':
                    query[field] = {'$regex': value, '$options': 'i'}
                elif lookup == 'in':
                    query[field] = {'$in': value}
                elif lookup == 'gt':
                    query[field] = {'$gt': value}
                elif lookup == 'gte':
                    query[field] = {'$gte': value}
                elif lookup == 'lt':
                    query[field] = {'$lt': value}
                elif lookup == 'lte':
                    query[field] = {'$lte': value}
                elif lookup == 'startswith':
                    query[field] = {'$regex': f'^{value}', '$options': ''}
                elif lookup == 'istartswith':
                    query[field] = {'$regex': f'^{value}', '$options': 'i'}
                elif lookup == 'endswith':
                    query[field] = {'$regex': f'{value}$', '$options': ''}
                elif lookup == 'iendswith':
                    query[field] = {'$regex': f'{value}$', '$options': 'i'}
                elif lookup == 'isnull':
                    if value:
                        query[field] = None
                    else:
                        query[field] = {'$ne': None}
            else:
                query[key] = value
        
        return query


class ModelMappingAdapter(MongoDBAdapter):
    """Adapter for ModelMapping model."""
    
    def __init__(self):
        """Initialize the adapter."""
        super().__init__('model_mappings')
    
    def create(self, **kwargs) -> Dict[str, Any]:
        """Create a new model mapping."""
        # Generate ID if not provided
        if 'id' not in kwargs:
            kwargs['id'] = str(uuid.uuid4())
        
        # Ensure is_active is set
        if 'is_active' not in kwargs:
            kwargs['is_active'] = True
        
        # Handle provider reference
        if 'provider' in kwargs and hasattr(kwargs['provider'], 'id'):
            kwargs['provider_id'] = str(kwargs['provider'].id)
            del kwargs['provider']
        
        # Insert into MongoDB
        mapping_id = self.mongo_service.create_model_mapping(kwargs)
        
        # Return the created mapping
        return self.get(id=mapping_id)
    
    def get(self, **kwargs) -> Dict[str, Any]:
        """Get a model mapping by filters."""
        # Handle Django-style ID lookup
        if 'pk' in kwargs:
            kwargs['id'] = kwargs.pop('pk')
        
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Get from MongoDB
        mapping = self.collection.find_one(query)
        
        if not mapping:
            raise Http404(f"Model mapping not found with query: {kwargs}")
        
        return mapping
    
    def filter(self, **kwargs) -> List[Dict[str, Any]]:
        """Filter model mappings by criteria."""
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Apply sorting
        sort_by = kwargs.pop('order_by', 'local_name') if 'order_by' in kwargs else 'local_name'
        sort_dir = 1  # Ascending
        
        # Handle descending sort
        if sort_by.startswith('-'):
            sort_by = sort_by[1:]
            sort_dir = -1
        
        # Get from MongoDB
        cursor = self.collection.find(query).sort(sort_by, sort_dir)
        
        return list(cursor)
    
    def all(self) -> List[Dict[str, Any]]:
        """Get all model mappings."""
        return list(self.collection.find().sort('local_name', 1))
    
    def update(self, mapping_id: str, **kwargs) -> Dict[str, Any]:
        """Update a model mapping."""
        # Remove ID from update data if present
        if 'id' in kwargs:
            del kwargs['id']
        
        # Handle provider reference
        if 'provider' in kwargs and hasattr(kwargs['provider'], 'id'):
            kwargs['provider_id'] = str(kwargs['provider'].id)
            del kwargs['provider']
        
        # Update in MongoDB
        self.mongo_service.update_model_mapping(mapping_id, kwargs)
        
        # Return the updated mapping
        return self.get(id=mapping_id)
    
    def delete(self, mapping_id: str) -> bool:
        """Delete a model mapping."""
        return self.mongo_service.delete_model_mapping(mapping_id)
    
    def count(self, **kwargs) -> int:
        """Count model mappings with filters."""
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Count in MongoDB
        return self.collection.count_documents(query)
    
    def _build_query(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Build a MongoDB query from Django-style filters."""
        query = {}
        
        for key, value in filters.items():
            # Handle Q objects
            if isinstance(value, Q):
                # This is a simplified conversion and may not handle all Q object cases
                for child in value.children:
                    if isinstance(child, tuple):
                        field, val = child
                        query[field] = val
            
            # Handle special Django-style lookups
            elif '__' in key:
                field, lookup = key.split('__', 1)
                
                # Handle foreign key lookups
                if field == 'provider' and lookup != 'id':
                    # For example, provider__name__icontains becomes provider_id lookup
                    # This is a simplification and may need to be expanded
                    if hasattr(value, 'id'):
                        query['provider_id'] = str(value.id)
                    continue
                
                if lookup == 'exact':
                    query[field] = value
                elif lookup == 'iexact':
                    query[field] = {'$regex': f'^{value}$', '$options': 'i'}
                elif lookup == 'contains':
                    query[field] = {'$regex': value, '$options': ''}
                elif lookup == 'icontains':
                    query[field] = {'$regex': value, '$options': 'i'}
                elif lookup == 'in':
                    query[field] = {'$in': value}
                elif lookup == 'gt':
                    query[field] = {'$gt': value}
                elif lookup == 'gte':
                    query[field] = {'$gte': value}
                elif lookup == 'lt':
                    query[field] = {'$lt': value}
                elif lookup == 'lte':
                    query[field] = {'$lte': value}
                elif lookup == 'startswith':
                    query[field] = {'$regex': f'^{value}', '$options': ''}
                elif lookup == 'istartswith':
                    query[field] = {'$regex': f'^{value}', '$options': 'i'}
                elif lookup == 'endswith':
                    query[field] = {'$regex': f'{value}$', '$options': ''}
                elif lookup == 'iendswith':
                    query[field] = {'$regex': f'{value}$', '$options': 'i'}
                elif lookup == 'isnull':
                    if value:
                        query[field] = None
                    else:
                        query[field] = {'$ne': None}
                elif lookup == 'id':
                    # Handle foreign key ID lookups
                    query[f'{field}_id'] = str(value)
            else:
                # Handle foreign key objects
                if key == 'provider' and hasattr(value, 'id'):
                    query['provider_id'] = str(value.id)
                else:
                    query[key] = value
        
        return query


class ModelRoutingAdapter(MongoDBAdapter):
    """Adapter for ModelRouting model."""
    
    def __init__(self):
        """Initialize the adapter."""
        super().__init__('model_routings')
    
    def create(self, **kwargs) -> Dict[str, Any]:
        """Create a new model routing rule."""
        # Generate ID if not provided
        if 'id' not in kwargs:
            kwargs['id'] = str(uuid.uuid4())
        
        # Add timestamps
        now = datetime.now()
        kwargs['created_at'] = now
        kwargs['updated_at'] = now
        
        # Ensure is_active is set
        if 'is_active' not in kwargs:
            kwargs['is_active'] = True
        
        # Ensure priority is set
        if 'priority' not in kwargs:
            kwargs['priority'] = 10
        
        # Handle target_model reference
        if 'target_model' in kwargs and hasattr(kwargs['target_model'], 'id'):
            kwargs['target_model_id'] = str(kwargs['target_model'].id)
            del kwargs['target_model']
        
        # Insert into MongoDB
        routing_id = self.mongo_service.create_model_routing(kwargs)
        
        # Return the created routing rule
        return self.get(id=routing_id)
    
    def get(self, **kwargs) -> Dict[str, Any]:
        """Get a model routing rule by filters."""
        # Handle Django-style ID lookup
        if 'pk' in kwargs:
            kwargs['id'] = kwargs.pop('pk')
        
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Get from MongoDB
        routing = self.collection.find_one(query)
        
        if not routing:
            raise Http404(f"Model routing rule not found with query: {kwargs}")
        
        return routing
    
    def filter(self, **kwargs) -> List[Dict[str, Any]]:
        """Filter model routing rules by criteria."""
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Apply sorting
        sort_by = kwargs.pop('order_by', 'priority') if 'order_by' in kwargs else 'priority'
        sort_dir = 1  # Ascending
        
        # Handle descending sort
        if sort_by.startswith('-'):
            sort_by = sort_by[1:]
            sort_dir = -1
        
        # Get from MongoDB
        cursor = self.collection.find(query).sort(sort_by, sort_dir)
        
        return list(cursor)
    
    def all(self) -> List[Dict[str, Any]]:
        """Get all model routing rules."""
        return list(self.collection.find().sort('priority', 1))
    
    def update(self, routing_id: str, **kwargs) -> Dict[str, Any]:
        """Update a model routing rule."""
        # Update timestamp
        kwargs['updated_at'] = datetime.now()
        
        # Remove ID from update data if present
        if 'id' in kwargs:
            del kwargs['id']
        
        # Handle target_model reference
        if 'target_model' in kwargs and hasattr(kwargs['target_model'], 'id'):
            kwargs['target_model_id'] = str(kwargs['target_model'].id)
            del kwargs['target_model']
        
        # Update in MongoDB
        self.mongo_service.update_model_routing(routing_id, kwargs)
        
        # Return the updated routing rule
        return self.get(id=routing_id)
    
    def delete(self, routing_id: str) -> bool:
        """Delete a model routing rule."""
        return self.mongo_service.delete_model_routing(routing_id)
    
    def count(self, **kwargs) -> int:
        """Count model routing rules with filters."""
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Count in MongoDB
        return self.collection.count_documents(query)
    
    def _build_query(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Build a MongoDB query from Django-style filters."""
        query = {}
        
        for key, value in filters.items():
            # Handle Q objects
            if isinstance(value, Q):
                # This is a simplified conversion and may not handle all Q object cases
                for child in value.children:
                    if isinstance(child, tuple):
                        field, val = child
                        query[field] = val
            
            # Handle special Django-style lookups
            elif '__' in key:
                field, lookup = key.split('__', 1)
                
                # Handle foreign key lookups
                if field == 'target_model' and lookup != 'id':
                    # For example, target_model__name__icontains becomes target_model_id lookup
                    # This is a simplification and may need to be expanded
                    if hasattr(value, 'id'):
                        query['target_model_id'] = str(value.id)
                    continue
                
                if lookup == 'exact':
                    query[field] = value
                elif lookup == 'iexact':
                    query[field] = {'$regex': f'^{value}$', '$options': 'i'}
                elif lookup == 'contains':
                    query[field] = {'$regex': value, '$options': ''}
                elif lookup == 'icontains':
                    query[field] = {'$regex': value, '$options': 'i'}
                elif lookup == 'in':
                    query[field] = {'$in': value}
                elif lookup == 'gt':
                    query[field] = {'$gt': value}
                elif lookup == 'gte':
                    query[field] = {'$gte': value}
                elif lookup == 'lt':
                    query[field] = {'$lt': value}
                elif lookup == 'lte':
                    query[field] = {'$lte': value}
                elif lookup == 'startswith':
                    query[field] = {'$regex': f'^{value}', '$options': ''}
                elif lookup == 'istartswith':
                    query[field] = {'$regex': f'^{value}', '$options': 'i'}
                elif lookup == 'endswith':
                    query[field] = {'$regex': f'{value}$', '$options': ''}
                elif lookup == 'iendswith':
                    query[field] = {'$regex': f'{value}$', '$options': 'i'}
                elif lookup == 'isnull':
                    if value:
                        query[field] = None
                    else:
                        query[field] = {'$ne': None}
                elif lookup == 'id':
                    # Handle foreign key ID lookups
                    query[f'{field}_id'] = str(value)
            else:
                # Handle foreign key objects
                if key == 'target_model' and hasattr(value, 'id'):
                    query['target_model_id'] = str(value.id)
                else:
                    query[key] = value
        
        return query


class APIRequestAdapter(MongoDBAdapter):
    """Adapter for APIRequest model."""
    
    def __init__(self):
        """Initialize the adapter."""
        super().__init__('api_requests')
    
    def create(self, **kwargs) -> Dict[str, Any]:
        """Create a new API request log."""
        # Generate ID if not provided
        if 'id' not in kwargs:
            kwargs['id'] = str(uuid.uuid4())
        
        # Add timestamp if not present
        if 'timestamp' not in kwargs:
            kwargs['timestamp'] = datetime.now()
        
        # Handle api_key reference
        if 'api_key' in kwargs and hasattr(kwargs['api_key'], 'id'):
            kwargs['api_key_id'] = str(kwargs['api_key'].id)
            del kwargs['api_key']
        
        # Handle provider_used reference
        if 'provider_used' in kwargs and hasattr(kwargs['provider_used'], 'id'):
            kwargs['provider_used_id'] = str(kwargs['provider_used'].id)
            del kwargs['provider_used']
        
        # Insert into MongoDB
        request_id = self.mongo_service.create_api_request(kwargs)
        
        # Return the created request log
        return self.get(id=request_id)
    
    def get(self, **kwargs) -> Dict[str, Any]:
        """Get an API request log by filters."""
        # Handle Django-style ID lookup
        if 'pk' in kwargs:
            kwargs['id'] = kwargs.pop('pk')
        
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Get from MongoDB
        request = self.collection.find_one(query)
        
        if not request:
            raise Http404(f"API request not found with query: {kwargs}")
        
        return request
    
    def filter(self, **kwargs) -> List[Dict[str, Any]]:
        """Filter API request logs by criteria."""
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Apply sorting
        sort_by = kwargs.pop('order_by', '-timestamp') if 'order_by' in kwargs else '-timestamp'
        sort_dir = 1  # Ascending
        
        # Handle descending sort
        if sort_by.startswith('-'):
            sort_by = sort_by[1:]
            sort_dir = -1
        
        # Get from MongoDB
        cursor = self.collection.find(query).sort(sort_by, sort_dir)
        
        return list(cursor)
    
    def all(self) -> List[Dict[str, Any]]:
        """Get all API request logs."""
        return list(self.collection.find().sort('timestamp', -1))
    
    def count(self, **kwargs) -> int:
        """Count API request logs with filters."""
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Count in MongoDB
        return self.collection.count_documents(query)
    
    def _build_query(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Build a MongoDB query from Django-style filters."""
        query = {}
        
        for key, value in filters.items():
            # Handle Q objects
            if isinstance(value, Q):
                # This is a simplified conversion and may not handle all Q object cases
                for child in value.children:
                    if isinstance(child, tuple):
                        field, val = child
                        query[field] = val
            
            # Handle special Django-style lookups
            elif '__' in key:
                field, lookup = key.split('__', 1)
                
                # Handle foreign key lookups
                if field in ['api_key', 'provider_used'] and lookup != 'id':
                    # For example, api_key__name__icontains becomes api_key_id lookup
                    # This is a simplification and may need to be expanded
                    if hasattr(value, 'id'):
                        query[f'{field}_id'] = str(value.id)
                    continue
                
                if lookup == 'exact':
                    query[field] = value
                elif lookup == 'iexact':
                    query[field] = {'$regex': f'^{value}$', '$options': 'i'}
                elif lookup == 'contains':
                    query[field] = {'$regex': value, '$options': ''}
                elif lookup == 'icontains':
                    query[field] = {'$regex': value, '$options': 'i'}
                elif lookup == 'in':
                    query[field] = {'$in': value}
                elif lookup == 'gt':
                    query[field] = {'$gt': value}
                elif lookup == 'gte':
                    query[field] = {'$gte': value}
                elif lookup == 'lt':
                    query[field] = {'$lt': value}
                elif lookup == 'lte':
                    query[field] = {'$lte': value}
                elif lookup == 'startswith':
                    query[field] = {'$regex': f'^{value}', '$options': ''}
                elif lookup == 'istartswith':
                    query[field] = {'$regex': f'^{value}', '$options': 'i'}
                elif lookup == 'endswith':
                    query[field] = {'$regex': f'{value}$', '$options': ''}
                elif lookup == 'iendswith':
                    query[field] = {'$regex': f'{value}$', '$options': 'i'}
                elif lookup == 'isnull':
                    if value:
                        query[field] = None
                    else:
                        query[field] = {'$ne': None}
                elif lookup == 'id':
                    # Handle foreign key ID lookups
                    query[f'{field}_id'] = str(value)
            else:
                # Handle foreign key objects
                if key in ['api_key', 'provider_used'] and hasattr(value, 'id'):
                    query[f'{key}_id'] = str(value.id)
                else:
                    query[key] = value
        
        return query


# Create singleton instances
api_key_adapter = APIKeyAdapter()
external_api_config_adapter = ExternalAPIConfigAdapter()
model_mapping_adapter = ModelMappingAdapter()
model_routing_adapter = ModelRoutingAdapter()
api_request_adapter = APIRequestAdapter()
