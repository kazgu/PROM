"""
MongoDB adapter for knowledge_graph app.

This module provides adapter classes that mimic Django model behavior but use MongoDB as the backend.
"""
import logging
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional, Union, Tuple

from django.core.paginator import Paginator
from django.db.models import Q
from django.http import Http404

from .mongodb_service import MongoDBService

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


class EntityAdapter(MongoDBAdapter):
    """Adapter for Entity model."""
    
    def __init__(self):
        """Initialize the adapter."""
        super().__init__('entities')
    
    def create(self, **kwargs) -> Dict[str, Any]:
        """Create a new entity."""
        # Generate ID if not provided
        if 'id' not in kwargs:
            kwargs['id'] = str(uuid.uuid4())
        
        # Add timestamps
        now = datetime.now()
        kwargs['created_at'] = now
        kwargs['updated_at'] = now
        
        # Normalize name if not provided
        if 'name' in kwargs and 'normalized_name' not in kwargs:
            kwargs['normalized_name'] = kwargs['name'].lower().strip()
        
        # Ensure properties is a dict
        if 'properties' not in kwargs:
            kwargs['properties'] = {}
        
        # Convert API key to ID if it's an object
        if 'api_key' in kwargs and hasattr(kwargs['api_key'], 'id'):
            kwargs['api_key_id'] = str(kwargs['api_key'].id)
            del kwargs['api_key']
        
        # Insert into MongoDB
        entity_id = self.mongo_service.create_entity(kwargs)
        
        # Return the created entity
        return self.get(id=entity_id)
    
    def get(self, **kwargs) -> Dict[str, Any]:
        """Get an entity by filters."""
        # Handle Django-style ID lookup
        if 'pk' in kwargs:
            kwargs['id'] = kwargs.pop('pk')
        
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Get from MongoDB
        entity = self.collection.find_one(query)
        
        if not entity:
            raise Http404(f"Entity not found with query: {kwargs}")
        
        return entity
    
    def filter(self, **kwargs) -> List[Dict[str, Any]]:
        """Filter entities by criteria."""
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
        """Get all entities."""
        return list(self.collection.find())
    
    def update(self, entity_id: str, **kwargs) -> Dict[str, Any]:
        """Update an entity."""
        # Update timestamp
        kwargs['updated_at'] = datetime.now()
        
        # Normalize name if provided
        if 'name' in kwargs and 'normalized_name' not in kwargs:
            kwargs['normalized_name'] = kwargs['name'].lower().strip()
        
        # Convert API key to ID if it's an object
        if 'api_key' in kwargs and hasattr(kwargs['api_key'], 'id'):
            kwargs['api_key_id'] = str(kwargs['api_key'].id)
            del kwargs['api_key']
        
        # Update in MongoDB
        self.mongo_service.update_entity(entity_id, kwargs)
        
        # Return the updated entity
        return self.get(id=entity_id)
    
    def delete(self, entity_id: str) -> bool:
        """Delete an entity."""
        return self.mongo_service.delete_entity(entity_id)
    
    def count(self, **kwargs) -> int:
        """Count entities with filters."""
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


class RelationshipAdapter(MongoDBAdapter):
    """Adapter for Relationship model."""
    
    def __init__(self):
        """Initialize the adapter."""
        super().__init__('relationships')
    
    def create(self, **kwargs) -> Dict[str, Any]:
        """Create a new relationship."""
        # Generate ID if not provided
        if 'id' not in kwargs:
            kwargs['id'] = str(uuid.uuid4())
        
        # Add timestamps
        now = datetime.now()
        kwargs['created_at'] = now
        kwargs['updated_at'] = now
        
        # Normalize name if not provided
        if 'name' in kwargs and 'normalized_name' not in kwargs:
            kwargs['normalized_name'] = kwargs['name'].lower().strip()
        
        # Ensure properties is a dict
        if 'properties' not in kwargs:
            kwargs['properties'] = {}
        
        # Convert API key to ID if it's an object
        if 'api_key' in kwargs and hasattr(kwargs['api_key'], 'id'):
            kwargs['api_key_id'] = str(kwargs['api_key'].id)
            del kwargs['api_key']
        
        # Insert into MongoDB
        relationship_id = self.mongo_service.create_relationship(kwargs)
        
        # Return the created relationship
        return self.get(id=relationship_id)
    
    def get(self, **kwargs) -> Dict[str, Any]:
        """Get a relationship by filters."""
        # Handle Django-style ID lookup
        if 'pk' in kwargs:
            kwargs['id'] = kwargs.pop('pk')
        
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Get from MongoDB
        relationship = self.collection.find_one(query)
        
        if not relationship:
            raise Http404(f"Relationship not found with query: {kwargs}")
        
        return relationship
    
    def filter(self, **kwargs) -> List[Dict[str, Any]]:
        """Filter relationships by criteria."""
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
        """Get all relationships."""
        return list(self.collection.find())
    
    def update(self, relationship_id: str, **kwargs) -> Dict[str, Any]:
        """Update a relationship."""
        # Update timestamp
        kwargs['updated_at'] = datetime.now()
        
        # Normalize name if provided
        if 'name' in kwargs and 'normalized_name' not in kwargs:
            kwargs['normalized_name'] = kwargs['name'].lower().strip()
        
        # Convert API key to ID if it's an object
        if 'api_key' in kwargs and hasattr(kwargs['api_key'], 'id'):
            kwargs['api_key_id'] = str(kwargs['api_key'].id)
            del kwargs['api_key']
        
        # Update in MongoDB
        self.mongo_service.update_relationship(relationship_id, kwargs)
        
        # Return the updated relationship
        return self.get(id=relationship_id)
    
    def delete(self, relationship_id: str) -> bool:
        """Delete a relationship."""
        return self.mongo_service.delete_relationship(relationship_id)
    
    def count(self, **kwargs) -> int:
        """Count relationships with filters."""
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


class TripleAdapter(MongoDBAdapter):
    """Adapter for Triple model."""
    
    def __init__(self):
        """Initialize the adapter."""
        super().__init__('triples')
    
    def create(self, **kwargs) -> Dict[str, Any]:
        """Create a new triple."""
        # Generate ID if not provided
        if 'id' not in kwargs:
            kwargs['id'] = str(uuid.uuid4())
        
        # Add timestamps
        now = datetime.now()
        kwargs['created_at'] = now
        kwargs['updated_at'] = now
        
        # Handle subject, predicate, and object
        for field in ['subject', 'predicate', 'object']:
            if field in kwargs and hasattr(kwargs[field], 'id'):
                kwargs[f'{field}_id'] = str(kwargs[field].id)
                del kwargs[field]
        
        # Convert API key to ID if it's an object
        if 'api_key' in kwargs and hasattr(kwargs['api_key'], 'id'):
            kwargs['api_key_id'] = str(kwargs['api_key'].id)
            del kwargs['api_key']
        
        # Insert into MongoDB
        triple_id = self.mongo_service.create_triple(kwargs)
        
        # Return the created triple
        return self.get(id=triple_id)
    
    def get(self, **kwargs) -> Dict[str, Any]:
        """Get a triple by filters."""
        # Handle Django-style ID lookup
        if 'pk' in kwargs:
            kwargs['id'] = kwargs.pop('pk')
        
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Get from MongoDB
        triple = self.collection.find_one(query)
        
        if not triple:
            raise Http404(f"Triple not found with query: {kwargs}")
        
        return triple
    
    def filter(self, **kwargs) -> List[Dict[str, Any]]:
        """Filter triples by criteria."""
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
        """Get all triples."""
        return list(self.collection.find().sort('created_at', -1))
    
    def update(self, triple_id: str, **kwargs) -> Dict[str, Any]:
        """Update a triple."""
        # Update timestamp
        kwargs['updated_at'] = datetime.now()
        
        # Handle subject, predicate, and object
        for field in ['subject', 'predicate', 'object']:
            if field in kwargs and hasattr(kwargs[field], 'id'):
                kwargs[f'{field}_id'] = str(kwargs[field].id)
                del kwargs[field]
        
        # Convert API key to ID if it's an object
        if 'api_key' in kwargs and hasattr(kwargs['api_key'], 'id'):
            kwargs['api_key_id'] = str(kwargs['api_key'].id)
            del kwargs['api_key']
        
        # Update in MongoDB
        self.mongo_service.update_triple(triple_id, kwargs)
        
        # Return the updated triple
        return self.get(id=triple_id)
    
    def delete(self, triple_id: str) -> bool:
        """Delete a triple."""
        return self.mongo_service.delete_triple(triple_id)
    
    def count(self, **kwargs) -> int:
        """Count triples with filters."""
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
                if field in ['subject', 'predicate', 'object'] and lookup != 'id':
                    # For example, subject__name__icontains becomes subject_id lookup
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
                if key in ['subject', 'predicate', 'object'] and hasattr(value, 'id'):
                    query[f'{key}_id'] = str(value.id)
                else:
                    query[key] = value
        
        return query


class QueryAdapter(MongoDBAdapter):
    """Adapter for Query model."""
    
    def __init__(self):
        """Initialize the adapter."""
        super().__init__('queries')
    
    def create(self, **kwargs) -> Dict[str, Any]:
        """Create a new query."""
        # Generate ID if not provided
        if 'id' not in kwargs:
            kwargs['id'] = str(uuid.uuid4())
        
        # Add timestamp
        if 'created_at' not in kwargs:
            kwargs['created_at'] = datetime.now()
        
        # Insert into MongoDB
        query_id = self.mongo_service.create_query(kwargs)
        
        # Return the created query
        return self.get(id=query_id)
    
    def get(self, **kwargs) -> Dict[str, Any]:
        """Get a query by filters."""
        # Handle Django-style ID lookup
        if 'pk' in kwargs:
            kwargs['id'] = kwargs.pop('pk')
        
        # Convert Q objects to MongoDB query
        query = self._build_query(kwargs)
        
        # Get from MongoDB
        query_obj = self.collection.find_one(query)
        
        if not query_obj:
            raise Http404(f"Query not found with query: {kwargs}")
        
        return query_obj
    
    def filter(self, **kwargs) -> List[Dict[str, Any]]:
        """Filter queries by criteria."""
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
        """Get all queries."""
        return list(self.collection.find().sort('created_at', -1))
    
    def count(self, **kwargs) -> int:
        """Count queries with filters."""
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


# Create singleton instances
entity_adapter = EntityAdapter()
relationship_adapter = RelationshipAdapter()
triple_adapter = TripleAdapter()
query_adapter = QueryAdapter()
