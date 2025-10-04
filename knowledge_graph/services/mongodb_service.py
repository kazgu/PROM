import logging
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple, Union

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from django.conf import settings

logger = logging.getLogger(__name__)

class MongoDBService:
    """Service for interacting with MongoDB."""
    
    def __init__(self, uri=None, db_name=None):
        """Initialize the MongoDB connection."""
        self.uri = uri or settings.MONGODB_URI
        self.db_name = db_name or settings.MONGODB_NAME
        self._client = None
        self._db = None
    
    @property
    def client(self) -> MongoClient:
        """Lazy load the MongoDB client."""
        if self._client is None:
            self._client = MongoClient(self.uri)
        return self._client
    
    @property
    def db(self) -> Database:
        """Get the MongoDB database."""
        if self._db is None:
            self._db = self.client[self.db_name]
        return self._db
    
    def close(self):
        """Close the MongoDB client."""
        if self._client is not None:
            self._client.close()
            self._client = None
            self._db = None
    
    def get_collection(self, collection_name: str) -> Collection:
        """Get a MongoDB collection."""
        return self.db[collection_name]
    
    # Entity operations
    
    def create_entity(self, entity_data: Dict[str, Any]) -> str:
        """Create a new entity in MongoDB."""
        collection = self.get_collection('entities')
        
        # Ensure ID is a string
        if 'id' not in entity_data:
            entity_data['id'] = str(uuid.uuid4())
        elif not isinstance(entity_data['id'], str):
            entity_data['id'] = str(entity_data['id'])
        
        # Add timestamps if not present
        now = datetime.now()
        if 'created_at' not in entity_data:
            entity_data['created_at'] = now
        if 'updated_at' not in entity_data:
            entity_data['updated_at'] = now
        
        # Ensure properties is a dict
        if 'properties' not in entity_data:
            entity_data['properties'] = {}
        
        # Insert the entity
        result = collection.insert_one(entity_data)
        
        return entity_data['id']
    
    def get_entity(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """Get an entity by ID."""
        collection = self.get_collection('entities')
        return collection.find_one({'id': entity_id})
    
    def update_entity(self, entity_id: str, entity_data: Dict[str, Any]) -> bool:
        """Update an entity."""
        collection = self.get_collection('entities')
        
        # Update timestamp
        entity_data['updated_at'] = datetime.now()
        
        # Remove ID from update data if present
        if 'id' in entity_data:
            del entity_data['id']
        
        result = collection.update_one(
            {'id': entity_id},
            {'$set': entity_data}
        )
        
        return result.modified_count > 0
    
    def delete_entity(self, entity_id: str) -> bool:
        """Delete an entity."""
        collection = self.get_collection('entities')
        result = collection.delete_one({'id': entity_id})
        return result.deleted_count > 0
    
    def list_entities(self, filters: Dict[str, Any] = None, 
                     sort_by: str = 'name', 
                     sort_dir: int = 1,
                     skip: int = 0, 
                     limit: int = 100) -> List[Dict[str, Any]]:
        """List entities with filtering and pagination."""
        collection = self.get_collection('entities')
        
        # Apply filters
        query = filters or {}
        
        # Handle text search if 'name' filter is present
        if 'name' in query and isinstance(query['name'], str):
            query['name'] = {'$regex': query['name'], '$options': 'i'}
        
        # Execute query with pagination
        cursor = collection.find(query)
        
        # Apply sorting
        cursor = cursor.sort(sort_by, sort_dir)
        
        # Apply pagination
        cursor = cursor.skip(skip).limit(limit)
        
        return list(cursor)
    
    def count_entities(self, filters: Dict[str, Any] = None) -> int:
        """Count entities with filtering."""
        collection = self.get_collection('entities')
        query = filters or {}
        return collection.count_documents(query)
    
    # Relationship operations
    
    def create_relationship(self, relationship_data: Dict[str, Any]) -> str:
        """Create a new relationship in MongoDB."""
        collection = self.get_collection('relationships')
        
        # Ensure ID is a string
        if 'id' not in relationship_data:
            relationship_data['id'] = str(uuid.uuid4())
        elif not isinstance(relationship_data['id'], str):
            relationship_data['id'] = str(relationship_data['id'])
        
        # Add timestamps if not present
        now = datetime.now()
        if 'created_at' not in relationship_data:
            relationship_data['created_at'] = now
        if 'updated_at' not in relationship_data:
            relationship_data['updated_at'] = now
        
        # Ensure properties is a dict
        if 'properties' not in relationship_data:
            relationship_data['properties'] = {}
        
        # Insert the relationship
        result = collection.insert_one(relationship_data)
        
        return relationship_data['id']
    
    def get_relationship(self, relationship_id: str) -> Optional[Dict[str, Any]]:
        """Get a relationship by ID."""
        collection = self.get_collection('relationships')
        return collection.find_one({'id': relationship_id})
    
    def update_relationship(self, relationship_id: str, relationship_data: Dict[str, Any]) -> bool:
        """Update a relationship."""
        collection = self.get_collection('relationships')
        
        # Update timestamp
        relationship_data['updated_at'] = datetime.now()
        
        # Remove ID from update data if present
        if 'id' in relationship_data:
            del relationship_data['id']
        
        result = collection.update_one(
            {'id': relationship_id},
            {'$set': relationship_data}
        )
        
        return result.modified_count > 0
    
    def delete_relationship(self, relationship_id: str) -> bool:
        """Delete a relationship."""
        collection = self.get_collection('relationships')
        result = collection.delete_one({'id': relationship_id})
        return result.deleted_count > 0
    
    def list_relationships(self, filters: Dict[str, Any] = None, 
                          sort_by: str = 'name', 
                          sort_dir: int = 1,
                          skip: int = 0, 
                          limit: int = 100) -> List[Dict[str, Any]]:
        """List relationships with filtering and pagination."""
        collection = self.get_collection('relationships')
        
        # Apply filters
        query = filters or {}
        
        # Handle text search if 'name' filter is present
        if 'name' in query and isinstance(query['name'], str):
            query['name'] = {'$regex': query['name'], '$options': 'i'}
        
        # Execute query with pagination
        cursor = collection.find(query)
        
        # Apply sorting
        cursor = cursor.sort(sort_by, sort_dir)
        
        # Apply pagination
        cursor = cursor.skip(skip).limit(limit)
        
        return list(cursor)
    
    def count_relationships(self, filters: Dict[str, Any] = None) -> int:
        """Count relationships with filtering."""
        collection = self.get_collection('relationships')
        query = filters or {}
        return collection.count_documents(query)
    
    # Triple operations
    
    def create_triple(self, triple_data: Dict[str, Any]) -> str:
        """Create a new triple in MongoDB."""
        collection = self.get_collection('triples')
        
        # Ensure ID is a string
        if 'id' not in triple_data:
            triple_data['id'] = str(uuid.uuid4())
        elif not isinstance(triple_data['id'], str):
            triple_data['id'] = str(triple_data['id'])
        
        # Add timestamps if not present
        now = datetime.now()
        if 'created_at' not in triple_data:
            triple_data['created_at'] = now
        if 'updated_at' not in triple_data:
            triple_data['updated_at'] = now
        
        # Ensure subject, predicate, and object IDs are strings
        for field in ['subject_id', 'predicate_id', 'object_id']:
            if field in triple_data and not isinstance(triple_data[field], str):
                triple_data[field] = str(triple_data[field])
        
        # Insert the triple
        result = collection.insert_one(triple_data)
        
        return triple_data['id']
    
    def get_triple(self, triple_id: str) -> Optional[Dict[str, Any]]:
        """Get a triple by ID."""
        collection = self.get_collection('triples')
        return collection.find_one({'id': triple_id})
    
    def update_triple(self, triple_id: str, triple_data: Dict[str, Any]) -> bool:
        """Update a triple."""
        collection = self.get_collection('triples')
        
        # Update timestamp
        triple_data['updated_at'] = datetime.now()
        
        # Remove ID from update data if present
        if 'id' in triple_data:
            del triple_data['id']
        
        result = collection.update_one(
            {'id': triple_id},
            {'$set': triple_data}
        )
        
        return result.modified_count > 0
    
    def delete_triple(self, triple_id: str) -> bool:
        """Delete a triple."""
        collection = self.get_collection('triples')
        result = collection.delete_one({'id': triple_id})
        return result.deleted_count > 0
    
    def list_triples(self, filters: Dict[str, Any] = None, 
                    sort_by: str = 'created_at', 
                    sort_dir: int = -1,
                    skip: int = 0, 
                    limit: int = 100) -> List[Dict[str, Any]]:
        """List triples with filtering and pagination."""
        collection = self.get_collection('triples')
        
        # Apply filters
        query = filters or {}
        
        # Execute query with pagination
        cursor = collection.find(query)
        
        # Apply sorting
        cursor = cursor.sort(sort_by, sort_dir)
        
        # Apply pagination
        cursor = cursor.skip(skip).limit(limit)
        
        return list(cursor)
    
    def count_triples(self, filters: Dict[str, Any] = None) -> int:
        """Count triples with filtering."""
        collection = self.get_collection('triples')
        query = filters or {}
        return collection.count_documents(query)
    
    # Query operations
    
    def create_query(self, query_data: Dict[str, Any]) -> str:
        """Create a new query in MongoDB."""
        collection = self.get_collection('queries')
        
        # Ensure ID is a string
        if 'id' not in query_data:
            query_data['id'] = str(uuid.uuid4())
        elif not isinstance(query_data['id'], str):
            query_data['id'] = str(query_data['id'])
        
        # Add timestamp if not present
        if 'created_at' not in query_data:
            query_data['created_at'] = datetime.now()
        
        # Insert the query
        result = collection.insert_one(query_data)
        
        return query_data['id']
    
    def get_query(self, query_id: str) -> Optional[Dict[str, Any]]:
        """Get a query by ID."""
        collection = self.get_collection('queries')
        return collection.find_one({'id': query_id})
    
    def list_queries(self, filters: Dict[str, Any] = None, 
                    sort_by: str = 'created_at', 
                    sort_dir: int = -1,
                    skip: int = 0, 
                    limit: int = 100) -> List[Dict[str, Any]]:
        """List queries with filtering and pagination."""
        collection = self.get_collection('queries')
        
        # Apply filters
        query = filters or {}
        
        # Execute query with pagination
        cursor = collection.find(query)
        
        # Apply sorting
        cursor = cursor.sort(sort_by, sort_dir)
        
        # Apply pagination
        cursor = cursor.skip(skip).limit(limit)
        
        return list(cursor)
    
    # API Key operations
    
    def create_api_key(self, api_key_data: Dict[str, Any]) -> str:
        """Create a new API key in MongoDB."""
        collection = self.get_collection('api_keys')
        
        # Ensure ID is a string
        if 'id' not in api_key_data:
            api_key_data['id'] = str(uuid.uuid4())
        elif not isinstance(api_key_data['id'], str):
            api_key_data['id'] = str(api_key_data['id'])
        
        # Add timestamps if not present
        now = datetime.now()
        if 'created_at' not in api_key_data:
            api_key_data['created_at'] = now
        
        # Ensure allowed_models is a list
        if 'allowed_models' not in api_key_data:
            api_key_data['allowed_models'] = []
        
        # Insert the API key
        result = collection.insert_one(api_key_data)
        
        return api_key_data['id']
    
    def get_api_key(self, api_key_id: str) -> Optional[Dict[str, Any]]:
        """Get an API key by ID."""
        collection = self.get_collection('api_keys')
        return collection.find_one({'id': api_key_id})
    
    def get_api_key_by_key(self, key: str) -> Optional[Dict[str, Any]]:
        """Get an API key by the key value."""
        collection = self.get_collection('api_keys')
        return collection.find_one({'key': key})
    
    def update_api_key(self, api_key_id: str, api_key_data: Dict[str, Any]) -> bool:
        """Update an API key."""
        collection = self.get_collection('api_keys')
        
        # Remove ID from update data if present
        if 'id' in api_key_data:
            del api_key_data['id']
        
        # Remove key from update data if present (key should not be updated)
        if 'key' in api_key_data:
            del api_key_data['key']
        
        result = collection.update_one(
            {'id': api_key_id},
            {'$set': api_key_data}
        )
        
        return result.modified_count > 0
    
    def delete_api_key(self, api_key_id: str) -> bool:
        """Delete an API key."""
        collection = self.get_collection('api_keys')
        result = collection.delete_one({'id': api_key_id})
        return result.deleted_count > 0
    
    def list_api_keys(self, filters: Dict[str, Any] = None, 
                     sort_by: str = 'name', 
                     sort_dir: int = 1,
                     skip: int = 0, 
                     limit: int = 100) -> List[Dict[str, Any]]:
        """List API keys with filtering and pagination."""
        collection = self.get_collection('api_keys')
        
        # Apply filters
        query = filters or {}
        
        # Execute query with pagination
        cursor = collection.find(query)
        
        # Apply sorting
        cursor = cursor.sort(sort_by, sort_dir)
        
        # Apply pagination
        cursor = cursor.skip(skip).limit(limit)
        
        return list(cursor)
    
    # API Request operations
    
    def create_api_request(self, api_request_data: Dict[str, Any]) -> str:
        """Create a new API request in MongoDB."""
        collection = self.get_collection('api_requests')
        
        # Ensure ID is a string
        if 'id' not in api_request_data:
            api_request_data['id'] = str(uuid.uuid4())
        elif not isinstance(api_request_data['id'], str):
            api_request_data['id'] = str(api_request_data['id'])
        
        # Add timestamp if not present
        if 'timestamp' not in api_request_data:
            api_request_data['timestamp'] = datetime.now()
        
        # Ensure api_key_id and provider_used_id are strings
        for field in ['api_key_id', 'provider_used_id']:
            if field in api_request_data and not isinstance(api_request_data[field], str):
                api_request_data[field] = str(api_request_data[field])
        
        # Insert the API request
        result = collection.insert_one(api_request_data)
        
        return api_request_data['id']
    
    def get_api_request(self, api_request_id: str) -> Optional[Dict[str, Any]]:
        """Get an API request by ID."""
        collection = self.get_collection('api_requests')
        return collection.find_one({'id': api_request_id})
    
    def list_api_requests(self, filters: Dict[str, Any] = None, 
                         sort_by: str = 'timestamp', 
                         sort_dir: int = -1,
                         skip: int = 0, 
                         limit: int = 100) -> List[Dict[str, Any]]:
        """List API requests with filtering and pagination."""
        collection = self.get_collection('api_requests')
        
        # Apply filters
        query = filters or {}
        
        # Execute query with pagination
        cursor = collection.find(query)
        
        # Apply sorting
        cursor = cursor.sort(sort_by, sort_dir)
        
        # Apply pagination
        cursor = cursor.skip(skip).limit(limit)
        
        return list(cursor)
    
    def count_api_requests(self, filters: Dict[str, Any] = None) -> int:
        """Count API requests with filtering."""
        collection = self.get_collection('api_requests')
        query = filters or {}
        return collection.count_documents(query)
    
    # User operations
    
    def create_user(self, user_data: Dict[str, Any]) -> str:
        """Create a new user in MongoDB."""
        collection = self.get_collection('users')
        
        # Ensure ID is a string
        if 'id' not in user_data:
            user_data['id'] = str(uuid.uuid4())
        elif not isinstance(user_data['id'], str):
            user_data['id'] = str(user_data['id'])
        
        # Add timestamps if not present
        now = datetime.now()
        if 'date_joined' not in user_data:
            user_data['date_joined'] = now
        
        # Ensure preferences is a dict
        if 'preferences' not in user_data:
            user_data['preferences'] = {}
        
        # Insert the user
        result = collection.insert_one(user_data)
        
        return user_data['id']
    
    def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get a user by ID."""
        collection = self.get_collection('users')
        return collection.find_one({'id': user_id})
    
    def get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """Get a user by username."""
        collection = self.get_collection('users')
        return collection.find_one({'username': username})
    
    def update_user(self, user_id: str, user_data: Dict[str, Any]) -> bool:
        """Update a user."""
        collection = self.get_collection('users')
        
        # Remove ID from update data if present
        if 'id' in user_data:
            del user_data['id']
        
        result = collection.update_one(
            {'id': user_id},
            {'$set': user_data}
        )
        
        return result.modified_count > 0
    
    def delete_user(self, user_id: str) -> bool:
        """Delete a user."""
        collection = self.get_collection('users')
        result = collection.delete_one({'id': user_id})
        return result.deleted_count > 0
    
    def list_users(self, filters: Dict[str, Any] = None, 
                  sort_by: str = 'username', 
                  sort_dir: int = 1,
                  skip: int = 0, 
                  limit: int = 100) -> List[Dict[str, Any]]:
        """List users with filtering and pagination."""
        collection = self.get_collection('users')
        
        # Apply filters
        query = filters or {}
        
        # Execute query with pagination
        cursor = collection.find(query)
        
        # Apply sorting
        cursor = cursor.sort(sort_by, sort_dir)
        
        # Apply pagination
        cursor = cursor.skip(skip).limit(limit)
        
        return list(cursor)
    
    # Migration helpers
    
    def migrate_from_django(self, model_class, collection_name: str, 
                           transform_func=None) -> int:
        """Migrate data from Django model to MongoDB collection."""
        collection = self.get_collection(collection_name)
        
        # Get all objects from the Django model
        objects = model_class.objects.all()
        count = 0
        
        for obj in objects:
            # Convert Django model instance to dict
            if transform_func:
                # Use custom transform function if provided
                data = transform_func(obj)
            else:
                # Default transformation
                data = {field.name: getattr(obj, field.name) 
                       for field in obj._meta.fields}
                
                # Ensure ID is a string
                if 'id' in data:
                    data['id'] = str(data['id'])
                
                # Convert datetime objects to native Python datetime
                for key, value in data.items():
                    if hasattr(value, 'isoformat'):  # Check if it's a datetime-like object
                        data[key] = value
            
            # Insert into MongoDB
            try:
                collection.insert_one(data)
                count += 1
            except Exception as e:
                logger.error(f"Error migrating {collection_name} object {getattr(obj, 'id', 'unknown')}: {str(e)}")
        
        return count
