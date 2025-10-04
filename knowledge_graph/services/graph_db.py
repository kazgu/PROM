import logging
from typing import List, Dict, Any, Optional, Tuple, Union
import uuid
import json
from datetime import datetime

from neo4j import GraphDatabase
from django.conf import settings
from django.http import Http404

from knowledge_graph.services.mongodb_adapter import entity_adapter, relationship_adapter, triple_adapter, query_adapter

logger = logging.getLogger(__name__)

class Neo4jGraphDB:
    """Service for interacting with the Neo4j graph database."""
    
    def __init__(self, uri=None, username=None, password=None):
        """Initialize the Neo4j connection."""
        self.uri = uri or settings.NEO4J_URI
        self.username = username or settings.NEO4J_USERNAME
        self.password = password or settings.NEO4J_PASSWORD
        self._driver = None
    
    @property
    def driver(self):
        """Lazy load the Neo4j driver."""
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                self.uri, 
                auth=(self.username, self.password)
            )
        return self._driver
    
    def close(self):
        """Close the Neo4j driver."""
        if self._driver is not None:
            self._driver.close()
            self._driver = None
    
    def sync_entity(self, entity: Dict[str, Any]) -> str:
        """Sync an entity to Neo4j."""
        cypher = """
        MERGE (e:Entity {id: $id})
        SET e.name = $name,
            e.normalized_name = $normalized_name,
            e.entity_type = $entity_type,
            e.context = $context,
            e.created_at = $created_at,
            e.updated_at = $updated_at,
            e.properties = $properties
        RETURN e.id as id
        """
        
        params = {
            "id": str(entity['id']),
            "name": entity['name'],
            "normalized_name": entity['normalized_name'],
            "entity_type": entity.get('entity_type'),
            "context": entity.get('context'),
            "created_at": entity.get('created_at').isoformat() if entity.get('created_at') else None,
            "updated_at": entity.get('updated_at').isoformat() if entity.get('updated_at') else None,
            "properties": json.dumps(entity.get('properties', {}))
        }
        
        with self.driver.session() as session:
            result = session.run(cypher, params)
            return result.single()["id"]
    
    def sync_relationship(self, relationship: Dict[str, Any]) -> str:
        """Sync a relationship type to Neo4j."""
        cypher = """
        MERGE (r:RelationshipType {id: $id})
        SET r.name = $name,
            r.normalized_name = $normalized_name,
            r.context = $context,
            r.created_at = $created_at,
            r.updated_at = $updated_at,
            r.properties = $properties
        RETURN r.id as id
        """
        
        params = {
            "id": str(relationship['id']),
            "name": relationship['name'],
            "normalized_name": relationship['normalized_name'],
            "context": relationship.get('context'),
            "created_at": relationship.get('created_at').isoformat() if relationship.get('created_at') else None,
            "updated_at": relationship.get('updated_at').isoformat() if relationship.get('updated_at') else None,
            "properties": json.dumps(relationship.get('properties', {}))
        }
        
        with self.driver.session() as session:
            result = session.run(cypher, params)
            return result.single()["id"]
    
    def sync_triple(self, triple: Dict[str, Any]) -> str:
        """Sync a triple to Neo4j."""
        # First, ensure subject and object entities exist
        try:
            subject = entity_adapter.get(id=triple['subject_id'])
            object_entity = entity_adapter.get(id=triple['object_id'])
            predicate = relationship_adapter.get(id=triple['predicate_id'])
            
            # Sync entities and relationship to Neo4j
            self.sync_entity(subject)
            self.sync_entity(object_entity)
            self.sync_relationship(predicate)
            
            # Then create the relationship between them
            cypher = """
            MATCH (s:Entity {id: $subject_id})
            MATCH (o:Entity {id: $object_id})
            MATCH (r:RelationshipType {id: $predicate_id})
            MERGE (s)-[rel:RELATES {id: $id, type: r.normalized_name}]->(o)
            SET rel.confidence = $confidence,
                rel.source_text = $source_text,
                rel.created_at = $created_at,
                rel.updated_at = $updated_at,
                rel.extracted_from = $extracted_from
            RETURN rel.id as id
            """
            
            params = {
                "id": str(triple['id']),
                "subject_id": str(triple['subject_id']),
                "predicate_id": str(triple['predicate_id']),
                "object_id": str(triple['object_id']),
                "confidence": triple.get('confidence', 1.0),
                "source_text": triple.get('source_text'),
                "created_at": triple.get('created_at').isoformat() if triple.get('created_at') else None,
                "updated_at": triple.get('updated_at').isoformat() if triple.get('updated_at') else None,
                "extracted_from": str(triple.get('extracted_from')) if triple.get('extracted_from') else None
            }
            
            with self.driver.session() as session:
                result = session.run(cypher, params)
                return result.single()["id"]
        except Http404 as e:
            logger.error(f"Entity or relationship not found for triple {triple['id']}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error syncing triple {triple['id']}: {str(e)}")
            raise
    
    def sync_all_triples(self) -> int:
        """Sync all triples from MongoDB to Neo4j."""
        count = 0
        for triple in triple_adapter.all():
            try:
                self.sync_triple(triple)
                count += 1
            except Exception as e:
                logger.error(f"Error syncing triple {triple.get('id')}: {str(e)}")
        return count
    
    def search_entity(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Search for entities by name."""
        cypher = """
        MATCH (e:Entity)
        WHERE e.name CONTAINS $query OR e.normalized_name CONTAINS $query_lower
        RETURN e.id as id, e.name as name, e.entity_type as entity_type, 
               e.normalized_name as normalized_name, e.properties as properties
        LIMIT $limit
        """
        
        params = {
            "query": query,
            "query_lower": query.lower(),
            "limit": limit
        }
        
        with self.driver.session() as session:
            result = session.run(cypher, params)
            return [dict(record) for record in result]
    
    def get_entity_relationships(self, entity_id: str, direction: str = 'both', 
                               limit: int = 100) -> List[Dict[str, Any]]:
        """Get relationships for a specific entity."""
        if direction == 'outgoing':
            cypher = """
            MATCH (e:Entity {id: $entity_id})-[r:RELATES]->(o:Entity)
            MATCH (rel_type:RelationshipType {normalized_name: r.type})
            RETURN e.id as subject_id, e.name as subject_name, 
                   rel_type.id as relationship_id, rel_type.name as relationship_name,
                   o.id as object_id, o.name as object_name,
                   r.confidence as confidence, r.source_text as source_text
            LIMIT $limit
            """
        elif direction == 'incoming':
            cypher = """
            MATCH (s:Entity)-[r:RELATES]->(e:Entity {id: $entity_id})
            MATCH (rel_type:RelationshipType {normalized_name: r.type})
            RETURN s.id as subject_id, s.name as subject_name, 
                   rel_type.id as relationship_id, rel_type.name as relationship_name,
                   e.id as object_id, e.name as object_name,
                   r.confidence as confidence, r.source_text as source_text
            LIMIT $limit
            """
        else:  # both
            cypher = """
            MATCH (e:Entity {id: $entity_id})
            MATCH path = (e)-[r:RELATES]-(other:Entity)
            MATCH (rel_type:RelationshipType {normalized_name: r.type})
            WITH e, r, other, rel_type, 
                 CASE WHEN startNode(r) = e THEN 'outgoing' ELSE 'incoming' END as direction
            RETURN 
                CASE WHEN direction = 'outgoing' THEN e.id ELSE other.id END as subject_id,
                CASE WHEN direction = 'outgoing' THEN e.name ELSE other.name END as subject_name,
                rel_type.id as relationship_id, 
                rel_type.name as relationship_name,
                CASE WHEN direction = 'outgoing' THEN other.id ELSE e.id END as object_id,
                CASE WHEN direction = 'outgoing' THEN other.name ELSE e.name END as object_name,
                r.confidence as confidence, 
                r.source_text as source_text,
                direction
            LIMIT $limit
            """
        
        params = {
            "entity_id": entity_id,
            "limit": limit
        }
        
        with self.driver.session() as session:
            result = session.run(cypher, params)
            return [dict(record) for record in result]
    
    def execute_query(self, query_text: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute a custom Cypher query."""
        try:
            with self.driver.session() as session:
                result = session.run(query_text, params or {})
                records = [dict(record) for record in result]
                
                # Save the query to MongoDB
                query_obj = query_adapter.create(
                    query_text=query_text,
                    structured_query=params,
                    result={"records": records},
                    created_at=datetime.now()
                )
                
                return {
                    "query_id": query_obj['id'],
                    "records": records
                }
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            # Save the failed query
            query_adapter.create(
                query_text=query_text,
                structured_query=params,
                result={"error": str(e)},
                created_at=datetime.now()
            )
            raise
    
    def path_between(self, start_entity_id: str, end_entity_id: str, 
                    max_depth: int = 4) -> List[Dict[str, Any]]:
        """Find paths between two entities."""
        cypher = """
        MATCH path = shortestPath((s:Entity {id: $start_id})-[r:RELATES*1..{max_depth}]-(e:Entity {id: $end_id}))
        UNWIND relationships(path) as rel
        MATCH (rel_type:RelationshipType {normalized_name: rel.type})
        WITH path, collect({
            relationship_id: rel.id,
            relationship_name: rel_type.name,
            confidence: rel.confidence,
            source_text: rel.source_text,
            from_id: startNode(rel).id,
            from_name: startNode(rel).name,
            to_id: endNode(rel).id,
            to_name: endNode(rel).name
        }) as rels
        RETURN length(path) as path_length, rels
        """
        
        params = {
            "start_id": start_entity_id,
            "end_id": end_entity_id,
            "max_depth": max_depth
        }
        
        with self.driver.session() as session:
            result = session.run(cypher, params)
            return [dict(record) for record in result]
