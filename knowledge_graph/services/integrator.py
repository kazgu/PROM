import logging
import json
import uuid
from typing import List, Dict, Any, Optional, Set, Tuple
import re

from django.conf import settings

from knowledge_graph.services.mongodb_adapter import entity_adapter, relationship_adapter, triple_adapter
from api_proxy.services.mongodb_adapter import external_api_config_adapter
from knowledge_graph.services.graph_db import Neo4jGraphDB
from api_proxy.services.openai import OpenAIClient

logger = logging.getLogger(__name__)

class KnowledgeIntegrator:
    """
    Service for integrating new knowledge into the existing knowledge graph.
    
    This service is responsible for:
    1. Finding connections between new entities and existing entities
    2. Discovering implicit relationships between entities
    3. Generating new triples to connect isolated parts of the knowledge graph
    4. Using LLMs to infer relationships between entities that might not be explicitly stated
    """
    
    def __init__(self, openai_client=None, neo4j_client=None):
        """Initialize the integrator with optional OpenAI and Neo4j clients."""
        # Initialize OpenAI client
        if openai_client:
            self.openai_client = openai_client
        else:
            # Try to create a client using admin API key if available
            try:
                api_configs = external_api_config_adapter.filter(api_type='openai', is_active=True)
                if api_configs and len(api_configs) > 0:
                    api_config = api_configs[0]
                    self.openai_client = OpenAIClient(api_key=api_config.get('api_key'), api_base=api_config.get('api_base'))
                else:
                    self.openai_client = None
            except Exception as e:
                logger.error(f"Could not initialize OpenAI client: {str(e)}")
                self.openai_client = None
        
        # Initialize Neo4j client
        self.neo4j_client = neo4j_client or Neo4jGraphDB()
    
    def integrate_new_entity(self, entity: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Integrate a new entity into the knowledge graph by finding connections
        to existing entities and generating new triples.
        
        Args:
            entity: The new entity to integrate
            
        Returns:
            List of newly created triples
        """
        logger.info(f"Integrating new entity: {entity.get('name')} ({entity.get('id')})")
        
        # Find potential connections using various methods
        new_triples = []
        
        # 1. Find connections based on name similarity
        name_triples = self._find_connections_by_name(entity)
        new_triples.extend(name_triples)
        
        # 2. Find connections based on entity type
        type_triples = self._find_connections_by_type(entity)
        new_triples.extend(type_triples)
        
        # 3. Use LLM to infer relationships if available
        if self.openai_client:
            llm_triples = self._infer_relationships_with_llm(entity)
            new_triples.extend(llm_triples)
        
        # 4. Find connections using graph-based methods
        graph_triples = self._find_connections_by_graph_analysis(entity)
        new_triples.extend(graph_triples)
        
        logger.info(f"Created {len(new_triples)} new triples for entity {entity.get('name')}")
        return new_triples
    
    def integrate_new_relationship(self, relationship: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Integrate a new relationship type into the knowledge graph by finding
        potential entity pairs that might be connected by this relationship.
        
        Args:
            relationship: The new relationship to integrate
            
        Returns:
            List of newly created triples
        """
        logger.info(f"Integrating new relationship: {relationship.get('name')} ({relationship.get('id')})")
        
        # Find potential entity pairs for this relationship
        new_triples = []
        
        # 1. Use LLM to suggest entity pairs for this relationship if available
        if self.openai_client:
            llm_triples = self._suggest_entity_pairs_for_relationship(relationship)
            new_triples.extend(llm_triples)
        
        logger.info(f"Created {len(new_triples)} new triples for relationship {relationship.get('name')}")
        return new_triples
    
    def integrate_new_triple(self, triple: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Integrate a new triple into the knowledge graph by finding connections
        to other triples and inferring new relationships.
        
        Args:
            triple: The new triple to integrate
            
        Returns:
            List of newly created triples
        """
        logger.info(f"Integrating new triple: {triple}")
        
        # Find potential new triples based on this triple
        new_triples = []
        
        # 1. Find transitive relationships
        transitive_triples = self._find_transitive_relationships(triple)
        new_triples.extend(transitive_triples)
        
        # 2. Find symmetric relationships
        symmetric_triples = self._find_symmetric_relationships(triple)
        new_triples.extend(symmetric_triples)
        
        # 3. Use LLM to infer new triples if available
        if self.openai_client:
            llm_triples = self._infer_triples_with_llm(triple)
            new_triples.extend(llm_triples)
        
        logger.info(f"Created {len(new_triples)} new triples based on triple {triple}")
        return new_triples
    
    def integrate_batch(self, entities: List[Dict[str, Any]] = None, 
                      relationships: List[Dict[str, Any]] = None,
                      triples: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Integrate a batch of new entities, relationships, and triples into the knowledge graph.
        
        Args:
            entities: List of new entities to integrate
            relationships: List of new relationships to integrate
            triples: List of new triples to integrate
            
        Returns:
            List of newly created triples
        """
        all_new_triples = []
        
        # Process entities if provided
        if entities:
            for entity in entities:
                entity_triples = self.integrate_new_entity(entity)
                all_new_triples.extend(entity_triples)
        
        # Process relationships if provided
        if relationships:
            for relationship in relationships:
                rel_triples = self.integrate_new_relationship(relationship)
                all_new_triples.extend(rel_triples)
        
        # Process triples if provided
        if triples:
            for triple in triples:
                triple_triples = self.integrate_new_triple(triple)
                all_new_triples.extend(triple_triples)
        
        # Find connections between newly created triples
        if all_new_triples:
            # Get unique entities from new triples
            new_entities = set()
            for triple in all_new_triples:
                # Get subject and object entities
                try:
                    subject = entity_adapter.get(id=triple.get('subject_id'))
                    object_entity = entity_adapter.get(id=triple.get('object_id'))
                    new_entities.add(subject.get('id'))
                    new_entities.add(object_entity.get('id'))
                except Exception as e:
                    logger.error(f"Error getting entities for triple: {str(e)}")
            
            # Find connections between these entities
            for entity_id in new_entities:
                try:
                    entity = entity_adapter.get(id=entity_id)
                    entity_triples = self._find_connections_within_set(entity, new_entities)
                    all_new_triples.extend(entity_triples)
                except Exception as e:
                    logger.error(f"Error finding connections for entity {entity_id}: {str(e)}")
        
        return all_new_triples
    
    def integrate_all(self) -> int:
        """
        Run integration on the entire knowledge graph to find and create
        missing connections between existing entities.
        
        Returns:
            Number of new triples created
        """
        logger.info("Starting full knowledge graph integration")
        
        # Get all entities
        entities = entity_adapter.all()
        total_entities = len(entities)
        
        # Process in batches to avoid memory issues
        batch_size = 100
        new_triples_count = 0
        
        for i in range(0, total_entities, batch_size):
            batch = entities[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} entities)")
            
            # Integrate each entity in the batch
            for entity in batch:
                entity_triples = self.integrate_new_entity(entity)
                new_triples_count += len(entity_triples)
        
        logger.info(f"Full integration complete. Created {new_triples_count} new triples")
        return new_triples_count
    
    def _find_connections_by_name(self, entity: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find connections to other entities based on name similarity."""
        new_triples = []
        
        # Find entities with similar names, filtering by API key if available
        entity_id = entity.get('id')
        entity_name = entity.get('normalized_name', '').lower()
        
        # Skip if no name
        if not entity_name:
            return []
        
        # Build query for similar entities
        query_params = {}
        
        # Exclude the entity itself
        if entity_id:
            query_params['id__ne'] = entity_id
        
        # Find entities with similar names
        words = [word for word in entity_name.split() if len(word) > 3]
        similar_entities = []
        
        # Try to find by name contains
        if entity_name:
            name_matches = entity_adapter.filter(normalized_name__contains=entity_name, **query_params)
            similar_entities.extend(name_matches)
        
        # Try to find by words
        for word in words:
            word_matches = entity_adapter.filter(normalized_name__contains=word, **query_params)
            similar_entities.extend(word_matches)
        
        # Remove duplicates
        seen_ids = set()
        unique_similar_entities = []
        for e in similar_entities:
            if e.get('id') not in seen_ids:
                seen_ids.add(e.get('id'))
                unique_similar_entities.append(e)
        
        # Limit to 10 similar entities
        similar_entities = unique_similar_entities[:10]
        
        if not similar_entities:
            return []
        
        # Create "related to" relationship for similar entities
        related_rels = relationship_adapter.filter(normalized_name="related to")
        if related_rels:
            related_to = related_rels[0]
        else:
            related_to = relationship_adapter.create(
                name="related to",
                normalized_name="related to"
            )
        
        # Create triples connecting the entity to similar entities
        for similar_entity in similar_entities:
            # Skip if a triple already exists between these entities
            existing_triples = triple_adapter.filter(
                subject_id=entity.get('id'),
                object_id=similar_entity.get('id')
            )
            
            if existing_triples:
                continue
            
            # Create a new triple
            triple = triple_adapter.create(
                subject_id=entity.get('id'),
                predicate_id=related_to.get('id'),
                object_id=similar_entity.get('id'),
                confidence=0.6,
                source_text=f"Name similarity between {entity.get('name')} and {similar_entity.get('name')}"
            )
            
            new_triples.append(triple)
            
            # Also sync to Neo4j
            try:
                self.neo4j_client.sync_triple(triple)
            except Exception as e:
                logger.error(f"Error syncing triple to Neo4j: {str(e)}")
        
        return new_triples
    
    def _find_connections_by_type(self, entity: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find connections to other entities based on entity type."""
        new_triples = []
        
        # Skip if entity has no type
        entity_type = entity.get('entity_type')
        if not entity_type:
            return []
        
        # Find entities with the same type, filtering by API key if available
        query_params = {
            'entity_type': entity_type
        }
        
        # Exclude the entity itself
        if entity.get('id'):
            query_params['id__ne'] = entity.get('id')
        
        # If entity has an API key, only find entities with the same API key
        if entity.get('api_key_id'):
            query_params['api_key_id'] = entity.get('api_key_id')
        
        # Get up to 5 entities with the same type
        same_type_entities = entity_adapter.filter(**query_params)[:5]
        
        if not same_type_entities:
            return []
        
        # Create "same type as" relationship
        same_type_rels = relationship_adapter.filter(normalized_name="same type as")
        if same_type_rels:
            same_type_as = same_type_rels[0]
        else:
            same_type_as = relationship_adapter.create(
                name="same type as",
                normalized_name="same type as"
            )
        
        # Create triples connecting the entity to same-type entities
        for same_type_entity in same_type_entities:
            # Skip if a triple already exists between these entities
            existing_triples = triple_adapter.filter(
                subject_id=entity.get('id'),
                object_id=same_type_entity.get('id')
            )
            
            if existing_triples:
                continue
            
            # Create a new triple
            triple = triple_adapter.create(
                subject_id=entity.get('id'),
                predicate_id=same_type_as.get('id'),
                object_id=same_type_entity.get('id'),
                confidence=0.7,
                source_text=f"Both entities are of type: {entity_type}"
            )
            
            new_triples.append(triple)
            
            # Also sync to Neo4j
            try:
                self.neo4j_client.sync_triple(triple)
            except Exception as e:
                logger.error(f"Error syncing triple to Neo4j: {str(e)}")
        
        return new_triples
    
    def _find_connections_by_graph_analysis(self, entity: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find connections using graph analysis techniques."""
        new_triples = []
        
        # Skip if entity has no ID
        entity_id = entity.get('id')
        if not entity_id:
            return []
        
        # Skip if entity has no connections yet
        entity_triples = triple_adapter.filter(subject_id=entity_id) + triple_adapter.filter(object_id=entity_id)
        
        if not entity_triples:
            return []
        
        # Find potential connections through common neighbors (2-hop connections)
        cypher = """
        MATCH (e:Entity {id: $entity_id})-[r1:RELATES]-(n:Entity)-[r2:RELATES]-(other:Entity)
        WHERE other.id <> $entity_id
        AND NOT (e)-[:RELATES]-(other)
        WITH other, count(n) as common_neighbors, collect(n.name) as shared_neighbors
        WHERE common_neighbors >= 2
        RETURN other.id as id, other.name as name, other.entity_type as entity_type,
               common_neighbors, shared_neighbors
        ORDER BY common_neighbors DESC
        LIMIT 5
        """
        
        try:
            with self.neo4j_client.driver.session() as session:
                result = session.run(cypher, {"entity_id": str(entity_id)})
                potential_connections = [dict(record) for record in result]
                
                if not potential_connections:
                    return []
                
                # Create "connected through" relationship
                connected_through_rels = relationship_adapter.filter(normalized_name="connected through")
                if connected_through_rels:
                    connected_through = connected_through_rels[0]
                else:
                    connected_through = relationship_adapter.create(
                        name="connected through",
                        normalized_name="connected through"
                    )
                
                # Create triples for potential connections
                for connection in potential_connections:
                    try:
                        # Get the other entity
                        other_entity_id = connection.get('id')
                        if not other_entity_id:
                            continue
                        
                        other_entity = entity_adapter.get(id=other_entity_id)
                        
                        # Skip if a triple already exists between these entities
                        existing_triples = triple_adapter.filter(
                            subject_id=entity_id,
                            object_id=other_entity_id
                        )
                        
                        if existing_triples:
                            continue
                        
                        # Create a new triple
                        confidence = min(0.5 + (connection.get('common_neighbors', 0) * 0.1), 0.9)
                        shared_neighbors = connection.get('shared_neighbors', [])
                        shared_text = ', '.join(shared_neighbors[:3]) if shared_neighbors else "common entities"
                        
                        triple = triple_adapter.create(
                            subject_id=entity_id,
                            predicate_id=connected_through.get('id'),
                            object_id=other_entity_id,
                            confidence=confidence,
                            source_text=f"Connected through common entities: {shared_text}"
                        )
                        
                        new_triples.append(triple)
                        
                        # Also sync to Neo4j
                        try:
                            self.neo4j_client.sync_triple(triple)
                        except Exception as e:
                            logger.error(f"Error syncing triple to Neo4j: {str(e)}")
                            
                    except Exception as e:
                        logger.warning(f"Error processing connection to entity {connection.get('id')}: {str(e)}")
                        continue
                    
        except Exception as e:
            logger.error(f"Error in graph analysis: {str(e)}")
        
        return new_triples
    
    def _find_transitive_relationships(self, triple: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find transitive relationships based on a new triple."""
        new_triples = []
        
        # Get subject, predicate, and object IDs
        subject_id = triple.get('subject_id')
        predicate_id = triple.get('predicate_id')
        object_id = triple.get('object_id')
        
        if not subject_id or not predicate_id or not object_id:
            return []
        
        # Get the predicate
        try:
            predicate = relationship_adapter.get(id=predicate_id)
        except Exception as e:
            logger.error(f"Error getting predicate: {str(e)}")
            return []
        
        # Check for potential transitive relationships
        # If A -> B and B -> C, then potentially A -> C
        
        # First, check if the object of this triple is the subject of other triples
        object_as_subject_triples = triple_adapter.filter(subject_id=object_id)
        
        for oas_triple in object_as_subject_triples:
            # Skip if a triple already exists between these entities
            existing_triples = triple_adapter.filter(
                subject_id=subject_id,
                object_id=oas_triple.get('object_id')
            )
            
            if existing_triples:
                continue
            
            # Get the predicate of the second triple
            try:
                oas_predicate = relationship_adapter.get(id=oas_triple.get('predicate_id'))
            except Exception as e:
                logger.error(f"Error getting predicate: {str(e)}")
                continue
            
            # Check if the predicates are compatible for transitivity
            if self._are_predicates_transitive(predicate, oas_predicate):
                # Create a new transitive relationship
                transitive_name = f"transitive_{predicate.get('normalized_name')}_{oas_predicate.get('normalized_name')}"
                transitive_rels = relationship_adapter.filter(normalized_name=transitive_name)
                
                if transitive_rels:
                    transitive_rel = transitive_rels[0]
                else:
                    transitive_rel = relationship_adapter.create(
                        name=f"transitive {predicate.get('name')} {oas_predicate.get('name')}",
                        normalized_name=transitive_name
                    )
                
                # Create a new triple
                confidence = min(triple.get('confidence', 0.8), oas_triple.get('confidence', 0.8)) * 0.9
                
                # Get subject and object entities for source text
                try:
                    subject = entity_adapter.get(id=subject_id)
                    object_entity = entity_adapter.get(id=oas_triple.get('object_id'))
                    intermediate = entity_adapter.get(id=object_id)
                    
                    source_text = f"Inferred from: {subject.get('name')} {predicate.get('name')} {intermediate.get('name')} and {intermediate.get('name')} {oas_predicate.get('name')} {object_entity.get('name')}"
                except Exception as e:
                    source_text = "Inferred from transitive relationship"
                
                new_triple = triple_adapter.create(
                    subject_id=subject_id,
                    predicate_id=transitive_rel.get('id'),
                    object_id=oas_triple.get('object_id'),
                    confidence=confidence,
                    source_text=source_text
                )
                
                new_triples.append(new_triple)
                
                # Also sync to Neo4j
                try:
                    self.neo4j_client.sync_triple(new_triple)
                except Exception as e:
                    logger.error(f"Error syncing triple to Neo4j: {str(e)}")
        
        # Second, check if the subject of this triple is the object of other triples
        subject_as_object_triples = triple_adapter.filter(object_id=subject_id)
        
        for sao_triple in subject_as_object_triples:
            # Skip if a triple already exists between these entities
            existing_triples = triple_adapter.filter(
                subject_id=sao_triple.get('subject_id'),
                object_id=object_id
            )
            
            if existing_triples:
                continue
            
            # Get the predicate of the second triple
            try:
                sao_predicate = relationship_adapter.get(id=sao_triple.get('predicate_id'))
            except Exception as e:
                logger.error(f"Error getting predicate: {str(e)}")
                continue
            
            # Check if the predicates are compatible for transitivity
            if self._are_predicates_transitive(sao_predicate, predicate):
                # Create a new transitive relationship
                transitive_name = f"transitive_{sao_predicate.get('normalized_name')}_{predicate.get('normalized_name')}"
                transitive_rels = relationship_adapter.filter(normalized_name=transitive_name)
                
                if transitive_rels:
                    transitive_rel = transitive_rels[0]
                else:
                    transitive_rel = relationship_adapter.create(
                        name=f"transitive {sao_predicate.get('name')} {predicate.get('name')}",
                        normalized_name=transitive_name
                    )
                
                # Create a new triple
                confidence = min(triple.get('confidence', 0.8), sao_triple.get('confidence', 0.8)) * 0.9
                
                # Get subject and object entities for source text
                try:
                    subject = entity_adapter.get(id=sao_triple.get('subject_id'))
                    object_entity = entity_adapter.get(id=object_id)
                    intermediate = entity_adapter.get(id=subject_id)
                    
                    source_text = f"Inferred from: {subject.get('name')} {sao_predicate.get('name')} {intermediate.get('name')} and {intermediate.get('name')} {predicate.get('name')} {object_entity.get('name')}"
                except Exception as e:
                    source_text = "Inferred from transitive relationship"
                
                new_triple = triple_adapter.create(
                    subject_id=sao_triple.get('subject_id'),
                    predicate_id=transitive_rel.get('id'),
                    object_id=object_id,
                    confidence=confidence,
                    source_text=source_text
                )
                
                new_triples.append(new_triple)
                
                # Also sync to Neo4j
                try:
                    self.neo4j_client.sync_triple(new_triple)
                except Exception as e:
                    logger.error(f"Error syncing triple to Neo4j: {str(e)}")
        
        return new_triples
    
    def _are_predicates_transitive(self, pred1: Dict[str, Any], pred2: Dict[str, Any]) -> bool:
        """Check if two predicates can form a transitive relationship."""
        # This is a simplified implementation
        # In a real system, you would have a more sophisticated way to determine transitivity
        
        # Some common transitive relationships
        transitive_pairs = [
            # If A is part of B and B is part of C, then A is part of C
            ('part of', 'part of'),
            # If A is located in B and B is located in C, then A is located in C
            ('located in', 'located in'),
            # If A is a subclass of B and B is a subclass of C, then A is a subclass of C
            ('subclass of', 'subclass of'),
            ('is a', 'is a'),
            # If A is owned by B and B is owned by C, then A is indirectly owned by C
            ('owned by', 'owned by'),
            # If A is a member of B and B is a subset of C, then A is a member of C
            ('member of', 'subset of'),
        ]
        
        # Check if the predicates form a known transitive pair
        pred1_name = pred1.get('normalized_name', '')
        pred2_name = pred2.get('normalized_name', '')
        
        return (pred1_name, pred2_name) in transitive_pairs
    
    def _find_symmetric_relationships(self, triple: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find symmetric relationships based on a new triple."""
        new_triples = []
        
        # Get subject, predicate, and object IDs
        subject_id = triple.get('subject_id')
        predicate_id = triple.get('predicate_id')
        object_id = triple.get('object_id')
        
        if not subject_id or not predicate_id or not object_id:
            return []
        
        # Get the predicate
        try:
            predicate = relationship_adapter.get(id=predicate_id)
        except Exception as e:
            logger.error(f"Error getting predicate: {str(e)}")
            return []
        
        # Check if the predicate is symmetric
        if self._is_predicate_symmetric(predicate):
            # Skip if the reverse triple already exists
            existing_triples = triple_adapter.filter(
                subject_id=object_id,
                predicate_id=predicate_id,
                object_id=subject_id
            )
            
            if existing_triples:
                return []
            
            # Get subject and object entities for source text
            try:
                subject = entity_adapter.get(id=subject_id)
                object_entity = entity_adapter.get(id=object_id)
                
                source_text = f"Symmetric relationship of: {subject.get('name')} {predicate.get('name')} {object_entity.get('name')}"
            except Exception as e:
                source_text = "Symmetric relationship"
            
            # Create the symmetric triple
            symmetric_triple = triple_adapter.create(
                subject_id=object_id,
                predicate_id=predicate_id,
                object_id=subject_id,
                confidence=triple.get('confidence', 0.8),
                source_text=source_text
            )
            
            new_triples.append(symmetric_triple)
            
            # Also sync to Neo4j
            try:
                self.neo4j_client.sync_triple(symmetric_triple)
            except Exception as e:
                logger.error(f"Error syncing triple to Neo4j: {str(e)}")
        
        return new_triples
    
    def _is_predicate_symmetric(self, predicate: Dict[str, Any]) -> bool:
        """Check if a predicate represents a symmetric relationship."""
        # This is a simplified implementation
        # In a real system, you would have a more sophisticated way to determine symmetry
        
        # Some common symmetric relationships
        symmetric_predicates = [
            'similar to',
            'related to',
            'connected to',
            'married to',
            'friend of',
            'colleague of',
            'sibling of',
            'same as',
            'equivalent to',
            'same type as',
        ]
        
        return predicate.get('normalized_name', '') in symmetric_predicates
    
    def _infer_relationships_with_llm(self, entity: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Use LLM to infer relationships between a new entity and existing entities."""
        if not self.openai_client:
            return []
        
        new_triples = []
        
        # Get some existing entities to compare with
        # Prioritize entities with more connections
        entity_id = entity.get('id')
        if not entity_id:
            return []
        
        # Build query for existing entities
        query_params = {}
        
        # Exclude the entity itself
        query_params['id__ne'] = entity_id
        
        # If entity has an API key, only find entities with the same API key
        if entity.get('api_key_id'):
            query_params['api_key_id'] = entity.get('api_key_id')
        
        # Get top 10 entities with the most connections
        # Since we can't easily count connections in MongoDB, we'll just get some entities
        existing_entities = entity_adapter.filter(**query_params)[:10]
        
        if not existing_entities:
            return []
        
        # Create a prompt to infer relationships
        system_prompt = """
        You are a knowledge graph relationship inference system. Your task is to infer potential relationships between entities.
        
        I will provide you with a new entity and a list of existing entities in our knowledge graph. For each existing entity, if you can infer a meaningful relationship with the new entity, provide it in the specified format.
        
        Return your response as a JSON list of inferred relationships in this format:
        [
            {
                "subject": "new entity name",
                "predicate": "relationship name",
                "object": "existing entity name",
                "confidence": 0.8,
                "explanation": "brief explanation of why this relationship exists"
            },
            {
                "subject": "existing entity name",
                "predicate": "relationship name",
                "object": "new entity name",
                "confidence": 0.7,
                "explanation": "brief explanation of why this relationship exists"
            }
        ]
        
        If no relationships can be inferred, return an empty list: []
        
        Only include relationships that are reasonably likely to be true. Assign lower confidence scores (0.5-0.7) for relationships that are more speculative.
        """
        
        user_prompt = f"""
        New entity: {entity.get('name')} (Type: {entity.get('entity_type') or 'Unknown'})
        
        Existing entities:
        {', '.join([f"{e.get('name')} (Type: {e.get('entity_type') or 'Unknown'})" for e in existing_entities])}
        
        Infer potential relationships between the new entity and the existing entities.
        """
        
        # Send the request to the LLM
        try:
            response = self.openai_client.chat_completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                model="gpt-4",
                temperature=0.2,
                max_tokens=2000,
            )
            
            # Extract and parse the relationships from the response
            if response.get('status_code') == 200 and 'response' in response:
                content = response['response']['choices'][0]['message']['content']
                
                # Find JSON in the response
                json_match = re.search(r'\[\s*\{.*\}\s*\]', content, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                    try:
                        inferred_relationships = json.loads(json_str)
                        
                        # Create triples from inferred relationships
                        for rel in inferred_relationships:
                            # Find the subject and object entities
                            if rel['subject'] == entity.get('name'):
                                subject_id = entity_id
                                object_name = rel['object']
                                object_entity = next((e for e in existing_entities if e.get('name') == object_name), None)
                                if not object_entity:
                                    continue
                                object_id = object_entity.get('id')
                            elif rel['object'] == entity.get('name'):
                                subject_name = rel['subject']
                                subject_entity = next((e for e in existing_entities if e.get('name') == subject_name), None)
                                if not subject_entity:
                                    continue
                                subject_id = subject_entity.get('id')
                                object_id = entity_id
                            else:
                                # Skip if neither subject nor object is the main entity
                                continue
                            
                            # Get or create the predicate with context
                            explanation = rel.get('explanation', f"Inferred relationship between {rel['subject']} and {rel['object']}")
                            predicate_name = rel['predicate']
                            predicate_normalized_name = predicate_name.lower()
                            
                            predicate_rels = relationship_adapter.filter(normalized_name=predicate_normalized_name)
                            if predicate_rels:
                                predicate = predicate_rels[0]
                                
                                # Update context if it's empty
                                if not predicate.get('context'):
                                    relationship_adapter.update(
                                        predicate.get('id'),
                                        context=explanation
                                    )
                            else:
                                predicate = relationship_adapter.create(
                                    name=predicate_name,
                                    normalized_name=predicate_normalized_name,
                                    context=explanation
                                )
                            
                            # Skip if a triple already exists between these entities with this predicate
                            existing_triples = triple_adapter.filter(
                                subject_id=subject_id,
                                predicate_id=predicate.get('id'),
                                object_id=object_id
                            )
                            
                            if existing_triples:
                                continue
                            
                            # Create a new triple
                            triple = triple_adapter.create(
                                subject_id=subject_id,
                                predicate_id=predicate.get('id'),
                                object_id=object_id,
                                confidence=rel.get('confidence', 0.6),
                                source_text=explanation
                            )
                            
                            new_triples.append(triple)
                            
                            # Also sync to Neo4j
                            try:
                                self.neo4j_client.sync_triple(triple)
                            except Exception as e:
                                logger.error(f"Error syncing triple to Neo4j: {str(e)}")
                    except json.JSONDecodeError:
                        logger.error(f"Error parsing JSON from LLM response: {json_str}")
        except Exception as e:
            logger.error(f"Error inferring relationships with LLM: {str(e)}")
        
        return new_triples
    
    def _suggest_entity_pairs_for_relationship(self, relationship: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Use LLM to suggest entity pairs that might be connected by a given relationship."""
        if not self.openai_client:
            return []
        
        new_triples = []
        
        # Get some existing entities to work with
        # Limit to a reasonable number to avoid overwhelming the LLM
        relationship_id = relationship.get('id')
        if not relationship_id:
            return []
        
        # Build query for existing entities
        query_params = {}
        
        # If relationship has an API key, only find entities with the same API key
        if relationship.get('api_key_id'):
            query_params['api_key_id'] = relationship.get('api_key_id')
        
        # Get a random sample of 20 entities
        existing_entities = entity_adapter.filter(**query_params)[:20]
        
        if len(existing_entities) < 2:
            return []  # Need at least 2 entities to form a relationship
        
        # Create a prompt to suggest entity pairs
        system_prompt = """
        You are a knowledge graph relationship suggestion system. Your task is to suggest entity pairs that might be connected by a given relationship.
        
        I will provide you with a relationship type and a list of entities in our knowledge graph. 
        Suggest pairs of entities that might be connected by this relationship.
        
        Return your response as a JSON list of suggested entity pairs in this format:
        [
            {
                "subject": "entity name",
                "object": "entity name",
                "confidence": 0.7,
                "explanation": "brief explanation of why this relationship exists"
            }
        ]
        
        If no pairs can be suggested, return an empty list: []
        
        Only include pairs that are reasonably likely to be connected by the given relationship. 
        Assign lower confidence scores (0.5-0.7) for pairs that are more speculative.
        """
        
        user_prompt = f"""
        Relationship: {relationship.get('name')}
        
        Entities:
        {', '.join([f"{e.get('name')} (Type: {e.get('entity_type') or 'Unknown'})" for e in existing_entities])}
        
        Suggest pairs of entities that might be connected by the relationship "{relationship.get('name')}".
        """
        
        # Send the request to the LLM
        try:
            response = self.openai_client.chat_completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                model="gpt-4",
                temperature=0.3,
                max_tokens=2000,
            )
            
            # Extract and parse the entity pairs from the response
            if response.get('status_code') == 200 and 'response' in response:
                content = response['response']['choices'][0]['message']['content']
                
                # Find JSON in the response
                json_match = re.search(r'\[\s*\{.*\}\s*\]', content, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                    try:
                        suggested_pairs = json.loads(json_str)
                        
                        # Process each suggested pair
                        for pair in suggested_pairs:
                            # Find the subject and object entities
                            subject_name = pair['subject']
                            object_name = pair['object']
                            
                            subject = next((e for e in existing_entities if e.get('name').lower() == subject_name.lower()), None)
                            object_entity = next((e for e in existing_entities if e.get('name').lower() == object_name.lower()), None)
                            
                            if not subject or not object_entity:
                                continue
                            
                            # Skip if a triple already exists between these entities with this relationship
                            existing_triples = triple_adapter.filter(
                                subject_id=subject.get('id'),
                                predicate_id=relationship_id,
                                object_id=object_entity.get('id')
                            )
                            
                            if existing_triples:
                                continue
                            
                            # Create a new triple
                            triple = triple_adapter.create(
                                subject_id=subject.get('id'),
                                predicate_id=relationship_id,
                                object_id=object_entity.get('id'),
                                confidence=pair.get('confidence', 0.6),
                                source_text=pair.get('explanation', f"Suggested relationship between {subject_name} and {object_name}")
                            )
                            
                            new_triples.append(triple)
                            
                            # Also sync to Neo4j
                            try:
                                self.neo4j_client.sync_triple(triple)
                            except Exception as e:
                                logger.error(f"Error syncing triple to Neo4j: {str(e)}")
                    except json.JSONDecodeError:
                        logger.error(f"Error parsing JSON from LLM response: {json_str}")
        except Exception as e:
            logger.error(f"Error suggesting entity pairs with LLM: {str(e)}")
        
        return new_triples
    
    def _infer_triples_with_llm(self, triple: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Use LLM to infer new triples based on an existing triple."""
        if not self.openai_client:
            return []
        
        new_triples = []
        
        # Get subject, predicate, and object
        subject_id = triple.get('subject_id')
        predicate_id = triple.get('predicate_id')
        object_id = triple.get('object_id')
        
        if not subject_id or not predicate_id or not object_id:
            return []
        
        try:
            subject = entity_adapter.get(id=subject_id)
            predicate = relationship_adapter.get(id=predicate_id)
            object_entity = entity_adapter.get(id=object_id)
        except Exception as e:
            logger.error(f"Error getting triple components: {str(e)}")
            return []
        
        # Create a prompt to infer new triples
        system_prompt = """
        You are a knowledge graph inference system. Your task is to infer new knowledge triples based on an existing triple.
        
        I will provide you with an existing triple (subject-predicate-object) from our knowledge graph. 
        Based on this triple, infer additional triples that are likely to be true.
        
        Return your response as a JSON list of inferred triples in this format:
        [
            {
                "subject": "entity name",
                "predicate": "relationship name",
                "object": "entity name",
                "confidence": 0.7,
                "explanation": "brief explanation of why this relationship exists"
            }
        ]
        
        If no additional triples can be inferred, return an empty list: []
        
        Only include triples that are reasonably likely to be true. Assign lower confidence scores (0.5-0.7) for triples that are more speculative.
        """
        
        user_prompt = f"""
        Existing triple:
        Subject: {subject.get('name')} (Type: {subject.get('entity_type') or 'Unknown'})
        Predicate: {predicate.get('name')}
        Object: {object_entity.get('name')} (Type: {object_entity.get('entity_type') or 'Unknown'})
        
        Infer additional triples based on this information.
        """
        
        # Send the request to the LLM
        try:
            response = self.openai_client.chat_completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                model="gpt-4",
                temperature=0.3,
                max_tokens=2000,
            )
            
            # Extract and parse the triples from the response
            if response.get('status_code') == 200 and 'response' in response:
                content = response['response']['choices'][0]['message']['content']
                
                # Find JSON in the response
                json_match = re.search(r'\[\s*\{.*\}\s*\]', content, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                    try:
                        inferred_triples = json.loads(json_str)
                        
                        # Process each inferred triple
                        for triple_dict in inferred_triples:
                            try:
                                # Get or create subject entity
                                subject_name = triple_dict['subject']
                                subject_normalized_name = subject_name.lower()
                                
                                subject_entities = entity_adapter.filter(normalized_name=subject_normalized_name)
                                if subject_entities:
                                    subject = subject_entities[0]
                                else:
                                    subject = entity_adapter.create(
                                        name=subject_name,
                                        normalized_name=subject_normalized_name
                                    )
                                
                                # Get or create object entity
                                object_name = triple_dict['object']
                                object_normalized_name = object_name.lower()
                                
                                object_entities = entity_adapter.filter(normalized_name=object_normalized_name)
                                if object_entities:
                                    object_entity = object_entities[0]
                                else:
                                    object_entity = entity_adapter.create(
                                        name=object_name,
                                        normalized_name=object_normalized_name
                                    )
                            except Exception as e:
                                logger.error(f"Error getting or creating entities: {str(e)}")
                                continue
                            
                            # Get or create predicate relationship
                            predicate_name = triple_dict['predicate']
                            predicate_normalized_name = predicate_name.lower()
                            
                            predicate_rels = relationship_adapter.filter(normalized_name=predicate_normalized_name)
                            if predicate_rels:
                                predicate = predicate_rels[0]
                            else:
                                predicate = relationship_adapter.create(
                                    name=predicate_name,
                                    normalized_name=predicate_normalized_name
                                )
                            
                            # Skip if a triple already exists between these entities with this predicate
                            existing_triples = triple_adapter.filter(
                                subject_id=subject.get('id'),
                                predicate_id=predicate.get('id'),
                                object_id=object_entity.get('id')
                            )
                            
                            if existing_triples:
                                continue
                            
                            # Create a new triple
                            explanation = triple_dict.get('explanation', f"Inferred from triple: {subject_name} {predicate_name} {object_name}")
                            new_triple = triple_adapter.create(
                                subject_id=subject.get('id'),
                                predicate_id=predicate.get('id'),
                                object_id=object_entity.get('id'),
                                confidence=triple_dict.get('confidence', 0.6),
                                source_text=explanation
                            )
                            
                            new_triples.append(new_triple)
                            
                            # Also sync to Neo4j
                            try:
                                self.neo4j_client.sync_triple(new_triple)
                            except Exception as e:
                                logger.error(f"Error syncing triple to Neo4j: {str(e)}")
                    except json.JSONDecodeError:
                        logger.error(f"Error parsing JSON from LLM response: {json_str}")
        except Exception as e:
            logger.error(f"Error inferring triples with LLM: {str(e)}")
        
        return new_triples
    
    def _find_connections_within_set(self, entity: Dict[str, Any], entity_ids: Set[str]) -> List[Dict[str, Any]]:
        """Find connections between an entity and a set of other entities."""
        new_triples = []
        
        # Skip self
        entity_id = entity.get('id')
        if not entity_id:
            return []
        
        # Remove self from entity set
        entity_ids_without_self = {eid for eid in entity_ids if eid != entity_id}
        
        if not entity_ids_without_self:
            return []
        
        # Use LLM to infer relationships between entities if available
        if self.openai_client:
            # Get entity objects
            entity_objects = []
            for eid in entity_ids_without_self:
                try:
                    e = entity_adapter.get(id=eid)
                    entity_objects.append(e)
                except Exception as e:
                    logger.error(f"Error getting entity {eid}: {str(e)}")
            
            # Get entity names
            entity_names = [e.get('name', '') for e in entity_objects]
            
            # Create a prompt to infer relationships
            system_prompt = """
            You are a knowledge graph relationship inference system. Your task is to infer potential relationships between entities.
            
            I will provide you with a main entity and a list of other entities. For each entity in the list, if you can infer a meaningful relationship with the main entity, provide it in the specified format.
            
            Return your response as a JSON list of inferred relationships in this format:
            [
                {
                    "subject": "main entity name",
                    "predicate": "relationship name",
                    "object": "other entity name",
                    "confidence": 0.8,
                    "explanation": "brief explanation of why this relationship exists"
                },
                {
                    "subject": "other entity name",
                    "predicate": "relationship name",
                    "object": "main entity name",
                    "confidence": 0.7,
                    "explanation": "brief explanation of why this relationship exists"
                }
            ]
            
            If no relationships can be inferred, return an empty list: []
            
            Only include relationships that are reasonably likely to be true. Assign lower confidence scores (0.5-0.7) for relationships that are more speculative.
            """
            
            user_prompt = f"""
            Main entity: {entity.get('name')} (Type: {entity.get('entity_type') or 'Unknown'})
            
            Other entities:
            {', '.join(entity_names)}
            
            Infer potential relationships between the main entity and the other entities.
            """
            
            # Send the request to the LLM
            try:
                response = self.openai_client.chat_completion(
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    model="gpt-4",
                    temperature=0.2,
                    max_tokens=2000,
                )
                
                # Extract and parse the relationships from the response
                if response.get('status_code') == 200 and 'response' in response:
                    content = response['response']['choices'][0]['message']['content']
                    
                    # Find JSON in the response
                    json_match = re.search(r'\[\s*\{.*\}\s*\]', content, re.DOTALL)
                    if json_match:
                        json_str = json_match.group(0)
                        try:
                            inferred_relationships = json.loads(json_str)
                            
                            # Create triples from inferred relationships
                            for rel in inferred_relationships:
                                # Find the subject and object entities
                                if rel['subject'] == entity.get('name'):
                                    subject_id = entity_id
                                    object_name = rel['object']
                                    object_entity = next((e for e in entity_objects if e.get('name') == object_name), None)
                                    if not object_entity:
                                        continue
                                    object_id = object_entity.get('id')
                                elif rel['object'] == entity.get('name'):
                                    subject_name = rel['subject']
                                    subject_entity = next((e for e in entity_objects if e.get('name') == subject_name), None)
                                    if not subject_entity:
                                        continue
                                    subject_id = subject_entity.get('id')
                                    object_id = entity_id
                                else:
                                    # Skip if neither subject nor object is the main entity
                                    continue
                                
                                # Get or create the predicate
                                predicate_name = rel['predicate']
                                predicate_normalized_name = predicate_name.lower()
                                
                                predicate_rels = relationship_adapter.filter(normalized_name=predicate_normalized_name)
                                if predicate_rels:
                                    predicate = predicate_rels[0]
                                else:
                                    predicate = relationship_adapter.create(
                                        name=predicate_name,
                                        normalized_name=predicate_normalized_name
                                    )
                                
                                # Skip if a triple already exists between these entities with this predicate
                                existing_triples = triple_adapter.filter(
                                    subject_id=subject_id,
                                    predicate_id=predicate.get('id'),
                                    object_id=object_id
                                )
                                
                                if existing_triples:
                                    continue
                                
                                # Create a new triple
                                explanation = rel.get('explanation', f"Inferred relationship between {rel['subject']} and {rel['object']}")
                                triple = triple_adapter.create(
                                    subject_id=subject_id,
                                    predicate_id=predicate.get('id'),
                                    object_id=object_id,
                                    confidence=rel.get('confidence', 0.6),
                                    source_text=explanation
                                )
                                
                                new_triples.append(triple)
                                
                                # Also sync to Neo4j
                                try:
                                    self.neo4j_client.sync_triple(triple)
                                except Exception as e:
                                    logger.error(f"Error syncing triple to Neo4j: {str(e)}")
                        except json.JSONDecodeError:
                            logger.error(f"Error parsing JSON from LLM response: {json_str}")
            except Exception as e:
                logger.error(f"Error inferring relationships with LLM: {str(e)}")
        
        return new_triples
