import logging
import json
import uuid
from typing import List, Dict, Any, Optional, Tuple, Set
import re

from django.http import Http404
from knowledge_graph.services.mongodb_adapter import entity_adapter, relationship_adapter, triple_adapter
from api_proxy.services.mongodb_adapter import external_api_config_adapter
from api_proxy.services.openai import OpenAIClient
from knowledge_graph.services.integrator import KnowledgeIntegrator

logger = logging.getLogger(__name__)

class TripleExtractor:
    """Service for extracting knowledge triples from text content."""
    
    def __init__(self, openai_client=None):
        """Initialize the extractor with an optional OpenAI client for extraction."""
        if openai_client:
            self.openai_client = openai_client
        else:
            # Try to create a client using admin API key if available
            try:
                # Use MongoDB adapter instead of Django ORM
                api_configs = external_api_config_adapter.filter(api_type='openai', is_active=True)
                if api_configs and len(api_configs) > 0:
                    api_config = api_configs[0]
                    self.openai_client = OpenAIClient(api_key=api_config.get('api_key'), api_base=api_config.get('api_base'))
                else:
                    self.openai_client = None
            except Exception as e:
                logger.error(f"Could not initialize OpenAI client: {str(e)}")
                self.openai_client = None
    
    def extract_from_conversation(self, messages: List[Dict[str, str]], 
                                api_request_id: Optional[str] = None,
                                api_key=None) -> List[Dict]:
        """Extract triples from a conversation between user and assistant."""
        combined_text = self._combine_messages(messages)
        return self.extract_from_text(combined_text, api_request_id, api_key)
    
    def _combine_messages(self, messages: List[Dict[str, str]]) -> str:
        """Combine message content from a conversation into a single text."""
        combined = []
        
        for msg in messages:
            role = msg.get('role', '')
            content = msg.get('content', '')
            
            if content:
                combined.append(f"{role.upper()}: {content}")
        
        return "\n\n".join(combined)
    
    def extract_from_text(self, text: str, api_request_id: Optional[str] = None, api_key=None) -> List[Dict]:
        """Extract knowledge triples from text content."""
        # First, try to extract using LLM if available
        if self.openai_client:
            return self._extract_using_llm(text, api_request_id, api_key)
        
        # Fallback to rule-based extraction
        return self._extract_using_rules(text, api_request_id, api_key)
    
    def _extract_using_llm(self, text: str, api_request_id: Optional[str] = None, api_key=None) -> List[Dict]:
        """Use an LLM to extract triples from text."""
        if not self.openai_client:
            logger.warning("No OpenAI client available, falling back to rule-based extraction")
            return self._extract_using_rules(text, api_request_id, api_key)
            
        try:
            # Create a system prompt instructing the model to extract triples
            system_prompt = """
            You are a knowledge triple extraction system. Your task is to extract factual knowledge triples (subject-predicate-object) from the given text.
            
            Guidelines:
            1. Focus on extracting factual statements only
            2. Subject and object should be specific entities, concepts, or things
            3. Predicate should describe the relationship between subject and object
            4. Assign entity types where possible (person, organization, location, concept, etc.)
            5. Assign a confidence score based on how explicitly stated the triple is (1.0 for directly stated, lower for inferred)
            6. Include the specific text where this knowledge was found
            
            Return your response as a JSON list of triples in this format:
            [
                {
                    "subject": "entity name",
                    "subject_type": "entity type",
                    "predicate": "relationship name",
                    "object": "entity name",
                    "object_type": "entity type",
                    "confidence": 0.95,
                    "source_text": "text snippet containing this knowledge"
                }
            ]
            
            If no triples can be extracted, return an empty list: []
            """
            
            # Create the prompt
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": text}
            ]
            
            # Send the request to the LLM
            response = self.openai_client.chat_completion(
                messages=messages,
                model="gpt-4",  # Use a model with good reasoning capabilities
                temperature=0.2,  # Low temperature for more deterministic results
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
                        extracted_triples = json.loads(json_str)
                        return self._save_triples(extracted_triples, api_request_id, api_key)
                    except json.JSONDecodeError:
                        logger.error(f"Error parsing JSON from LLM response: {json_str}")
                else:
                    # Try to parse the entire content as JSON directly
                    try:
                        extracted_triples = json.loads(content)
                        if isinstance(extracted_triples, list):
                            return self._save_triples(extracted_triples, api_request_id, api_key)
                    except json.JSONDecodeError:
                        logger.error("Could not find valid JSON in LLM response")
            
            logger.warning("Failed to extract triples using LLM, falling back to rule-based extraction")
            return self._extract_using_rules(text, api_request_id, api_key)
            
        except Exception as e:
            logger.error(f"Error extracting triples using LLM: {str(e)}")
            return self._extract_using_rules(text, api_request_id, api_key)
    
    def _extract_using_rules(self, text: str, api_request_id: Optional[str] = None, api_key=None) -> List[Dict]:
        """Use rule-based patterns to extract triples from text."""
        extracted_triples = []
        
        # Enhanced patterns for detecting subject-predicate-object triples
        
        # Pattern 1: Simple capitalized entity with verb and another capitalized entity
        pattern1 = r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+([a-z]+(?:\s+[a-z]+){0,2})\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)'
        
        # Pattern 2: Simple entity with "is a/an" relationship
        pattern2 = r'([A-Z][a-z]+(?:\s+[a-Z][a-z]+)*)\s+is\s+(?:a|an)\s+([a-z]+(?:\s+[a-z]+)*)'
        
        # Pattern 3: Entity with possessive relationship
        pattern3 = r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)(\'s|s\')\s+([a-z]+(?:\s+[a-z]+)*)\s+is\s+([A-Z][a-z]+(?:\s+[a-z]+)*)'
        
        # Process pattern 1
        matches = re.finditer(pattern1, text)
        for match in matches:
            subject, predicate, object_entity = match.groups()
            
            # Build a simple triple dictionary
            triple_dict = {
                "subject": subject.strip(),
                "subject_type": None,
                "predicate": predicate.strip(),
                "object": object_entity.strip(),
                "object_type": None,
                "confidence": 0.6,  # Lower confidence for rule-based extraction
                "source_text": match.group(0)
            }
            
            extracted_triples.append(triple_dict)
            
        # Process pattern 2 - "is a" relationship
        matches = re.finditer(pattern2, text)
        for match in matches:
            subject, entity_type = match.groups()
            
            triple_dict = {
                "subject": subject.strip(),
                "subject_type": entity_type.strip(),
                "predicate": "is a",
                "object": entity_type.strip(),
                "object_type": "type",
                "confidence": 0.7,
                "source_text": match.group(0)
            }
            
            extracted_triples.append(triple_dict)
            
        # Process pattern 3 - possessive relationship
        matches = re.finditer(pattern3, text)
        for match in matches:
            subject, possessive, relation, object_entity = match.groups()
            
            triple_dict = {
                "subject": subject.strip(),
                "subject_type": None,
                "predicate": f"has {relation.strip()}",
                "object": object_entity.strip(),
                "object_type": None,
                "confidence": 0.65,
                "source_text": match.group(0)
            }
            
            extracted_triples.append(triple_dict)
        
        return self._save_triples(extracted_triples, api_request_id, api_key)
    
    def _save_triples(self, triple_dicts: List[Dict[str, Any]], 
                    api_request_id: Optional[str] = None,
                    api_key=None) -> List[Dict]:
        """Save extracted triples to the database and integrate them with existing knowledge."""
        saved_triples = []
        new_entities = []
        new_relationships = []
        
        # No need for transaction.atomic() since we're using MongoDB
        for triple_dict in triple_dicts:
            try:
                # Get the source text for context
                source_text = triple_dict.get('source_text', '')
                
                # Get or create subject entity with API key
                subject_name = triple_dict['subject']
                subject_normalized_name = subject_name.lower()
                subject_type = triple_dict.get('subject_type')
                
                # Try to find existing entity
                subject_entities = entity_adapter.filter(
                    normalized_name=subject_normalized_name,
                    entity_type=subject_type
                )
                
                if subject_entities and len(subject_entities) > 0:
                    subject = subject_entities[0]
                    subject_created = False
                    
                    # Update context if it's empty and we have source text
                    if not subject.get('context') and source_text:
                        entity_adapter.update(
                            subject['id'],
                            context=source_text
                        )
                else:
                    # Create new entity
                    subject = entity_adapter.create(
                        name=subject_name,
                        normalized_name=subject_normalized_name,
                        entity_type=subject_type,
                        context=source_text,
                        api_key_id=api_key['id'] if isinstance(api_key, dict) and 'id' in api_key else api_key
                    )
                    subject_created = True
                    new_entities.append(subject)
                
                # Get or create object entity with API key
                object_name = triple_dict['object']
                object_normalized_name = object_name.lower()
                object_type = triple_dict.get('object_type')
                
                # Try to find existing entity
                object_entities = entity_adapter.filter(
                    normalized_name=object_normalized_name,
                    entity_type=object_type
                )
                
                if object_entities and len(object_entities) > 0:
                    object_entity = object_entities[0]
                    object_created = False
                    
                    # Update context if it's empty and we have source text
                    if not object_entity.get('context') and source_text:
                        entity_adapter.update(
                            object_entity['id'],
                            context=source_text
                        )
                else:
                    # Create new entity
                    object_entity = entity_adapter.create(
                        name=object_name,
                        normalized_name=object_normalized_name,
                        entity_type=object_type,
                        context=source_text,
                        api_key_id=api_key['id'] if isinstance(api_key, dict) and 'id' in api_key else api_key
                    )
                    object_created = True
                    new_entities.append(object_entity)
                
                # Get or create predicate relationship with API key
                predicate_name = triple_dict['predicate']
                predicate_normalized_name = predicate_name.lower()
                
                # Try to find existing relationship
                predicate_rels = relationship_adapter.filter(
                    normalized_name=predicate_normalized_name
                )
                
                if predicate_rels and len(predicate_rels) > 0:
                    predicate = predicate_rels[0]
                    predicate_created = False
                    
                    # Update context if it's empty and we have source text
                    if not predicate.get('context') and source_text:
                        relationship_adapter.update(
                            predicate['id'],
                            context=source_text
                        )
                else:
                    # Create new relationship
                    predicate = relationship_adapter.create(
                        name=predicate_name,
                        normalized_name=predicate_normalized_name,
                        context=source_text,
                        api_key_id=api_key['id'] if isinstance(api_key, dict) and 'id' in api_key else api_key
                    )
                    predicate_created = True
                    new_relationships.append(predicate)
                
                # Create the triple
                triple_data = {
                    'subject_id': subject['id'],
                    'predicate_id': predicate['id'],
                    'object_id': object_entity['id'],
                    'confidence': triple_dict.get('confidence', 1.0),
                    'source_text': triple_dict.get('source_text'),
                }
                
                # Only set extracted_from if it's a valid UUID
                if api_request_id:
                    try:
                        # Try to parse as UUID to validate
                        uuid.UUID(str(api_request_id))
                        triple_data['extracted_from'] = api_request_id
                    except (ValueError, TypeError, AttributeError):
                        # If not a valid UUID, generate a new UUID based on the API request ID
                        # This ensures we can still track the extraction source
                        new_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, str(api_request_id))
                        triple_data['extracted_from'] = str(new_uuid)
                        logger.info(f"Generated UUID {new_uuid} from non-UUID request ID: {api_request_id}")
                
                # Set the API key if provided
                if api_key:
                    triple_data['api_key_id'] = api_key['id'] if isinstance(api_key, dict) and 'id' in api_key else api_key
                
                # Try to find existing triple
                existing_triples = triple_adapter.filter(
                    subject_id=subject['id'],
                    predicate_id=predicate['id'],
                    object_id=object_entity['id']
                )
                
                if existing_triples and len(existing_triples) > 0:
                    triple = existing_triples[0]
                    
                    # If the triple already exists, update confidence if new confidence is higher
                    if triple.get('confidence', 0) < triple_dict.get('confidence', 1.0):
                        triple_adapter.update(
                            triple['id'],
                            confidence=triple_dict.get('confidence', 1.0)
                        )
                else:
                    # Create new triple
                    triple = triple_adapter.create(**triple_data)
                
                saved_triples.append(triple)
            
            except Exception as e:
                logger.error(f"Error saving triple {triple_dict}: {str(e)}")
        
        # After saving all triples, integrate new knowledge with existing knowledge
        if saved_triples:
            try:
                # Run knowledge integration in the background to avoid blocking
                # This will find connections between new entities/relationships and existing ones
                integrator = KnowledgeIntegrator()
                
                # Start with integrating new entities
                for entity in new_entities:
                    integrator.integrate_new_entity(entity)
                
                # Then integrate new relationships
                for relationship in new_relationships:
                    integrator.integrate_new_relationship(relationship)
                
                # Finally integrate new triples
                for triple in saved_triples:
                    integrator.integrate_new_triple(triple)
                
                logger.info(f"Integrated {len(saved_triples)} new triples into the knowledge graph")
            except Exception as e:
                logger.error(f"Error during knowledge integration: {str(e)}")
        
        return saved_triples
