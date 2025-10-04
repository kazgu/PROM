import logging
import json
from django.shortcuts import get_object_or_404
from django.http import JsonResponse, Http404
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from rest_framework import viewsets, status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.pagination import PageNumberPagination

from .models import Entity, Relationship, Triple, Query
from .serializers import (
    EntitySerializer, RelationshipSerializer, TripleSerializer, 
    TripleCreateSerializer, TripleDetailSerializer, QuerySerializer
)
from .services.extractor import TripleExtractor
from .services.graph_db import Neo4jGraphDB
from .services.analytics import GraphAnalytics
from .services.mongodb_adapter import entity_adapter, relationship_adapter, triple_adapter, query_adapter

logger = logging.getLogger(__name__)


class StandardResultsSetPagination(PageNumberPagination):
    """Pagination class for list endpoints."""
    page_size = 20
    page_size_query_param = 'page_size'
    max_page_size = 100


class EntityViewSet(viewsets.ViewSet):
    """ViewSet for Entity model."""
    serializer_class = EntitySerializer
    permission_classes = [IsAuthenticated]
    pagination_class = StandardResultsSetPagination
    
    def get_serializer(self, *args, **kwargs):
        """Return the serializer instance."""
        serializer_class = self.get_serializer_class()
        return serializer_class(*args, **kwargs)
    
    def get_serializer_class(self):
        """Return the serializer class."""
        return self.serializer_class
    
    def paginate_queryset(self, queryset):
        """Return a paginated queryset."""
        if self.paginator is None:
            return None
        return self.paginator.paginate_queryset(queryset, self.request, view=self)
    
    def get_paginated_response(self, data):
        """Return a paginated response."""
        assert self.paginator is not None
        return self.paginator.get_paginated_response(data)
    
    @property
    def paginator(self):
        """Return the paginator instance."""
        if not hasattr(self, '_paginator'):
            if self.pagination_class is None:
                self._paginator = None
            else:
                self._paginator = self.pagination_class()
        return self._paginator
    
    def list(self, request):
        """List entities."""
        # Get query parameters
        name = request.query_params.get('name', None)
        entity_type = request.query_params.get('type', None)
        
        # Build filters
        filters = {}
        if name:
            filters['name__icontains'] = name
        if entity_type:
            filters['entity_type'] = entity_type
        
        # Get entities from MongoDB
        entities = entity_adapter.filter(**filters)
        
        # Apply pagination
        page = self.paginate_queryset(entities)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = self.get_serializer(entities, many=True)
        return Response(serializer.data)
    
    def retrieve(self, request, pk=None):
        """Retrieve an entity."""
        try:
            entity = entity_adapter.get(id=pk)
            serializer = self.get_serializer(entity)
            return Response(serializer.data)
        except Http404:
            return Response(
                {"error": f"Entity with ID {pk} not found"},
                status=status.HTTP_404_NOT_FOUND
            )
    
    def create(self, request):
        """Create an entity."""
        serializer = self.get_serializer(data=request.data)
        if serializer.is_valid():
            entity = entity_adapter.create(**serializer.validated_data)
            return Response(
                self.get_serializer(entity).data,
                status=status.HTTP_201_CREATED
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
    def update(self, request, pk=None):
        """Update an entity."""
        try:
            entity = entity_adapter.get(id=pk)
            serializer = self.get_serializer(data=request.data)
            if serializer.is_valid():
                updated_entity = entity_adapter.update(pk, **serializer.validated_data)
                return Response(self.get_serializer(updated_entity).data)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Http404:
            return Response(
                {"error": f"Entity with ID {pk} not found"},
                status=status.HTTP_404_NOT_FOUND
            )
    
    def partial_update(self, request, pk=None):
        """Partially update an entity."""
        try:
            entity = entity_adapter.get(id=pk)
            serializer = self.get_serializer(data=request.data, partial=True)
            if serializer.is_valid():
                updated_entity = entity_adapter.update(pk, **serializer.validated_data)
                return Response(self.get_serializer(updated_entity).data)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Http404:
            return Response(
                {"error": f"Entity with ID {pk} not found"},
                status=status.HTTP_404_NOT_FOUND
            )
    
    def destroy(self, request, pk=None):
        """Delete an entity."""
        try:
            entity = entity_adapter.get(id=pk)
            entity_adapter.delete(pk)
            return Response(status=status.HTTP_204_NO_CONTENT)
        except Http404:
            return Response(
                {"error": f"Entity with ID {pk} not found"},
                status=status.HTTP_404_NOT_FOUND
            )


class RelationshipViewSet(viewsets.ViewSet):
    """ViewSet for Relationship model."""
    serializer_class = RelationshipSerializer
    permission_classes = [IsAuthenticated]
    pagination_class = StandardResultsSetPagination
    
    def get_serializer(self, *args, **kwargs):
        """Return the serializer instance."""
        serializer_class = self.get_serializer_class()
        return serializer_class(*args, **kwargs)
    
    def get_serializer_class(self):
        """Return the serializer class."""
        return self.serializer_class
    
    def paginate_queryset(self, queryset):
        """Return a paginated queryset."""
        if self.paginator is None:
            return None
        return self.paginator.paginate_queryset(queryset, self.request, view=self)
    
    def get_paginated_response(self, data):
        """Return a paginated response."""
        assert self.paginator is not None
        return self.paginator.get_paginated_response(data)
    
    @property
    def paginator(self):
        """Return the paginator instance."""
        if not hasattr(self, '_paginator'):
            if self.pagination_class is None:
                self._paginator = None
            else:
                self._paginator = self.pagination_class()
        return self._paginator
    
    def list(self, request):
        """List relationships."""
        # Get query parameters
        name = request.query_params.get('name', None)
        
        # Build filters
        filters = {}
        if name:
            filters['name__icontains'] = name
        
        # Get relationships from MongoDB
        relationships = relationship_adapter.filter(**filters)
        
        # Apply pagination
        page = self.paginate_queryset(relationships)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = self.get_serializer(relationships, many=True)
        return Response(serializer.data)
    
    def retrieve(self, request, pk=None):
        """Retrieve a relationship."""
        try:
            relationship = relationship_adapter.get(id=pk)
            serializer = self.get_serializer(relationship)
            return Response(serializer.data)
        except Http404:
            return Response(
                {"error": f"Relationship with ID {pk} not found"},
                status=status.HTTP_404_NOT_FOUND
            )
    
    def create(self, request):
        """Create a relationship."""
        serializer = self.get_serializer(data=request.data)
        if serializer.is_valid():
            relationship = relationship_adapter.create(**serializer.validated_data)
            return Response(
                self.get_serializer(relationship).data,
                status=status.HTTP_201_CREATED
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
    def update(self, request, pk=None):
        """Update a relationship."""
        try:
            relationship = relationship_adapter.get(id=pk)
            serializer = self.get_serializer(data=request.data)
            if serializer.is_valid():
                updated_relationship = relationship_adapter.update(pk, **serializer.validated_data)
                return Response(self.get_serializer(updated_relationship).data)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Http404:
            return Response(
                {"error": f"Relationship with ID {pk} not found"},
                status=status.HTTP_404_NOT_FOUND
            )
    
    def partial_update(self, request, pk=None):
        """Partially update a relationship."""
        try:
            relationship = relationship_adapter.get(id=pk)
            serializer = self.get_serializer(data=request.data, partial=True)
            if serializer.is_valid():
                updated_relationship = relationship_adapter.update(pk, **serializer.validated_data)
                return Response(self.get_serializer(updated_relationship).data)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Http404:
            return Response(
                {"error": f"Relationship with ID {pk} not found"},
                status=status.HTTP_404_NOT_FOUND
            )
    
    def destroy(self, request, pk=None):
        """Delete a relationship."""
        try:
            relationship = relationship_adapter.get(id=pk)
            relationship_adapter.delete(pk)
            return Response(status=status.HTTP_204_NO_CONTENT)
        except Http404:
            return Response(
                {"error": f"Relationship with ID {pk} not found"},
                status=status.HTTP_404_NOT_FOUND
            )


class TripleViewSet(viewsets.ViewSet):
    """ViewSet for Triple model."""
    permission_classes = [IsAuthenticated]
    pagination_class = StandardResultsSetPagination
    
    def get_serializer(self, *args, **kwargs):
        """Return the serializer instance."""
        serializer_class = self.get_serializer_class()
        return serializer_class(*args, **kwargs)
    
    def get_serializer_class(self):
        """Return appropriate serializer class based on action."""
        if self.action == 'create':
            return TripleCreateSerializer
        elif self.action in ['retrieve', 'update', 'partial_update']:
            return TripleDetailSerializer
        return TripleSerializer
    
    def paginate_queryset(self, queryset):
        """Return a paginated queryset."""
        if self.paginator is None:
            return None
        return self.paginator.paginate_queryset(queryset, self.request, view=self)
    
    def get_paginated_response(self, data):
        """Return a paginated response."""
        assert self.paginator is not None
        return self.paginator.get_paginated_response(data)
    
    @property
    def paginator(self):
        """Return the paginator instance."""
        if not hasattr(self, '_paginator'):
            if self.pagination_class is None:
                self._paginator = None
            else:
                self._paginator = self.pagination_class()
        return self._paginator
    
    def list(self, request):
        """List triples."""
        # Get query parameters
        api_key_id = request.query_params.get('api_key_id', None)
        subject = request.query_params.get('subject', None)
        predicate = request.query_params.get('predicate', None)
        object_name = request.query_params.get('object', None)
        min_confidence = request.query_params.get('min_confidence', None)
        
        # Build filters
        filters = {}
        if api_key_id:
            filters['api_key_id'] = api_key_id
        
        # These filters would need to be handled differently in MongoDB
        # since we're storing IDs rather than objects
        if subject:
            # Get entities with matching name
            subject_entities = entity_adapter.filter(name__icontains=subject)
            subject_ids = [entity['id'] for entity in subject_entities]
            if subject_ids:
                filters['subject_id__in'] = subject_ids
            else:
                # No matching subjects, return empty result
                return Response([])
        
        if predicate:
            # Get relationships with matching name
            predicate_rels = relationship_adapter.filter(name__icontains=predicate)
            predicate_ids = [rel['id'] for rel in predicate_rels]
            if predicate_ids:
                filters['predicate_id__in'] = predicate_ids
            else:
                # No matching predicates, return empty result
                return Response([])
        
        if object_name:
            # Get entities with matching name
            object_entities = entity_adapter.filter(name__icontains=object_name)
            object_ids = [entity['id'] for entity in object_entities]
            if object_ids:
                filters['object_id__in'] = object_ids
            else:
                # No matching objects, return empty result
                return Response([])
        
        if min_confidence:
            filters['confidence__gte'] = float(min_confidence)
        
        # Get triples from MongoDB
        triples = triple_adapter.filter(**filters)
        
        # Apply pagination
        page = self.paginate_queryset(triples)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = self.get_serializer(triples, many=True)
        return Response(serializer.data)
    
    def retrieve(self, request, pk=None):
        """Retrieve a triple."""
        try:
            triple = triple_adapter.get(id=pk)
            serializer = self.get_serializer(triple)
            return Response(serializer.data)
        except Http404:
            return Response(
                {"error": f"Triple with ID {pk} not found"},
                status=status.HTTP_404_NOT_FOUND
            )
    
    def create(self, request):
        """Create a triple."""
        serializer = self.get_serializer(data=request.data)
        if serializer.is_valid():
            triple = triple_adapter.create(**serializer.validated_data)
            return Response(
                self.get_serializer(triple).data,
                status=status.HTTP_201_CREATED
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
    def update(self, request, pk=None):
        """Update a triple."""
        try:
            triple = triple_adapter.get(id=pk)
            serializer = self.get_serializer(data=request.data)
            if serializer.is_valid():
                updated_triple = triple_adapter.update(pk, **serializer.validated_data)
                return Response(self.get_serializer(updated_triple).data)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Http404:
            return Response(
                {"error": f"Triple with ID {pk} not found"},
                status=status.HTTP_404_NOT_FOUND
            )
    
    def partial_update(self, request, pk=None):
        """Partially update a triple."""
        try:
            triple = triple_adapter.get(id=pk)
            serializer = self.get_serializer(data=request.data, partial=True)
            if serializer.is_valid():
                updated_triple = triple_adapter.update(pk, **serializer.validated_data)
                return Response(self.get_serializer(updated_triple).data)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Http404:
            return Response(
                {"error": f"Triple with ID {pk} not found"},
                status=status.HTTP_404_NOT_FOUND
            )
    
    def destroy(self, request, pk=None):
        """Delete a triple."""
        try:
            triple = triple_adapter.get(id=pk)
            triple_adapter.delete(pk)
            return Response(status=status.HTTP_204_NO_CONTENT)
        except Http404:
            return Response(
                {"error": f"Triple with ID {pk} not found"},
                status=status.HTTP_404_NOT_FOUND
            )


class QueryViewSet(viewsets.ViewSet):
    """ViewSet for Query model (read-only)."""
    serializer_class = QuerySerializer
    permission_classes = [IsAuthenticated]
    pagination_class = StandardResultsSetPagination
    
    def get_serializer(self, *args, **kwargs):
        """Return the serializer instance."""
        serializer_class = self.get_serializer_class()
        return serializer_class(*args, **kwargs)
    
    def get_serializer_class(self):
        """Return the serializer class."""
        return self.serializer_class
    
    def paginate_queryset(self, queryset):
        """Return a paginated queryset."""
        if self.paginator is None:
            return None
        return self.paginator.paginate_queryset(queryset, self.request, view=self)
    
    def get_paginated_response(self, data):
        """Return a paginated response."""
        assert self.paginator is not None
        return self.paginator.get_paginated_response(data)
    
    @property
    def paginator(self):
        """Return the paginator instance."""
        if not hasattr(self, '_paginator'):
            if self.pagination_class is None:
                self._paginator = None
            else:
                self._paginator = self.pagination_class()
        return self._paginator
    
    def list(self, request):
        """List queries."""
        # Get queries from MongoDB
        queries = query_adapter.filter()
        
        # Apply pagination
        page = self.paginate_queryset(queries)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = self.get_serializer(queries, many=True)
        return Response(serializer.data)
    
    def retrieve(self, request, pk=None):
        """Retrieve a query."""
        try:
            query = query_adapter.get(id=pk)
            serializer = self.get_serializer(query)
            return Response(serializer.data)
        except Http404:
            return Response(
                {"error": f"Query with ID {pk} not found"},
                status=status.HTTP_404_NOT_FOUND
            )


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def extract_triples(request):
    """Extract triples from text or conversation."""
    data = request.data
    text = data.get('text')
    messages = data.get('messages')
    api_key_id = data.get('api_key_id')  # Get API key ID from request
    
    if not text and not messages:
        return Response(
            {"error": "Either 'text' or 'messages' parameter is required"},
            status=status.HTTP_400_BAD_REQUEST
        )
    
    try:
        # Get the API key object if provided
        api_key = None
        if api_key_id:
            # We'll need to modify the TripleExtractor to work with API key ID instead of object
            # For now, we'll just pass the ID
            api_key = api_key_id
        
        # Initialize the triple extractor
        extractor = TripleExtractor()
        
        if text:
            # Extract from plain text
            triples = extractor.extract_from_text(text, api_key=api_key)
        else:
            # Extract from conversation messages
            triples = extractor.extract_from_conversation(messages, api_key=api_key)
        
        # Return the extracted triples
        # Assuming the extractor now returns MongoDB-compatible objects
        return Response({
            "success": True,
            "count": len(triples),
            "triples": triples
        })
        
    except Exception as e:
        logger.error(f"Error extracting triples: {str(e)}")
        return Response(
            {"error": f"Failed to extract triples: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def entity_relationships(request, entity_id):
    """Get relationships for a specific entity."""
    direction = request.query_params.get('direction', 'both')
    limit = int(request.query_params.get('limit', 50))
    
    if direction not in ['both', 'incoming', 'outgoing']:
        return Response(
            {"error": "Direction must be one of: both, incoming, outgoing"},
            status=status.HTTP_400_BAD_REQUEST
        )
    
    try:
        # Check if entity exists
        try:
            entity = entity_adapter.get(id=entity_id)
        except Http404:
            return Response(
                {"error": f"Entity with ID {entity_id} not found"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        # Get relationships from Neo4j
        graph_db = Neo4jGraphDB()
        relationships = graph_db.get_entity_relationships(
            str(entity_id), 
            direction=direction,
            limit=limit
        )
        
        return Response({
            "entity": entity,
            "relationships": relationships
        })
        
    except Exception as e:
        logger.error(f"Error getting entity relationships: {str(e)}")
        return Response(
            {"error": f"Failed to retrieve relationships: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def path_between_entities(request, start_id, end_id):
    """Find paths between two entities."""
    max_depth = int(request.query_params.get('max_depth', 4))
    
    try:
        # Check if entities exist
        try:
            entity_adapter.get(id=start_id)
        except Http404:
            return Response(
                {"error": f"Entity with ID {start_id} not found"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        try:
            entity_adapter.get(id=end_id)
        except Http404:
            return Response(
                {"error": f"Entity with ID {end_id} not found"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        # Get path from Neo4j
        graph_db = Neo4jGraphDB()
        paths = graph_db.path_between(
            str(start_id),
            str(end_id),
            max_depth=max_depth
        )
        
        return Response({
            "paths": paths
        })
        
    except Exception as e:
        logger.error(f"Error finding path between entities: {str(e)}")
        return Response(
            {"error": f"Failed to find path: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def execute_graph_query(request):
    """Execute a custom Cypher query on the graph database."""
    data = request.data
    query_text = data.get('query')
    params = data.get('params', {})
    
    if not query_text:
        return Response(
            {"error": "Query parameter is required"},
            status=status.HTTP_400_BAD_REQUEST
        )
    
    try:
        # Execute query in Neo4j
        graph_db = Neo4jGraphDB()
        result = graph_db.execute_query(query_text, params)
        
        return Response(result)
        
    except Exception as e:
        logger.error(f"Error executing graph query: {str(e)}")
        return Response(
            {"error": f"Failed to execute query: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def sync_to_neo4j(request):
    """Sync all triples to Neo4j."""
    try:
        graph_db = Neo4jGraphDB()
        count = graph_db.sync_all_triples()
        
        return Response({
            "success": True,
            "synced_count": count
        })
        
    except Exception as e:
        logger.error(f"Error syncing to Neo4j: {str(e)}")
        return Response(
            {"error": f"Failed to sync to Neo4j: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def search_entities(request):
    """Search for entities by name."""
    query = request.query_params.get('q', '')
    limit = int(request.query_params.get('limit', 10))
    
    if not query:
        return Response(
            {"error": "Search query parameter 'q' is required"},
            status=status.HTTP_400_BAD_REQUEST
        )
    
    try:
        # Search in Neo4j
        graph_db = Neo4jGraphDB()
        results = graph_db.search_entity(query, limit=limit)
        
        return Response({
            "query": query,
            "results": results
        })
        
    except Exception as e:
        logger.error(f"Error searching entities: {str(e)}")
        return Response(
            {"error": f"Failed to search entities: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


# Graph Analytics Endpoints

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def graph_statistics(request):
    """Get statistics about the knowledge graph."""
    try:
        analytics = GraphAnalytics()
        stats = analytics.get_graph_statistics()
        
        return Response(stats)
        
    except Exception as e:
        logger.error(f"Error getting graph statistics: {str(e)}")
        return Response(
            {"error": f"Failed to get graph statistics: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def most_connected_entities(request):
    """Get the most connected entities in the graph."""
    limit = int(request.query_params.get('limit', 10))
    
    try:
        analytics = GraphAnalytics()
        entities = analytics.get_most_connected_entities(limit=limit)
        
        return Response({
            "entities": entities
        })
        
    except Exception as e:
        logger.error(f"Error getting most connected entities: {str(e)}")
        return Response(
            {"error": f"Failed to get most connected entities: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def similar_entities(request, entity_id):
    """Find entities similar to the given entity."""
    limit = int(request.query_params.get('limit', 10))
    
    try:
        # Check if entity exists
        try:
            entity_adapter.get(id=entity_id)
        except Http404:
            return Response(
                {"error": f"Entity with ID {entity_id} not found"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        analytics = GraphAnalytics()
        entities = analytics.find_similar_entities(str(entity_id), limit=limit)
        
        return Response({
            "entity_id": entity_id,
            "similar_entities": entities
        })
        
    except Exception as e:
        logger.error(f"Error finding similar entities: {str(e)}")
        return Response(
            {"error": f"Failed to find similar entities: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def entity_importance(request):
    """Get entity importance based on PageRank."""
    limit = int(request.query_params.get('limit', 20))
    
    try:
        analytics = GraphAnalytics()
        entities = analytics.get_entity_importance(limit=limit)
        
        return Response({
            "entities": entities
        })
        
    except Exception as e:
        logger.error(f"Error calculating entity importance: {str(e)}")
        return Response(
            {"error": f"Failed to calculate entity importance: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def find_communities(request):
    """Find communities in the knowledge graph."""
    min_size = int(request.query_params.get('min_size', 3))
    
    try:
        analytics = GraphAnalytics()
        communities = analytics.find_communities(min_community_size=min_size)
        
        return Response({
            "communities": communities,
            "count": len(communities)
        })
        
    except Exception as e:
        logger.error(f"Error finding communities: {str(e)}")
        return Response(
            {"error": f"Failed to find communities: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def analyze_relationship(request, relationship_name):
    """Analyze a specific relationship type."""
    try:
        analytics = GraphAnalytics()
        analysis = analytics.relationship_analysis(relationship_name)
        
        if "error" in analysis:
            return Response(
                {"error": analysis["error"]},
                status=status.HTTP_404_NOT_FOUND
            )
        
        return Response(analysis)
        
    except Exception as e:
        logger.error(f"Error analyzing relationship: {str(e)}")
        return Response(
            {"error": f"Failed to analyze relationship: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def entity_summary(request, entity_id):
    """Get a summary of an entity's relationships."""
    try:
        # Check if entity exists
        try:
            entity_adapter.get(id=entity_id)
        except Http404:
            return Response(
                {"error": f"Entity with ID {entity_id} not found"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        analytics = GraphAnalytics()
        summary = analytics.summarize_entity(str(entity_id))
        
        if "error" in summary:
            return Response(
                {"error": summary["error"]},
                status=status.HTTP_404_NOT_FOUND
            )
        
        return Response(summary)
        
    except Exception as e:
        logger.error(f"Error summarizing entity: {str(e)}")
        return Response(
            {"error": f"Failed to summarize entity: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def knowledge_gaps(request):
    """Identify potential knowledge gaps in the graph."""
    try:
        analytics = GraphAnalytics()
        gaps = analytics.knowledge_gaps()
        
        return Response({
            "gaps": gaps,
            "count": len(gaps)
        })
        
    except Exception as e:
        logger.error(f"Error identifying knowledge gaps: {str(e)}")
        return Response(
            {"error": f"Failed to identify knowledge gaps: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def entity_types(request):
    """Get all entity types with counts."""
    try:
        # Get all entities from MongoDB
        entities = entity_adapter.all()
        
        # Extract unique entity types and count occurrences
        entity_type_counts = {}
        for entity in entities:
            entity_type = entity.get('entity_type')
            if entity_type and entity_type.strip():
                if entity_type not in entity_type_counts:
                    entity_type_counts[entity_type] = 0
                entity_type_counts[entity_type] += 1
        
        # Format the result
        result = []
        for entity_type, count in entity_type_counts.items():
            result.append({
                'name': entity_type,
                'normalized_name': entity_type.lower(),
                'count': count
            })
        
        # Sort by count (descending)
        result.sort(key=lambda x: x['count'], reverse=True)
        
        return Response(result)
        
    except Exception as e:
        logger.error(f"Error getting entity types: {str(e)}")
        return Response(
            {"error": f"Failed to get entity types: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def relationship_types(request):
    """Get all relationship types with counts."""
    try:
        # Get all relationships from MongoDB
        relationships = relationship_adapter.all()
        
        # Get all triples from MongoDB
        triples = triple_adapter.all()
        
        # Create a dictionary to store relationship counts
        relationship_counts = {}
        
        # Count occurrences of each relationship in triples
        for triple in triples:
            predicate_id = triple.get('predicate_id')
            if predicate_id:
                if predicate_id not in relationship_counts:
                    relationship_counts[predicate_id] = 0
                relationship_counts[predicate_id] += 1
        
        # Format the result
        result = []
        for relationship in relationships:
            relationship_id = relationship.get('id')
            relationship_name = relationship.get('name')
            normalized_name = relationship.get('normalized_name')
            
            if relationship_id in relationship_counts:
                result.append({
                    'name': relationship_name,
                    'normalized_name': normalized_name,
                    'count': relationship_counts[relationship_id]
                })
            else:
                # Relationship exists but has no triples
                result.append({
                    'name': relationship_name,
                    'normalized_name': normalized_name,
                    'count': 0
                })
        
        # Sort by count (descending)
        result.sort(key=lambda x: x['count'], reverse=True)
        
        return Response(result)
        
    except Exception as e:
        logger.error(f"Error getting relationship types: {str(e)}")
        return Response(
            {"error": f"Failed to get relationship types: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def graph_data(request):
    """Get graph data for visualization."""
    try:
        # Get query parameters
        search = request.query_params.get('search', '')
        entity_types = request.query_params.getlist('entity_types', [])
        relation_types = request.query_params.getlist('relation_types', [])
        start_date = request.query_params.get('start_date', None)
        end_date = request.query_params.get('end_date', None)
        limit = int(request.query_params.get('limit', 1000))
        api_key_id = request.query_params.get('api_key_id', None)

        # Log query parameters for debugging
        logger.info(f"Graph data request - Query params: {dict(request.query_params)}")
        logger.info(f"API Key ID: {api_key_id}")
        
        # Build entity filters
        entity_filters = {}
        if api_key_id:
            logger.info(f"Filtering entities by API key ID: {api_key_id}")
            entity_filters['api_key_id'] = api_key_id
        
        if search:
            entity_filters['name__icontains'] = search
        
        if entity_types:
            entity_filters['entity_type__in'] = entity_types
        
        if start_date:
            entity_filters['created_at__gte'] = start_date
        
        if end_date:
            entity_filters['created_at__lte'] = end_date
        
        # Get entities from MongoDB
        entities = entity_adapter.filter(**entity_filters)
        
        # Limit the number of entities
        if len(entities) > limit:
            entities = entities[:limit]
        
        # Get entity IDs for filtering triples
        entity_ids = [entity['id'] for entity in entities]
        
        # Build triple filters
        triple_filters = {
            'subject_id__in': entity_ids,
            'object_id__in': entity_ids
        }
        
        if relation_types:
            # Get relationship IDs for the given normalized names
            rel_filters = {'normalized_name__in': relation_types}
            relationships = relationship_adapter.filter(**rel_filters)
            rel_ids = [rel['id'] for rel in relationships]
            
            if rel_ids:
                triple_filters['predicate_id__in'] = rel_ids
            else:
                # No matching relationships, return empty result for triples
                return Response({
                    'entities': entities,
                    'triples': []
                })
        
        if start_date:
            triple_filters['created_at__gte'] = start_date
        
        if end_date:
            triple_filters['created_at__lte'] = end_date
        
        # Get triples from MongoDB
        triples = triple_adapter.filter(**triple_filters)
        
        # Limit the number of triples
        if len(triples) > limit:
            triples = triples[:limit]
        
        # Enrich triples with entity and relationship names
        # Create lookup dictionaries for entities and relationships
        entity_lookup = {entity['id']: entity for entity in entities}
        
        # Get all relationship IDs from triples
        rel_ids = list(set(triple['predicate_id'] for triple in triples))
        relationships = relationship_adapter.filter(id__in=rel_ids)
        rel_lookup = {rel['id']: rel for rel in relationships}
        
        # Get any missing entities that might be in triples but not in our entity list
        missing_entity_ids = []
        for triple in triples:
            if triple['subject_id'] not in entity_lookup:
                missing_entity_ids.append(triple['subject_id'])
            if triple['object_id'] not in entity_lookup:
                missing_entity_ids.append(triple['object_id'])
        
        if missing_entity_ids:
            missing_entities = entity_adapter.filter(id__in=missing_entity_ids)
            for entity in missing_entities:
                entity_lookup[entity['id']] = entity
        
        # Prepare response data
        triples_data = []
        for triple in triples:
            subject_id = triple['subject_id']
            predicate_id = triple['predicate_id']
            object_id = triple['object_id']
            
            # Skip if we don't have the entity or relationship info
            if (subject_id not in entity_lookup or 
                predicate_id not in rel_lookup or 
                object_id not in entity_lookup):
                continue
            
            triples_data.append({
                'id': triple['id'],
                'subject_id': subject_id,
                'subject_name': entity_lookup[subject_id]['name'],
                'predicate_id': predicate_id,
                'predicate_name': rel_lookup[predicate_id]['name'],
                'object_id': object_id,
                'object_name': entity_lookup[object_id]['name'],
                'confidence': triple.get('confidence', 1.0)
            })
        
        # Format entities for response
        entities_data = []
        for entity in entities:
            entities_data.append({
                'id': entity['id'],
                'name': entity['name'],
                'entity_type': entity.get('entity_type', ''),
                'properties': entity.get('properties', {})
            })
        
        return Response({
            'entities': entities_data,
            'triples': triples_data
        })
        
    except Exception as e:
        logger.error(f"Error getting graph data: {str(e)}")
        return Response(
            {"error": f"Failed to get graph data: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
