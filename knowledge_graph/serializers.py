from rest_framework import serializers
from .models import Entity, Relationship, Triple, Query


class EntitySerializer(serializers.ModelSerializer):
    """Serializer for Entity model."""
    
    class Meta:
        model = Entity
        fields = '__all__'
        read_only_fields = ['id', 'created_at', 'updated_at', 'normalized_name']


class RelationshipSerializer(serializers.ModelSerializer):
    """Serializer for Relationship model."""
    
    class Meta:
        model = Relationship
        fields = '__all__'
        read_only_fields = ['id', 'created_at', 'updated_at', 'normalized_name']


class TripleSerializer(serializers.ModelSerializer):
    """Basic serializer for Triple model."""
    subject_name = serializers.CharField(source='subject.name', read_only=True)
    predicate_name = serializers.CharField(source='predicate.name', read_only=True)
    object_name = serializers.CharField(source='object.name', read_only=True)
    
    class Meta:
        model = Triple
        fields = [
            'id', 'subject', 'subject_name', 'predicate', 'predicate_name',
            'object', 'object_name', 'confidence', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']


class TripleDetailSerializer(serializers.ModelSerializer):
    """Detailed serializer for Triple model with full entity and relationship details."""
    subject = EntitySerializer(read_only=True)
    predicate = RelationshipSerializer(read_only=True)
    object = EntitySerializer(read_only=True)
    
    class Meta:
        model = Triple
        fields = '__all__'
        read_only_fields = ['id', 'created_at', 'updated_at']


class TripleCreateSerializer(serializers.ModelSerializer):
    """Serializer for creating Triple instances."""
    subject_name = serializers.CharField(required=False, write_only=True)
    subject_type = serializers.CharField(required=False, write_only=True)
    predicate_name = serializers.CharField(required=False, write_only=True)
    object_name = serializers.CharField(required=False, write_only=True)
    object_type = serializers.CharField(required=False, write_only=True)
    
    class Meta:
        model = Triple
        fields = [
            'id', 'subject', 'subject_name', 'subject_type', 'predicate', 'predicate_name',
            'object', 'object_name', 'object_type', 'confidence', 'source_text', 'extracted_from'
        ]
        read_only_fields = ['id']
    
    def validate(self, attrs):
        """Validate that either IDs or names are provided for subject, predicate, and object."""
        # Check subject
        if 'subject' not in attrs and 'subject_name' not in attrs:
            raise serializers.ValidationError("Either subject ID or subject_name must be provided")
        
        # Check predicate
        if 'predicate' not in attrs and 'predicate_name' not in attrs:
            raise serializers.ValidationError("Either predicate ID or predicate_name must be provided")
        
        # Check object
        if 'object' not in attrs and 'object_name' not in attrs:
            raise serializers.ValidationError("Either object ID or object_name must be provided")
        
        return attrs
    
    def create(self, validated_data):
        """Create a Triple instance, creating or retrieving related entities if needed."""
        from .models import Entity, Relationship
        
        # Handle subject
        subject = validated_data.pop('subject', None)
        subject_name = validated_data.pop('subject_name', None)
        subject_type = validated_data.pop('subject_type', None)
        
        if not subject and subject_name:
            # Get or create subject entity
            subject, _ = Entity.objects.get_or_create(
                normalized_name=subject_name.lower(),
                entity_type=subject_type,
                defaults={'name': subject_name}
            )
        
        # Handle predicate
        predicate = validated_data.pop('predicate', None)
        predicate_name = validated_data.pop('predicate_name', None)
        
        if not predicate and predicate_name:
            # Get or create predicate relationship
            predicate, _ = Relationship.objects.get_or_create(
                normalized_name=predicate_name.lower(),
                defaults={'name': predicate_name}
            )
        
        # Handle object
        object_entity = validated_data.pop('object', None)
        object_name = validated_data.pop('object_name', None)
        object_type = validated_data.pop('object_type', None)
        
        if not object_entity and object_name:
            # Get or create object entity
            object_entity, _ = Entity.objects.get_or_create(
                normalized_name=object_name.lower(),
                entity_type=object_type,
                defaults={'name': object_name}
            )
        
        # Create the triple
        triple = Triple.objects.create(
            subject=subject,
            predicate=predicate,
            object=object_entity,
            **validated_data
        )
        
        return triple


class QuerySerializer(serializers.ModelSerializer):
    """Serializer for Query model."""
    
    class Meta:
        model = Query
        fields = '__all__'
        read_only_fields = ['id', 'created_at']
