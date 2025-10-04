from django.db import models
import uuid


class Entity(models.Model):
    """Model for storing entities in the knowledge graph."""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    normalized_name = models.CharField(max_length=255, db_index=True)
    entity_type = models.CharField(max_length=100, blank=True, null=True)
    context = models.TextField(blank=True, null=True, help_text="The sentence or context from which this entity was extracted")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    properties = models.JSONField(default=dict, blank=True)
    api_key = models.ForeignKey(
        'api_proxy.APIKey',
        on_delete=models.CASCADE,
        related_name='entities',
        null=True,
        blank=True,
        help_text="The API key that was used to generate this entity"
    )
    
    class Meta:
        unique_together = ('normalized_name', 'entity_type', 'api_key')
    
    def __str__(self):
        return f"{self.name}" + (f" ({self.entity_type})" if self.entity_type else "")
    
    def save(self, *args, **kwargs):
        # Normalize the entity name for easier searching and matching
        if not self.normalized_name:
            self.normalized_name = self.name.lower().strip()
        super().save(*args, **kwargs)


class Relationship(models.Model):
    """Model for storing relationships between entities."""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    normalized_name = models.CharField(max_length=255, db_index=True)
    context = models.TextField(blank=True, null=True, help_text="The sentence or context from which this relationship was extracted")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    properties = models.JSONField(default=dict, blank=True)
    api_key = models.ForeignKey(
        'api_proxy.APIKey',
        on_delete=models.CASCADE,
        related_name='relationships',
        null=True,
        blank=True,
        help_text="The API key that was used to generate this relationship"
    )
    
    class Meta:
        unique_together = ('normalized_name', 'api_key')
    
    def __str__(self):
        return self.name
    
    def save(self, *args, **kwargs):
        # Normalize the relationship name for easier searching and matching
        if not self.normalized_name:
            self.normalized_name = self.name.lower().strip()
        super().save(*args, **kwargs)


class Triple(models.Model):
    """Model for storing subject-predicate-object triples in the knowledge graph."""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    subject = models.ForeignKey(
        Entity, 
        on_delete=models.CASCADE, 
        related_name='subject_triples'
    )
    predicate = models.ForeignKey(
        Relationship, 
        on_delete=models.CASCADE
    )
    object = models.ForeignKey(
        Entity, 
        on_delete=models.CASCADE, 
        related_name='object_triples'
    )
    confidence = models.FloatField(default=1.0)
    source_text = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    extracted_from = models.UUIDField(blank=True, null=True)  # Reference to APIRequest ID
    api_key = models.ForeignKey(
        'api_proxy.APIKey',
        on_delete=models.CASCADE,
        related_name='triples',
        null=True,
        blank=True,
        help_text="The API key that was used to generate this triple"
    )
    
    class Meta:
        unique_together = ('subject', 'predicate', 'object', 'api_key')
    
    def __str__(self):
        return f"{self.subject} - {self.predicate} - {self.object}"


class Query(models.Model):
    """Model for storing user queries for the knowledge graph."""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    query_text = models.TextField()
    structured_query = models.JSONField(blank=True, null=True)
    result = models.JSONField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    def __str__(self):
        return self.query_text[:50] + ("..." if len(self.query_text) > 50 else "")
