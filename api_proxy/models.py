from django.db import models
import uuid
import secrets


class APIKey(models.Model):
    """Model for storing API keys for users to access the system."""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    key = models.CharField(max_length=64, unique=True, editable=False)
    name = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    last_used = models.DateTimeField(null=True, blank=True)
    is_active = models.BooleanField(default=True)
    request_count = models.IntegerField(default=0)
    allowed_models = models.JSONField(default=list)
    
    @classmethod
    def generate_key(cls):
        """Generate a new API key."""
        return f"pk-{secrets.token_hex(32)}"
    
    def save(self, *args, **kwargs):
        """Override save to generate a key if one doesn't exist."""
        if not self.key:
            self.key = self.generate_key()
        super().save(*args, **kwargs)
    
    def __str__(self):
        return f"{self.name} ({self.key[:8]}...)"


class ExternalAPIConfig(models.Model):
    """Model for storing external LLM API configurations."""
    TYPE_CHOICES = [
        ('openai', 'OpenAI'),
        ('claude', 'Claude'),
        ('other', 'Other'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    api_type = models.CharField(max_length=50, choices=TYPE_CHOICES)
    api_key = models.CharField(max_length=255)
    api_base = models.URLField(blank=True, null=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    priority = models.IntegerField(default=100)
    config = models.JSONField(default=dict, blank=True)
    
    def __str__(self):
        return f"{self.name} ({self.api_type})"


class ModelMapping(models.Model):
    """Model for mapping local model names to provider model names."""
    local_name = models.CharField(max_length=255, unique=True)
    provider = models.ForeignKey(ExternalAPIConfig, on_delete=models.CASCADE)
    provider_model_name = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True)
    
    class Meta:
        unique_together = ('provider', 'provider_model_name')
    
    def __str__(self):
        return f"{self.local_name} -> {self.provider.name}:{self.provider_model_name}"


class ModelRouting(models.Model):
    """Model for routing rules that determine which provider to use based on conditions."""
    CONDITION_TYPE_CHOICES = [
        ('model', 'Requested Model'),
        ('prompt_length', 'Prompt Length'),
        ('api_key', 'API Key'),
        ('content_match', 'Content Match'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    condition_type = models.CharField(max_length=50, choices=CONDITION_TYPE_CHOICES)
    condition_value = models.JSONField()
    target_model = models.ForeignKey(ExternalAPIConfig, on_delete=models.CASCADE, related_name='routing_rules')
    priority = models.IntegerField(default=10)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['priority', 'created_at']
    
    def __str__(self):
        return f"{self.name} -> {self.target_model.name}"


class APIRequest(models.Model):
    """Model for logging API requests."""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    api_key = models.ForeignKey(APIKey, on_delete=models.SET_NULL, null=True, related_name='requests')
    endpoint = models.CharField(max_length=255)
    method = models.CharField(max_length=10)
    request_data = models.JSONField()
    response_data = models.JSONField(null=True, blank=True)
    status_code = models.IntegerField(null=True, blank=True)
    model_used = models.CharField(max_length=255, null=True, blank=True)
    provider_used = models.ForeignKey(ExternalAPIConfig, on_delete=models.SET_NULL, null=True, blank=True)
    tokens_used = models.IntegerField(default=0)
    duration_ms = models.IntegerField(null=True, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    error = models.TextField(null=True, blank=True)
    
    def __str__(self):
        return f"{self.endpoint} - {self.timestamp}"
