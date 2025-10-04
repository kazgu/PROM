from django.contrib import admin
from .models import APIKey, ExternalAPIConfig, ModelMapping, ModelRouting, APIRequest

@admin.register(APIKey)
class APIKeyAdmin(admin.ModelAdmin):
    list_display = ('name', 'key', 'created_at', 'last_used', 'is_active', 'request_count')
    list_filter = ('is_active', 'created_at')
    search_fields = ('name', 'key')
    readonly_fields = ('key', 'created_at', 'last_used', 'request_count')

@admin.register(ExternalAPIConfig)
class ExternalAPIConfigAdmin(admin.ModelAdmin):
    list_display = ('name', 'api_type', 'is_active', 'priority', 'created_at')
    list_filter = ('api_type', 'is_active')
    search_fields = ('name',)
    readonly_fields = ('created_at', 'updated_at')

@admin.register(ModelMapping)
class ModelMappingAdmin(admin.ModelAdmin):
    list_display = ('local_name', 'provider', 'provider_model_name', 'is_active')
    list_filter = ('is_active', 'provider')
    search_fields = ('local_name', 'provider_model_name')

@admin.register(ModelRouting)
class ModelRoutingAdmin(admin.ModelAdmin):
    list_display = ('name', 'condition_type', 'target_model', 'priority', 'is_active')
    list_filter = ('condition_type', 'is_active')
    search_fields = ('name',)
    readonly_fields = ('created_at', 'updated_at')

@admin.register(APIRequest)
class APIRequestAdmin(admin.ModelAdmin):
    list_display = ('endpoint', 'method', 'api_key', 'model_used', 'status_code', 'tokens_used', 'timestamp')
    list_filter = ('endpoint', 'method', 'status_code', 'model_used')
    search_fields = ('endpoint', 'api_key__name', 'model_used')
    readonly_fields = ('timestamp', 'duration_ms', 'tokens_used', 'request_data', 'response_data')
