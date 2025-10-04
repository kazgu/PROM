from django.contrib import admin
from .models import Entity, Relationship, Triple, Query

@admin.register(Entity)
class EntityAdmin(admin.ModelAdmin):
    list_display = ('name', 'entity_type', 'normalized_name', 'created_at')
    list_filter = ('entity_type', 'created_at')
    search_fields = ('name', 'normalized_name', 'entity_type')
    readonly_fields = ('created_at', 'updated_at')

@admin.register(Relationship)
class RelationshipAdmin(admin.ModelAdmin):
    list_display = ('name', 'normalized_name', 'created_at')
    search_fields = ('name', 'normalized_name')
    readonly_fields = ('created_at', 'updated_at')

@admin.register(Triple)
class TripleAdmin(admin.ModelAdmin):
    list_display = ('subject', 'predicate', 'object', 'confidence', 'created_at', 'api_key')
    list_filter = ('predicate', 'confidence', 'created_at', 'api_key')
    search_fields = ('subject__name', 'predicate__name', 'object__name', 'source_text')
    readonly_fields = ('created_at', 'updated_at')

@admin.register(Query)
class QueryAdmin(admin.ModelAdmin):
    list_display = ('query_text', 'created_at')
    search_fields = ('query_text',)
    readonly_fields = ('created_at',)
