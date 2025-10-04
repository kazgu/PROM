"""
URL configuration for personal_kg project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from django.views.generic import TemplateView
from django.conf import settings
from django.conf.urls.static import static
from rest_framework_simplejwt.views import TokenRefreshView
from rest_framework.routers import DefaultRouter

from api_proxy.views import (
    chat_completions, completions, list_models,
    create_api_key, list_api_keys, get_api_key, revoke_api_key,
    list_external_api_configs, create_external_api_config,
    get_external_api_config, update_external_api_config, delete_external_api_config,
    list_model_routing_rules, create_model_routing_rule, get_model_routing_rule,
    update_model_routing_rule, delete_model_routing_rule, list_api_requests,
    get_api_request_details, usage_statistics
)
from api_proxy.dashboard_views import dashboard_stats, recent_activity, system_status
from users.views import (
    register_user, CustomTokenObtainPairView, ProfileView,
    UserViewSet, TeamViewSet, TeamMembershipViewSet, UserAPIKeyViewSet
)
from knowledge_graph.views import (
    EntityViewSet, RelationshipViewSet, TripleViewSet, QueryViewSet,
    extract_triples, entity_relationships, path_between_entities,
    execute_graph_query, sync_to_neo4j, search_entities, graph_statistics,
    most_connected_entities, similar_entities, entity_importance,
    find_communities, analyze_relationship, entity_summary, knowledge_gaps,
    entity_types, relationship_types, graph_data
)

# Create a router for ViewSets
router = DefaultRouter()
router.register(r'users', UserViewSet, basename='user')
router.register(r'teams', TeamViewSet, basename='team')
router.register(r'user-api-keys', UserAPIKeyViewSet, basename='user-api-key')
router.register(r'entities', EntityViewSet, basename='entity')
router.register(r'relationships', RelationshipViewSet, basename='relationship')
router.register(r'triples', TripleViewSet, basename='triple')
router.register(r'queries', QueryViewSet, basename='query')

urlpatterns = [
    # Admin
    path("admin/", admin.site.urls),
    
    # API routes
    path("api/", include(router.urls)),
    
    # Auth endpoints
    path("api/auth/token/", CustomTokenObtainPairView.as_view(), name="token_obtain_pair"),
    path("api/auth/token/refresh/", TokenRefreshView.as_view(), name="token_refresh"),
    path("api/auth/register/", register_user, name="register"),
    path("api/auth/profile/", ProfileView.as_view(), name="profile"),
    
    # Team memberships (nested under teams)
    path("api/teams/<uuid:team_id>/members/", TeamMembershipViewSet.as_view({'get': 'list', 'post': 'create'}), name="team-members-list"),
    path("api/teams/<uuid:team_id>/members/<uuid:pk>/", TeamMembershipViewSet.as_view({'get': 'retrieve', 'put': 'update', 'delete': 'destroy'}), name="team-members-detail"),
    
    # API Key management
    path("api/keys/create/", create_api_key, name="create_api_key"),
    path("api/keys/", list_api_keys, name="list_api_keys"),
    path("api/keys/<uuid:key_id>/", get_api_key, name="get_api_key"),
    path("api/keys/<uuid:key_id>/revoke/", revoke_api_key, name="revoke_api_key"),
    
    # API Model (External API Config) management
    path("api/models/", list_external_api_configs, name="list_models_configs"),
    path("api/models/create/", create_external_api_config, name="create_model_config"),
    path("api/models/<uuid:config_id>/", get_external_api_config, name="get_model_config"),
    path("api/models/<uuid:config_id>/update/", update_external_api_config, name="update_model_config"),
    path("api/models/<uuid:config_id>/delete/", delete_external_api_config, name="delete_model_config"),
    
    # Model Routing Rules management
    path("api/rules/", list_model_routing_rules, name="list_routing_rules"),
    path("api/rules/create/", create_model_routing_rule, name="create_routing_rule"),
    path("api/rules/<uuid:rule_id>/", get_model_routing_rule, name="get_routing_rule"),
    path("api/rules/<uuid:rule_id>/update/", update_model_routing_rule, name="update_routing_rule"),
    path("api/rules/<uuid:rule_id>/delete/", delete_model_routing_rule, name="delete_routing_rule"),
    
    # API Request Logs
    path("api/logs/", list_api_requests, name="list_api_requests"),
    path("api/logs/<uuid:request_id>/", get_api_request_details, name="get_request_details"),
    
    # Usage Statistics
    path("api/usage/", usage_statistics, name="usage_statistics"),
    
    # Dashboard endpoints
    path("api/dashboard/stats/", dashboard_stats, name="dashboard_stats"),
    path("api/dashboard/activity/", recent_activity, name="recent_activity"),
    path("api/system/status/", system_status, name="system_status"),
    
    # Knowledge Graph endpoints
    path("api/knowledge/extract-triples/", extract_triples, name="extract_triples"),
    path("api/knowledge/entity/<uuid:entity_id>/relationships/", entity_relationships, name="entity_relationships"),
    path("api/knowledge/path/<uuid:start_id>/<uuid:end_id>/", path_between_entities, name="path_between_entities"),
    path("api/knowledge/graph-query/", execute_graph_query, name="execute_graph_query"),
    path("api/knowledge/sync-to-neo4j/", sync_to_neo4j, name="sync_to_neo4j"),
    path("api/knowledge/search-entities/", search_entities, name="search_entities"),
    path("api/knowledge/entity-types/", entity_types, name="entity_types"),
    path("api/knowledge/relationship-types/", relationship_types, name="relationship_types"),
    path("api/knowledge/graph/", graph_data, name="graph_data"),
    
    # Knowledge Graph Analytics endpoints
    path("api/analytics/statistics/", graph_statistics, name="graph_statistics"),
    path("api/analytics/most-connected/", most_connected_entities, name="most_connected_entities"),
    path("api/analytics/similar-entities/<uuid:entity_id>/", similar_entities, name="similar_entities"),
    path("api/analytics/entity-importance/", entity_importance, name="entity_importance"),
    path("api/analytics/communities/", find_communities, name="find_communities"),
    path("api/analytics/relationship/<str:relationship_name>/", analyze_relationship, name="analyze_relationship"),
    path("api/analytics/entity-summary/<uuid:entity_id>/", entity_summary, name="entity_summary"),
    path("api/analytics/knowledge-gaps/", knowledge_gaps, name="knowledge_gaps"),
    
    # OpenAI-compatible API endpoints
    path("api/v1/chat/completions", chat_completions, name="chat_completions"),
    path("api/v1/completions", completions, name="completions"),
    path("api/v1/models", list_models, name="list_models"),
    # Also keep the original paths for compatibility
    path("v1/chat/completions", chat_completions, name="chat_completions_no_prefix"),
    path("v1/completions", completions, name="completions_no_prefix"),
    path("v1/models", list_models, name="list_models_no_prefix"),
    
    # Frontend views (will be added as we create templates)
    path("", TemplateView.as_view(template_name="dashboard.html"), name="dashboard"),
    path("dashboard/", TemplateView.as_view(template_name="dashboard.html"), name="dashboard"),
    path("graph/", TemplateView.as_view(template_name="graph.html"), name="graph"),
    path("api-console/", TemplateView.as_view(template_name="api_console.html"), name="api_console"),
    path("chat/", TemplateView.as_view(template_name="chat.html"), name="chat"),
]

# Add static files URLs in development
if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
