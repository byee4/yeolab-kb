from django.urls import path
from . import views

app_name = "publications"

urlpatterns = [
    # Web UI
    path("", views.home, name="home"),
    path("healthz/", views.healthz, name="healthz"),
    path("search/", views.search, name="search"),
    path("pub/<str:pmid>/", views.publication_detail, name="publication_detail"),
    path("author/<int:author_id>/", views.author_detail, name="author_detail"),
    path("authors/", views.author_list, name="author_list"),
    path("datasets/", views.dataset_list, name="dataset_list"),
    path("dataset/<int:accession_id>/", views.dataset_detail, name="dataset_detail"),
    path("dataset/<int:accession_id>/download.sh", views.dataset_download_script, name="dataset_download_script"),
    path("methods/", views.method_list, name="method_list"),
    path("method/<int:method_id>/", views.method_detail, name="method_detail"),
    path("analysis/", views.analysis_list, name="analysis_list"),
    path("analysis/<int:pipeline_id>/", views.analysis_detail, name="analysis_detail"),
    path("analysis/dataset/<str:accession>/", views.analysis_detail_by_accession, name="analysis_detail_by_accession"),

    # Admin / Update
    path("admin/", views.admin_panel, name="admin_panel"),
    path("admin/start-update/", views.admin_start_update, name="admin_start_update"),
    path("admin/update-status/", views.admin_update_status, name="admin_update_status"),
    path("admin/upload-encode-json/", views.admin_upload_encode_json, name="admin_upload_encode_json"),
    path("admin/preview-add/", views.admin_preview_add, name="admin_preview_add"),
    path("admin/confirm-add/", views.admin_confirm_add, name="admin_confirm_add"),
    path("admin/preview-remove/", views.admin_preview_remove, name="admin_preview_remove"),
    path("admin/confirm-remove/", views.admin_confirm_remove, name="admin_confirm_remove"),
    path("admin/sync-code-examples/", views.admin_sync_code_examples, name="admin_sync_code_examples"),
    path("admin/code-editor/", views.admin_code_editor, name="admin_code_editor"),
    path("admin/code-editor/datasets/", views.admin_code_editor_datasets, name="admin_code_editor_datasets"),
    path("admin/code-editor/dataset/<str:accession>/", views.admin_code_editor_dataset_content, name="admin_code_editor_dataset_content"),
    path("admin/code-editor/fetch/", views.admin_code_editor_fetch, name="admin_code_editor_fetch"),
    path("admin/code-editor/save/", views.admin_code_editor_save, name="admin_code_editor_save"),
    path("admin/code-editor/push/", views.admin_code_editor_push, name="admin_code_editor_push"),
    path("admin/code-editor/delete/", views.admin_code_editor_delete, name="admin_code_editor_delete"),
    path("admin/code-editor/lookup-date/<str:accession>/", views.admin_code_editor_lookup_date, name="admin_code_editor_lookup_date"),

    # Chat
    path("chat/", views.chat_page, name="chat"),
    path("api/chat/", views.chat_message, name="api_chat"),

    # REST API
    path("api/stats/", views.api_stats, name="api_stats"),
    path("api/publications/", views.api_publications, name="api_publications"),
    path("api/publications/<str:pmid>/", views.api_publication_detail, name="api_publication_detail"),
    path("api/datasets/", views.api_datasets, name="api_datasets"),
    path("api/datasets/<int:accession_id>/", views.api_dataset_detail, name="api_dataset_detail"),
    path("api/authors/", views.api_authors, name="api_authors"),
    path("api/submit/", views.api_submit_pmid, name="api_submit_pmid"),
    path("api/remove/", views.api_remove_pmid, name="api_remove_pmid"),
    path("api/update/start/", views.admin_start_update, name="api_start_update"),
    path("api/update/status/", views.admin_update_status, name="api_update_status"),
]
