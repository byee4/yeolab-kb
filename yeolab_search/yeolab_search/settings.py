"""
Django settings for the Yeo Lab Publications search app.

Supports both SQLite (local dev) and PostgreSQL (production).
Set DATABASE_URL env var to use PostgreSQL; omit for SQLite fallback.
"""
import os
from pathlib import Path

import dj_database_url
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env file from project root (two levels up from settings.py)
_dotenv_path = BASE_DIR.parent / '.env'
if _dotenv_path.is_file():
    load_dotenv(_dotenv_path)

# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------
SECRET_KEY = os.environ.get(
    'DJANGO_SECRET_KEY',
    'django-insecure-yeolab-dev-key-change-in-production',
)

DEBUG = os.environ.get('DJANGO_DEBUG', 'True').lower() in ('true', '1', 'yes')

ALLOWED_HOSTS = [
    h.strip()
    for h in os.environ.get('DJANGO_ALLOWED_HOSTS', '*').split(',')
    if h.strip()
]

# ---------------------------------------------------------------------------
# Application definition
# ---------------------------------------------------------------------------
INSTALLED_APPS = [
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.staticfiles',
    'social_django',
    'globus_portal_framework',
    'publications',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware',          # static files
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'social_django.middleware.SocialAuthExceptionMiddleware',
    'globus_portal_framework.middleware.ExpiredTokenMiddleware',
    'globus_portal_framework.middleware.GlobusAuthExceptionMiddleware',
]

AUTHENTICATION_BACKENDS = [
    'globus_portal_framework.auth.GlobusOpenIdConnect',
    'django.contrib.auth.backends.ModelBackend',
]

ROOT_URLCONF = 'yeolab_search.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'social_django.context_processors.backends',
                'social_django.context_processors.login_redirect',
            ],
        },
    },
]

WSGI_APPLICATION = 'yeolab_search.wsgi.application'

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
# If DATABASE_URL is set, use PostgreSQL (production).
# Otherwise, fall back to SQLite (local development).
DATABASE_URL = os.environ.get('DATABASE_URL')

if DATABASE_URL:
    DATABASES = {
        'default': dj_database_url.parse(DATABASE_URL, conn_max_age=600),
    }
    # Avoid serving requests with stale pooled connections after DB restarts.
    DATABASES['default']['CONN_HEALTH_CHECKS'] = True
else:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': os.environ.get(
                'YEOLAB_DB_PATH',
                str((BASE_DIR.parent / 'yeolab_publications.db').resolve()),
            ),
        }
    }

# ---------------------------------------------------------------------------
# Internationalization
# ---------------------------------------------------------------------------
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'America/Los_Angeles'
USE_TZ = True

# ---------------------------------------------------------------------------
# Static files
# ---------------------------------------------------------------------------
STATIC_URL = 'static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'
STORAGES = {
    'staticfiles': {
        'BACKEND': 'whitenoise.storage.CompressedManifestStaticFilesStorage',
    },
}

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# ---------------------------------------------------------------------------
# Globus Auth configuration
# ---------------------------------------------------------------------------
# Register your app at https://app.globus.org/settings/developers
# Select "Register a portal, science gateway, or other application"
# Set redirect URL to: http://localhost:8000/complete/globus/
# (or your production URL)

SOCIAL_AUTH_GLOBUS_KEY = os.environ.get('GLOBUS_CLIENT_ID', '')
SOCIAL_AUTH_GLOBUS_SECRET = os.environ.get('GLOBUS_CLIENT_SECRET', '')

SOCIAL_AUTH_GLOBUS_SCOPE = [
    'openid',
    'profile',
    'email',
]

# In reverse-proxy environments (e.g., App Platform), force HTTPS callback
# generation so OAuth redirect_uri exactly matches provider registration.
SOCIAL_AUTH_REDIRECT_IS_HTTPS = not DEBUG

# Optional explicit override for OAuth callback URL.
# Example: https://yeolab-kb-stbkj.ondigitalocean.app/complete/globus/
SOCIAL_AUTH_GLOBUS_REDIRECT_URI = os.environ.get(
    'SOCIAL_AUTH_GLOBUS_REDIRECT_URI',
    '',
).strip()

LOGIN_URL = '/login/globus/'
LOGIN_REDIRECT_URL = '/'
LOGOUT_REDIRECT_URL = '/'

# Optional: restrict to specific Globus identity providers
# SOCIAL_AUTH_GLOBUS_ALLOWED_DOMAINS = ['ucsd.edu']

SOCIAL_AUTH_PIPELINE = (
    'social_core.pipeline.social_auth.social_details',
    'social_core.pipeline.social_auth.social_uid',
    'social_core.pipeline.social_auth.auth_allowed',
    'social_core.pipeline.social_auth.social_user',
    'social_core.pipeline.user.get_username',
    'social_core.pipeline.user.create_user',
    'social_core.pipeline.social_auth.associate_user',
    'social_core.pipeline.social_auth.load_extra_data',
    'social_core.pipeline.user.user_details',
)

# ---------------------------------------------------------------------------
# Production security (only when DEBUG is False)
# ---------------------------------------------------------------------------
if not DEBUG:
    CSRF_TRUSTED_ORIGINS = [
        origin.strip()
        for origin in os.environ.get('CSRF_TRUSTED_ORIGINS', '').split(',')
        if origin.strip()
    ]
    SECURE_SSL_REDIRECT = os.environ.get('SECURE_SSL_REDIRECT', 'True').lower() in ('true', '1')
    SECURE_HSTS_SECONDS = 31536000
    SECURE_HSTS_INCLUDE_SUBDOMAINS = True
    SECURE_HSTS_PRELOAD = True
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
    SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')
