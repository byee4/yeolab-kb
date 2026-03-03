from django.urls import path, include
from django.contrib.auth import logout as auth_logout
from django.shortcuts import redirect


def logout_view(request):
    """Simple logout that clears session and redirects home."""
    auth_logout(request)
    return redirect('/')


urlpatterns = [
    # Globus OAuth2 login/callback (social-auth-app-django)
    path('', include('social_django.urls', namespace='social')),

    # Logout
    path('logout/', logout_view, name='logout'),

    # Application
    path('', include('publications.urls')),
]

# Global error handlers
handler404 = "publications.views.custom_404"
