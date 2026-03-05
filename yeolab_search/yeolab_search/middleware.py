import logging
from requests.exceptions import HTTPError
from django.contrib.auth import logout
from django.shortcuts import redirect
from django.http import HttpResponse

logger = logging.getLogger(__name__)

class GlobusDebugMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        try:
            if (
                request.COOKIES.get("globus_oauth_retry") == "1"
                and getattr(getattr(request, "user", None), "is_authenticated", False)
            ):
                response.delete_cookie("globus_oauth_retry")
        except Exception:
            pass
        return response

    def process_exception(self, request, exception):
        # Catch the requests.exceptions.HTTPError thrown by social-auth
        if isinstance(exception, HTTPError) and exception.response is not None:
            logger.error("=== Globus OAuth Token Exchange Error ===")
            logger.error(f"Status Code: {exception.response.status_code}")
            logger.error(f"Request URL: {exception.response.request.url}")
            logger.error(f"Request Body: {exception.response.request.body}")
            logger.error(f"Response Body: {exception.response.text}")
            logger.error("=========================================")

            # Recovery path: if Globus token exchange failed with 5xx on callback,
            # clear auth/session state and send user through a clean login flow.
            if (
                request.path.startswith("/complete/globus/")
                and exception.response.status_code >= 500
            ):
                already_retried = request.COOKIES.get("globus_oauth_retry") == "1"
                if already_retried:
                    response = HttpResponse(
                        "Globus authentication is temporarily unavailable. "
                        "Please wait a moment and try logging in again.",
                        status=502,
                        content_type="text/plain",
                    )
                    response.delete_cookie("globus_oauth_retry")
                    response.delete_cookie("sessionid")
                    response.delete_cookie("csrftoken")
                    return response
                try:
                    logout(request)
                except Exception:
                    pass
                try:
                    if hasattr(request, "session"):
                        request.session.flush()
                except Exception:
                    pass
                response = redirect("/login/globus/?oauth_retry=1&fresh=1")
                response.delete_cookie("sessionid")
                response.delete_cookie("csrftoken")
                response.set_cookie(
                    "globus_oauth_retry",
                    "1",
                    max_age=180,
                    httponly=True,
                    secure=request.is_secure(),
                    samesite="Lax",
                )
                return response
