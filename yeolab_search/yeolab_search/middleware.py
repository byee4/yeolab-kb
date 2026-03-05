import logging
from requests.exceptions import HTTPError
from django.contrib.auth import logout
from django.shortcuts import redirect

logger = logging.getLogger(__name__)

class GlobusDebugMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        return self.get_response(request)

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
                try:
                    logout(request)
                    if hasattr(request, "session"):
                        request.session.flush()
                except Exception:
                    pass
                return redirect("/login/globus/?oauth_retry=1")
