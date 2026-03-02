import logging
from requests.exceptions import HTTPError

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