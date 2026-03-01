from django import template
from urllib.parse import urlencode

register = template.Library()


@register.simple_tag(takes_context=True)
def query_string(context, **kwargs):
    """Build a query string preserving existing GET params, overriding with kwargs."""
    request = context["request"]
    params = request.GET.copy()
    for k, v in kwargs.items():
        if v is not None and v != "":
            params[k] = v
        elif k in params:
            del params[k]
    return f"?{params.urlencode()}" if params else ""


@register.filter
def get_item(dictionary, key):
    """Template filter to access dict by key."""
    if isinstance(dictionary, dict):
        return dictionary.get(key, [])
    return []
