# © agrprojects

import re

from aiohttp import web
from .stream_routes import routes


_PROTECTED_MEDIA_PREFIXES = (
    "/prepare/",
    "/api/generate/",
    "/api/download/",
    "/watch/",
    "/thumb/",
)
_DIRECT_MEDIA_PATH = re.compile(r"^/(?:[A-Za-z0-9_-]{6})?\d+(?:/|$)")


@web.middleware
async def require_media_access_code(request: web.Request, handler):
    """Reject protected page/API requests before route code can run.

    The individual handlers also validate the code against Supabase. This
    middleware is intentionally limited to the protected route families so
    public health, favicon, robots, and admin routes remain available while a
    stale or accidentally changed handler cannot render a media page without
    a code.
    """
    is_protected_route = request.path.startswith(_PROTECTED_MEDIA_PREFIXES)
    is_direct_media_path = bool(_DIRECT_MEDIA_PATH.match(request.path))
    if is_protected_route or is_direct_media_path:
        access_code = request.rel_url.query.get("access_code", "").strip()
        if not access_code:
            raise web.HTTPForbidden(
                text="This link requires an access_code.",
                content_type="text/plain",
            )
    return await handler(request)


async def web_server():
    web_app = web.Application(
        client_max_size=30000000,
        middlewares=[require_media_access_code],
    )
    web_app.add_routes(routes)
    return web_app
