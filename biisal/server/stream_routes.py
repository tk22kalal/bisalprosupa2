import re
import time
import math
import json
import logging
import secrets
import mimetypes
import asyncio
from datetime import datetime, timezone
import aiohttp as aiohttp_client
from aiohttp import web
from aiohttp.http_exceptions import BadStatusLine
from pyrogram.errors import FloodWait
from pyrogram.enums import ParseMode
from urllib.parse import quote_plus, urlencode
from biisal.bot import multi_clients, work_loads, StreamBot
from biisal.server.exceptions import FIleNotFound, InvalidHash
from biisal import StartTime, __version__
from ..utils.time_format import get_readable_time
from ..utils.custom_dl import ByteStreamer
from biisal.utils.render_template import render_page
from biisal.utils.database import Database
from biisal.utils.file_properties import get_name, get_hash, get_media_from_message
from biisal.utils.human_readable import humanbytes
from biisal.vars import Var
from biisal.utils.supabase_quota import supabase_quota

stream_log = logging.getLogger("stream.routes")

routes = web.RouteTableDef()

db = Database(Var.DATABASE_URL, Var.name)


async def render_prepare_page(temp_data, access_code):
    try:
        with open("biisal-file-stream-pro/biisal/template/prepare.html") as f:
            template_content = f.read()
    except FileNotFoundError:
        try:
            with open("biisal/template/prepare.html") as f:
                template_content = f.read()
        except FileNotFoundError:
            return "<html><body><h1>Error: prepare.html template not found</h1></body></html>"

    file_size = humanbytes(temp_data.get('file_size', 0))
    file_name = temp_data.get('file_name', 'Unknown File')
    caption = temp_data.get('caption', file_name)
    mime_type = temp_data.get('mime_type', 'application/octet-stream')
    tag = mime_type.split("/")[0].strip() if mime_type else 'file'

    template_content = template_content.replace("{{file_name}}", file_name)
    template_content = template_content.replace("{{caption}}", caption)
    template_content = template_content.replace("{{file_size}}", file_size)
    template_content = template_content.replace("{{token}}", temp_data['token'])
    template_content = template_content.replace("{{tag}}", tag)
    template_content = template_content.replace(
        "{{access_code_json}}",
        json.dumps(access_code),
    )

    return template_content


@routes.get("/favicon.ico")
async def favicon_handler(_):
    return web.Response(status=204)


@routes.get("/robots.txt")
async def robots_handler(_):
    try:
        with open("robots.txt", "r") as f:
            content = f.read()
        return web.Response(text=content, content_type="text/plain")
    except FileNotFoundError:
        return web.Response(
            text="User-agent: *\nAllow: /\n",
            content_type="text/plain"
        )


@routes.get("/", allow_head=True)
async def root_route_handler(_):
    telegram_bot = "Not connected"
    if hasattr(StreamBot, 'username') and StreamBot.username:
        telegram_bot = "@" + StreamBot.username
    return web.json_response(
        {
            "server_status": "running",
            "uptime": get_readable_time(int(time.time() - StartTime)),
            "telegram_bot": telegram_bot,
            "connected_bots": len(multi_clients),
            "loads": dict(
                ("bot" + str(c + 1), l)
                for c, (_, l) in enumerate(
                    sorted(work_loads.items(), key=lambda x: x[1], reverse=True)
                )
            ),
            "version": __version__,
        }
    )


@routes.get(r"/prepare/{token}", allow_head=True)
async def prepare_stream_handler(request: web.Request):
    try:
        token = request.match_info["token"]
        access_code = request.rel_url.query.get("access_code", "").strip()
        _, access_error = await supabase_quota.validate_access_code(access_code)
        if access_error:
            return web.Response(
                text=access_error["message"],
                status=access_error["status"],
                content_type="text/plain",
            )

        serve_domain = Var.SERVE_DOMAIN if Var.SERVE_DOMAIN in ('web', 'webx') else None
        temp_data = await db.get_temp_file(token, serve_domain=serve_domain)
        if not temp_data:
            return web.Response(text="Link expired or not found", status=404)
        return web.Response(
            text=await render_prepare_page(temp_data, access_code),
            content_type='text/html',
        )
    except web.HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error in prepare_stream_handler: {e}")
        return web.Response(text="Error loading page", status=500)


@routes.get(r"/api/generate/{token}")
async def generate_stream_handler(request: web.Request):
    try:
        token = request.match_info["token"]
        player = request.rel_url.query.get("player", "plyr")
        access_code = request.rel_url.query.get("access_code", "").strip()
        _, access_error = await supabase_quota.validate_access_code(access_code)
        if access_error:
            return web.json_response(
                {"success": False, "error": access_error["message"]},
                status=access_error["status"],
                content_type="application/json",
            )

        serve_domain = Var.SERVE_DOMAIN if Var.SERVE_DOMAIN in ('web', 'webx') else None
        temp_data = await db.get_temp_file(token, serve_domain=serve_domain)
        if not temp_data:
            return web.json_response(
                {"success": False, "error": "Link expired or not found"},
                status=404,
                content_type='application/json'
            )

        client = StreamBot
        original_msg = await client.get_messages(temp_data['from_chat_id'], temp_data['message_id'])
        if not original_msg:
            return web.json_response(
                {"success": False, "error": "Original message not found"},
                status=404,
                content_type='application/json'
            )

        max_retries = 3
        log_msg = None
        for attempt in range(max_retries):
            try:
                log_msg = await original_msg.copy(
                    chat_id=Var.BIN_CHANNEL,
                    caption=temp_data['caption'][:1024],
                    parse_mode=ParseMode.HTML
                )
                break
            except FloodWait as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(e.value)
                else:
                    return web.json_response(
                        {"success": False, "error": "Server is busy. Please try again in a few seconds."},
                        status=429,
                        content_type='application/json'
                    )
            except Exception as copy_error:
                logging.error(f"Error copying message (attempt {attempt + 1}): {copy_error}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                else:
                    return web.json_response(
                        {"success": False, "error": "Failed to process file. Please try again."},
                        status=500,
                        content_type='application/json'
                    )

        if not log_msg:
            return web.json_response(
                {"success": False, "error": "Failed to process file after retries"},
                status=500,
                content_type='application/json'
            )

        file_name = get_name(log_msg) or temp_data['file_name'] or "file"
        if isinstance(file_name, bytes):
            file_name = file_name.decode('utf-8', errors='ignore')
        file_name = re.sub(r"[\r\n\t\x00-\x1f\x7f]", "", str(file_name)).strip() or "file"
        file_hash = get_hash(log_msg)

        request_host = request.host
        forwarded_proto = request.headers.get('X-Forwarded-Proto', '').lower()
        if forwarded_proto in ('https', 'http'):
            scheme = forwarded_proto
        elif Var.HAS_SSL:
            scheme = 'https'
        else:
            scheme = request.scheme if request.scheme else 'http'
        base_url = f"{scheme}://{request_host}/"

        stream_query = urlencode({
            "hash": file_hash,
            "player": player,
            "access_code": access_code,
        })
        stream_link = (
            f"{base_url}watch/{log_msg.id}/{quote_plus(file_name)}"
            f"?{stream_query}"
        )

        response_data = {
            "success": True,
            "stream_url": stream_link,
            "file_name": file_name
        }
        if temp_data.get('thumbnail_url'):
            response_data['thumbnail_url'] = temp_data['thumbnail_url']

        return web.json_response(response_data, content_type='application/json')

    except Exception as e:
        logging.error(f"Error in generate_stream_handler: {e}", exc_info=True)
        return web.json_response(
            {"success": False, "error": "Server error. Please try again later."},
            status=500,
            content_type='application/json'
        )


@routes.get(r"/api/download/{token}")
async def generate_download_handler(request: web.Request):
    try:
        if not supabase_quota.downloads_enabled:
            return web.json_response(
                {
                    "success": False,
                    "error": (
                        "Direct downloads are temporarily disabled. "
                        "Please use streaming."
                    ),
                },
                status=403,
                content_type="application/json",
            )

        token = request.match_info["token"]
        access_code = request.rel_url.query.get("access_code", "").strip()
        _, access_error = await supabase_quota.validate_access_code(access_code)
        if access_error:
            return web.json_response(
                {"success": False, "error": access_error["message"]},
                status=access_error["status"],
                content_type="application/json",
            )

        serve_domain = Var.SERVE_DOMAIN if Var.SERVE_DOMAIN in ('web', 'webx') else None
        temp_data = await db.get_temp_file(token, serve_domain=serve_domain)
        if not temp_data:
            return web.json_response(
                {"success": False, "error": "Link expired or not found"},
                status=404,
                content_type='application/json'
            )

        client = StreamBot
        original_msg = await client.get_messages(temp_data['from_chat_id'], temp_data['message_id'])
        if not original_msg:
            return web.json_response(
                {"success": False, "error": "Original message not found"},
                status=404,
                content_type='application/json'
            )

        max_retries = 3
        log_msg = None
        for attempt in range(max_retries):
            try:
                log_msg = await original_msg.copy(
                    chat_id=Var.BIN_CHANNEL,
                    caption=temp_data['caption'][:1024],
                    parse_mode=ParseMode.HTML
                )
                break
            except FloodWait as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(e.value)
                else:
                    return web.json_response(
                        {"success": False, "error": "Server is busy. Please try again in a few seconds."},
                        status=429,
                        content_type='application/json'
                    )
            except Exception as copy_error:
                logging.error(f"Error copying message (attempt {attempt + 1}): {copy_error}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                else:
                    return web.json_response(
                        {"success": False, "error": "Failed to process file. Please try again."},
                        status=500,
                        content_type='application/json'
                    )

        if not log_msg:
            return web.json_response(
                {"success": False, "error": "Failed to process file after retries"},
                status=500,
                content_type='application/json'
            )

        file_name = get_name(log_msg) or temp_data['file_name'] or "file"
        if isinstance(file_name, bytes):
            file_name = file_name.decode('utf-8', errors='ignore')
        file_name = re.sub(r"[\r\n\t\x00-\x1f\x7f]", "", str(file_name)).strip() or "file"
        file_hash = get_hash(log_msg)

        request_host = request.host
        forwarded_proto = request.headers.get('X-Forwarded-Proto', '').lower()
        if forwarded_proto in ('https', 'http'):
            scheme = forwarded_proto
        elif Var.HAS_SSL:
            scheme = 'https'
        else:
            scheme = request.scheme if request.scheme else 'http'
        base_url = f"{scheme}://{request_host}/"

        download_query = urlencode({
            "hash": file_hash,
            "download": "1",
            "access_code": access_code,
        })
        download_link = (
            f"{base_url}{log_msg.id}/{quote_plus(file_name)}"
            f"?{download_query}"
        )

        response_data = {
            "success": True,
            "download_url": download_link,
            "file_name": file_name
        }
        if temp_data.get('thumbnail_url'):
            response_data['thumbnail_url'] = temp_data['thumbnail_url']

        return web.json_response(response_data, content_type='application/json')

    except Exception as e:
        logging.error(f"Error in generate_download_handler: {e}", exc_info=True)
        return web.json_response(
            {"success": False, "error": "Server error. Please try again later."},
            status=500,
            content_type='application/json'
        )


@routes.get(r"/watch/{path:.+}", allow_head=True)
async def stream_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        match = re.search(r"^([a-zA-Z0-9_-]{6})(\d+)$", path)
        if match:
            secure_hash = match.group(1)
            id = int(match.group(2))
        else:
            id = int(re.search(r"(\d+)(?:\/\S+)?", path).group(1))
            secure_hash = request.rel_url.query.get("hash")
        player = request.rel_url.query.get("player")
        access_code = request.rel_url.query.get("access_code")
        if not access_code:
            raise web.HTTPForbidden(text="This link requires an access_code.")
        _, access_error = await supabase_quota.validate_access_code(access_code)
        if access_error:
            raise web.HTTPForbidden(text=access_error["message"])
        return web.Response(
            text=await render_page(
                id,
                secure_hash,
                player=player,
                access_code=access_code,
            ),
            content_type='text/html',
        )
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except web.HTTPException:
        raise
    except (AttributeError, BadStatusLine, ConnectionResetError):
        return web.Response(status=200)
    except Exception as e:
        logging.critical(e.with_traceback(None))
        raise web.HTTPInternalServerError(text=str(e))


@routes.get(r"/thumb/{id}", allow_head=True)
async def thumb_handler(request: web.Request):
    """Serve Telegram's own embedded video thumbnail (metadata already attached to the
    message) as a fast poster image, instead of extracting a frame with ffmpeg."""
    try:
        id = int(request.match_info["id"])
        secure_hash = request.rel_url.query.get("hash")
        access_code = request.rel_url.query.get("access_code")
        if not access_code:
            raise web.HTTPForbidden(text="This link requires an access_code.")
        _, access_error = await supabase_quota.validate_access_code(access_code)
        if access_error:
            raise web.HTTPForbidden(text=access_error["message"])

        message = await StreamBot.get_messages(int(Var.BIN_CHANNEL), id)
        if not message or message.empty:
            raise FIleNotFound

        media = get_media_from_message(message)
        if not media:
            raise FIleNotFound

        unique_id = getattr(media, "file_unique_id", "") or ""
        if not secure_hash or unique_id[:6] != secure_hash:
            raise InvalidHash

        thumbs = getattr(media, "thumbs", None)
        if not thumbs:
            raise web.HTTPNotFound(text="No thumbnail available for this file")

        thumb_bytes = await StreamBot.download_media(thumbs[-1].file_id, in_memory=True)
        data = thumb_bytes.getvalue() if hasattr(thumb_bytes, "getvalue") else thumb_bytes

        return web.Response(
            body=data,
            content_type="image/jpeg",
            headers={"Cache-Control": "public, max-age=86400"},
        )
    except InvalidHash:
        raise web.HTTPForbidden(text="Invalid hash")
    except FIleNotFound:
        raise web.HTTPNotFound(text="File not found")
    except web.HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error in thumb_handler: {e}")
        raise web.HTTPInternalServerError(text="Error loading thumbnail")


@routes.get(r"/{path:.+}", allow_head=True)
async def path_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        match = re.search(r"^([a-zA-Z0-9_-]{6})(\d+)$", path)
        if match:
            secure_hash = match.group(1)
            id = int(match.group(2))
        else:
            id = int(re.search(r"(\d+)(?:\/\S+)?", path).group(1))
            secure_hash = request.rel_url.query.get("hash")
        return await media_streamer(request, id, secure_hash)
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except web.HTTPException:
        raise
    except (AttributeError, BadStatusLine, ConnectionResetError):
        return web.Response(status=200)
    except Exception as e:
        logging.critical(e.with_traceback(None))
        raise web.HTTPInternalServerError(text=str(e))


class_cache = {}

_client_home_dcs: dict = {}


async def _home_dc(index: int) -> int:
    if index not in _client_home_dcs:
        _client_home_dcs[index] = await multi_clients[index].storage.dc_id()
    return _client_home_dcs[index]


async def media_streamer(request: web.Request, id: int, secure_hash: str):
    """
    Optimized media streamer using StreamResponse and prefetching for high-speed streaming.
    """
    range_header = request.headers.get("Range", 0)
    access_code = request.rel_url.query.get("access_code")
    action = "download" if request.rel_url.query.get("download") == "1" else "stream"
    lease = None
    if not access_code:
        raise web.HTTPForbidden(text="This link requires an access_code.")
    if action == "download" and not supabase_quota.downloads_enabled:
        raise web.HTTPForbidden(
            text="Direct downloads are temporarily disabled. Please use streaming."
        )

    # Multi-client logic: find the least busy bot
    sorted_indices = sorted(work_loads, key=work_loads.get)
    candidates = sorted_indices[:3] if len(sorted_indices) >= 3 else sorted_indices

    file_id = None
    index = None
    tg_connect = None
    last_error = None

    for attempt, candidate_index in enumerate(candidates):
        candidate_client = multi_clients[candidate_index]

        if candidate_client in class_cache:
            streamer = class_cache[candidate_client]
        else:
            streamer = ByteStreamer(candidate_client)
            class_cache[candidate_client] = streamer

        try:
            file_id = await asyncio.wait_for(
                streamer.get_file_properties(id),
                timeout=15
            )
            index = candidate_index
            tg_connect = streamer
            if Var.MULTI_CLIENT or attempt > 0:
                logging.info(
                    f"Client {index} serving {request.remote}"
                    + (f" (retry attempt {attempt})" if attempt > 0 else "")
                )
            break
        except asyncio.TimeoutError:
            last_error = f"Client {candidate_index} timed out fetching file properties"
            logging.warning(f"{last_error}, trying next client...")
        except FIleNotFound:
            raise
        except Exception as e:
            last_error = str(e)
            logging.warning(f"Client {candidate_index} failed ({e}), trying next client...")

    if file_id is None:
        logging.error(f"All clients failed to fetch file properties. Last error: {last_error}")
        raise web.HTTPServiceUnavailable(text="Stream unavailable, please try again.")

    if file_id.unique_id[:6] != secure_hash:
        raise InvalidHash

    if request.method == "HEAD":
        _, access_error = await supabase_quota.validate_access_code(access_code)
        if access_error:
            raise web.HTTPForbidden(text=access_error["message"])

    file_size = file_id.file_size

    # Handle Range Headers for Seeking (Crucial for Video.js/Plyr)
    if range_header:
        from_bytes, until_bytes = range_header.replace("bytes=", "").split("-")
        from_bytes = int(from_bytes)
        until_bytes = int(until_bytes) if until_bytes else file_size - 1
    else:
        from_bytes = request.http_range.start or 0
        until_bytes = (request.http_range.stop or file_size) - 1

    if (until_bytes > file_size) or (from_bytes < 0) or (until_bytes < from_bytes):
        return web.Response(
            status=416,
            body="416: Range not satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    chunk_size = 1024 * 1024
    until_bytes = min(until_bytes, file_size - 1)

    offset = from_bytes - (from_bytes % chunk_size)
    first_part_cut = from_bytes - offset
    last_part_cut = until_bytes % chunk_size + 1

    req_length = until_bytes - from_bytes + 1
    part_count = math.ceil((until_bytes + 1) / chunk_size) - math.floor(offset / chunk_size)

    if request.method != "HEAD":
        lease, quota_error = await supabase_quota.acquire(
            access_code,
            action,
        )
        if quota_error:
            status = quota_error["status"]
            message = quota_error["message"]
            if status == 429:
                raise web.HTTPTooManyRequests(text=message)
            if status == 403:
                raise web.HTTPForbidden(text=message)
            if status == 400:
                raise web.HTTPBadRequest(text=message)
            raise web.HTTPServiceUnavailable(text=message)

    mime_type = file_id.mime_type or "application/octet-stream"
    file_name = file_id.file_name
    if file_name:
        file_name = re.sub(r"[\r\n\t\x00-\x1f\x7f]", "", str(file_name)).strip()
        if not file_name:
            file_name = f"{secrets.token_hex(2)}.bin"

    # Use StreamResponse for better performance and reduced buffering
    response = web.StreamResponse(
        status=206 if range_header else 200,
        headers={
            "Content-Type": f"{mime_type}",
            "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
            "Content-Length": str(req_length),
            "Content-Disposition": f'attachment; filename="{file_name}"',
            "Accept-Ranges": "bytes",
            "Access-Control-Allow-Origin": "*", # Good for PWA usage
        },
    )

    try:
        await response.prepare(request)
        if request.method == "HEAD":
            return response
        # yield_file is now an optimized async generator in custom_dl.py
        transfer_started = time.monotonic()
        bytes_sent = 0
        async for chunk in tg_connect.yield_file(
            file_id, index, offset, first_part_cut, last_part_cut, part_count, chunk_size
        ):
            await response.write(chunk)
            bytes_sent += len(chunk)
            max_bps = supabase_quota.max_transfer_bytes_per_second
            if max_bps > 0:
                target_elapsed = bytes_sent / max_bps
                elapsed = time.monotonic() - transfer_started
                if target_elapsed > elapsed:
                    await asyncio.sleep(target_elapsed - elapsed)
            else:
                await asyncio.sleep(0)
    except (ConnectionResetError, RuntimeError):
        # User closed the player or disconnected
        pass
    except Exception as e:
        logging.error(f"Streaming error on request: {e}")

    finally:
        await supabase_quota.release(lease)

    return response


# ──────────────────────────────────────────────────────────────────────────────
# /root-tree  —  GitHub repo file index (admin use)
# ──────────────────────────────────────────────────────────────────────────────

_ROOT_REPO   = "sunday2212/WEBREPLITX5"
_ROOT_FOLDER = "frontend/1234xxx"


def _build_tree(flat_items: list) -> dict:
    """
    Convert GitHub's flat tree list into a nested dict.
    Only keeps .html blobs and their parent directories inside _ROOT_FOLDER.
    Structure: { name: {'_t': 'dir', '_c': {...}} | {'_t': 'file'} }
    """
    root: dict = {}
    prefix = _ROOT_FOLDER + "/"

    for item in flat_items:
        path: str = item.get("path", "")
        kind: str = item.get("type", "")

        if not path.startswith(prefix):
            continue

        rel = path[len(prefix):]
        if not rel:
            continue

        if kind == "blob" and not rel.endswith(".html"):
            continue

        parts = rel.split("/")
        node = root
        for i, part in enumerate(parts):
            is_last = (i == len(parts) - 1)
            if is_last:
                if kind == "blob":
                    node[part] = {"_t": "file"}
                else:
                    node.setdefault(part, {"_t": "dir", "_c": {}})
            else:
                node.setdefault(part, {"_t": "dir", "_c": {}})
                node = node[part]["_c"]

    return root


def _render_tree_html(node: dict, depth: int = 0) -> str:
    """Recursively render the nested tree as HTML details/summary."""
    if not node:
        return '<p class="empty">— empty —</p>'

    dirs  = sorted(k for k, v in node.items() if v["_t"] == "dir")
    files = sorted(k for k, v in node.items() if v["_t"] == "file")
    html  = ""

    for name in dirs:
        children = node[name].get("_c", {})
        def _count(n):
            t = sum(1 for v in n.values() if v["_t"] == "file")
            for v in n.values():
                if v["_t"] == "dir":
                    t += _count(v.get("_c", {}))
            return t
        cnt = _count(children)
        badge = f'<span class="badge">{cnt}</span>' if cnt else ""
        inner = _render_tree_html(children, depth + 1)
        html += (
            f'<details>'
            f'<summary><span class="arr">▶</span>📁 {name} {badge}</summary>'
            f'<div class="indent">{inner}</div>'
            f'</details>'
        )

    for name in files:
        display = name[:-5] if name.endswith(".html") else name
        html += f'<div class="file"><span class="fi">📄</span>{display}</div>'

    return html


@routes.get("/root-tree")
async def root_tree_handler(request: web.Request) -> web.Response:
    """Serve an interactive collapsible file index of the GitHub repo folder."""
    token = Var.GIT_TOKEN
    if not token:
        return web.Response(
            text="<!DOCTYPE html><html><body style='font-family:sans-serif;padding:40px;background:#0d1117;color:#f85149'>"
                 "<h2>⚙️ Configuration Required</h2>"
                 "<p>Set the <code>GIT_TOKEN</code> environment variable on the server and restart the bot.</p>"
                 "</body></html>",
            content_type="text/html", status=200
        )

    api_url = f"https://api.github.com/repos/{_ROOT_REPO}/git/trees/main?recursive=1"
    headers_gh = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "StreamBot-FileIndex/1.0",
    }

    try:
        async with aiohttp_client.ClientSession() as session:
            async with session.get(api_url, headers=headers_gh, timeout=aiohttp_client.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    return web.Response(
                        text=f"<h2>GitHub API error {resp.status}</h2><pre>{body[:500]}</pre>",
                        content_type="text/html", status=502
                    )
                data = await resp.json()
    except Exception as exc:
        logging.error(f"root-tree GitHub fetch error: {exc}")
        return web.Response(
            text=f"<h2>Fetch error</h2><pre>{exc}</pre>",
            content_type="text/html", status=502
        )

    truncated = data.get("truncated", False)
    flat_items = data.get("tree", [])
    tree = _build_tree(flat_items)
    tree_html = _render_tree_html(tree)

    ts = datetime.now(timezone.utc).strftime("%d %b %Y, %H:%M UTC")
    trunc_warn = (
        '<p class="warn">⚠️ Repository tree was truncated by GitHub — some files may be missing.</p>'
        if truncated else ""
    )

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>File Index — {_ROOT_FOLDER}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#0d1117;color:#c9d1d9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;padding:20px 16px;min-height:100vh}}
h1{{color:#58a6ff;font-size:1.35rem;margin-bottom:4px}}
.meta{{color:#8b949e;font-size:.78rem;margin-bottom:18px}}
.warn{{background:#3d1f00;color:#e3b341;border:1px solid #e3b341;border-radius:6px;padding:8px 12px;margin-bottom:12px;font-size:.82rem}}
.tree{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px 12px}}
details{{margin:2px 0}}
summary{{
  cursor:pointer;padding:7px 10px;border-radius:6px;
  display:flex;align-items:center;gap:6px;
  font-weight:600;color:#58a6ff;
  list-style:none;-webkit-tap-highlight-color:transparent
}}
summary::-webkit-details-marker{{display:none}}
summary:hover{{background:#21262d}}
details[open]>summary{{color:#79c0ff}}
.arr{{font-size:.6rem;color:#8b949e;transition:transform .15s;display:inline-block;min-width:10px}}
details[open]>summary .arr{{transform:rotate(90deg)}}
.indent{{padding-left:18px;border-left:1px solid #30363d;margin-left:15px;margin-top:2px}}
.file{{padding:6px 10px 6px 36px;color:#c9d1d9;font-size:.88rem;border-radius:4px;display:flex;align-items:center;gap:7px}}
.file:hover{{background:#21262d}}
.fi{{font-size:.9rem}}
.badge{{background:#21262d;color:#8b949e;font-size:.68rem;padding:1px 6px;border-radius:10px;font-weight:400;margin-left:4px}}
.badge{{background:#21262d;color:#8b949e;font-size:.68rem;padding:1px 6px;border-radius:10px;font-weight:400;margin-left:4px}}
.empty{{color:#8b949e;font-style:italic;padding:6px 10px;font-size:.82rem}}
</style>
</head>
<body>
<h1>📁 File Index</h1>
<p class="meta">Last updated: {ts}</p>
{trunc_warn}
<div class="tree">
{tree_html}
</div>
</body>
</html>"""

    return web.Response(text=page, content_type="text/html", charset="utf-8")
