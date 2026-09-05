import re
import logging
import urllib.parse

import jinja2

from biisal.vars import Var
from biisal.bot import StreamBot
from biisal.utils.human_readable import humanbytes
from biisal.utils.file_properties import get_file_ids
from biisal.server.exceptions import InvalidHash


def _sanitize_filename(name: str) -> str:
    return re.sub(r"[\r\n\t\x00-\x1f\x7f]", "", str(name or "")).strip()


async def render_page(id, secure_hash, src=None, player=None, access_code=None):
    file_data = await get_file_ids(StreamBot, int(Var.BIN_CHANNEL), int(id))

    if file_data.unique_id[:6] != secure_hash:
        logging.debug(f"link hash: {secure_hash} - {file_data.unique_id[:6]}")
        logging.debug(f"Invalid hash for message ID {id}")
        raise InvalidHash

    raw_name = file_data.file_name or ""
    clean_name = _sanitize_filename(raw_name) or "file"

    query = {"hash": secure_hash}
    if access_code:
        query["access_code"] = access_code
    src = urllib.parse.urljoin(
        Var.URL,
        f"{id}/{urllib.parse.quote_plus(clean_name)}?{urllib.parse.urlencode(query)}",
    )

    mime_type = file_data.mime_type or ""
    tag = mime_type.split("/")[0].strip() or "video"
    file_size = humanbytes(file_data.file_size)
    display_name = clean_name.replace("_", " ")

    poster_url = ""
    if tag == "video" and getattr(file_data, "has_thumb", False):
        poster_query = {"hash": secure_hash}
        if access_code:
            poster_query["access_code"] = access_code
        poster_url = urllib.parse.urljoin(
            Var.URL,
            f"thumb/{id}?{urllib.parse.urlencode(poster_query)}",
        )

    if tag in ("video", "audio"):
        if player == "videojs":
            template_file = "biisal/template/req_videojs.html"
        else:
            template_file = "biisal/template/req.html"
    else:
        template_file = "biisal/template/dl.html"

    with open(template_file) as f:
        template = jinja2.Template(f.read())

    return template.render(
        file_name=display_name,
        file_url=src,
        file_size=file_size,
        file_unique_id=file_data.unique_id,
        tag=tag,
        mime_type=mime_type,
        player=player or "plyr",
        poster_url=poster_url,
    )
