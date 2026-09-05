"""Supabase-backed access-code validation and media quota enforcement."""

import asyncio
import logging
import os
from collections import defaultdict
from typing import Optional

import aiohttp


logger = logging.getLogger("stream.supabase_quota")


class SupabaseQuota:
    """Coordinates atomic daily claims in Supabase and local active-request caps."""

    def __init__(self):
        self.url = os.getenv("SUPABASE_URL", "").rstrip("/")
        self.key = os.getenv("SUPABASE_KEY", "")
        self.download_limit = self._int_env("MEDIA_DOWNLOAD_DAILY_LIMIT", 5)
        self.stream_limit = self._int_env("MEDIA_STREAM_DAILY_LIMIT", 5)
        self.max_active_downloads = self._int_env(
            "MEDIA_MAX_ACTIVE_DOWNLOADS_PER_USER", 1
        )
        self.max_active_streams = self._int_env(
            "MEDIA_MAX_ACTIVE_STREAMS_PER_USER", 1
        )
        self.max_active_requests_global = self._int_env(
            "MEDIA_MAX_ACTIVE_REQUESTS_GLOBAL", 0, minimum=0
        )
        self.max_transfer_bytes_per_second = self._int_env(
            "MEDIA_MAX_TRANSFER_BYTES_PER_SECOND", 1_000_000
        )
        self._active = defaultdict(lambda: {"download": 0, "stream": 0})
        self._global_active = 0
        self._active_lock = asyncio.Lock()

    @staticmethod
    def _int_env(name: str, default: int, minimum: int = 1) -> int:
        try:
            return max(minimum, int(os.getenv(name, str(default))))
        except (TypeError, ValueError):
            return default

    @property
    def enabled(self) -> bool:
        return bool(self.url and self.key)

    @property
    def downloads_enabled(self) -> bool:
        return os.getenv("MEDIA_DOWNLOADS_ENABLED", "true").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

    async def _rpc(self, function_name: str, payload: dict):
        if not self.enabled:
            return None, "Supabase quota service is not configured"

        endpoint = f"{self.url}/rest/v1/rpc/{function_name}"
        headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        try:
            timeout = aiohttp.ClientTimeout(total=8)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(endpoint, json=payload, headers=headers) as response:
                    body_text = await response.text()
                    if response.status < 200 or response.status >= 300:
                        logger.error(
                            "Supabase RPC %s failed with status %s: %s",
                            function_name,
                            response.status,
                            body_text[:500],
                        )
                        return None, "Supabase quota service returned an error"
                    try:
                        return await _parse_json_body(body_text), None
                    except ValueError:
                        logger.error("Supabase RPC %s returned invalid JSON", function_name)
                        return None, "Supabase quota service returned invalid data"
        except (aiohttp.ClientError, asyncio.TimeoutError) as error:
            logger.error("Supabase RPC %s request failed: %s", function_name, error)
            return None, "Supabase quota service is unavailable"

    async def _resolve_code(self, code: str):
        result, error = await self._rpc(
            "resolve_media_access_code",
            {"p_code": code},
        )
        if error:
            return None, error, 503

        result = _first_object(result)
        if not result or not result.get("valid"):
            return None, "This link is invalid or has expired.", 403

        user_id = result.get("user_id")
        if not user_id:
            return None, "This link is not assigned to a user.", 403
        return str(user_id), None, None

    async def validate_access_code(self, code: str):
        """Validate a code without consuming a stream/download quota unit."""
        code = (code or "").strip()
        if not code:
            return None, {
                "status": 403,
                "message": "This link requires an access_code.",
            }

        user_id, error, status = await self._resolve_code(code)
        if error:
            return None, {"status": status, "message": error}
        return user_id, None

    async def acquire(self, code: str, action: str):
        """Consume one idempotent daily claim and reserve one active request."""
        code = (code or "").strip()
        if not code:
            return None, {"status": 403, "message": "A valid access_code is required."}
        if action not in ("download", "stream"):
            return None, {"status": 400, "message": "Invalid media action."}
        if action == "download" and not self.downloads_enabled:
            return None, {
                "status": 403,
                "message": "Direct downloads are temporarily disabled. Please use streaming.",
            }

        user_id, error, status = await self._resolve_code(code)
        if error:
            return None, {"status": status, "message": error}

        limit = (
            self.max_active_downloads
            if action == "download"
            else self.max_active_streams
        )
        async with self._active_lock:
            if (
                self.max_active_requests_global > 0
                and self._global_active >= self.max_active_requests_global
            ):
                return None, {
                    "status": 429,
                    "message": (
                        "The server is currently serving its maximum number of "
                        "media requests. Please try again shortly."
                    ),
                }
            active = self._active[user_id][action]
            if active >= limit:
                return None, {
                    "status": 429,
                    "message": (
                        f"Too many active {action}s for this user. "
                        "Please wait for one to finish."
                    ),
                }
            self._global_active += 1
            self._active[user_id][action] += 1

        result, error = await self._rpc(
            "claim_media_access",
            {
                "p_code": code,
                "p_action": action,
            },
        )
        if error:
            await self.release((user_id, action))
            return None, {"status": 503, "message": error}

        result = _first_object(result)
        if not result or not result.get("allowed"):
            await self.release((user_id, action))
            reason = (result or {}).get("reason")
            if reason == "daily_limit":
                return None, {
                    "status": 429,
                    "message": (
                        f"Daily {action} limit reached. "
                        "Please try again after the daily reset."
                    ),
                }
            return None, {
                "status": 403,
                "message": "This link is invalid or has expired.",
            }

        return (user_id, action), None

    async def release(self, lease: Optional[tuple]):
        if not lease:
            return
        user_id, action = lease
        async with self._active_lock:
            self._global_active = max(0, self._global_active - 1)
            counts = self._active.get(user_id)
            if not counts:
                return
            counts[action] = max(0, counts[action] - 1)
            if counts["download"] == 0 and counts["stream"] == 0:
                self._active.pop(user_id, None)


async def _parse_json_body(body_text: str):
    import json

    return json.loads(body_text)


def _first_object(value):
    if isinstance(value, list):
        return value[0] if value and isinstance(value[0], dict) else None
    return value if isinstance(value, dict) else None


supabase_quota = SupabaseQuota()
