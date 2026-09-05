import base64
import aiohttp
import logging
import secrets
from pathlib import Path


async def upload_image_to_github(
    image_path: str,
    github_token: str,
    folder_name: str,
    title_name: str,
    repo_owner: str = "afrahtafreeh",
    repo_name: str = "thumbimages"
) -> str:
    try:
        if not github_token:
            raise ValueError("GitHub token is required")

        with open(image_path, 'rb') as f:
            image_data = f.read()

        sanitized_title = sanitize_filename(title_name)
        unique_suffix = secrets.token_hex(4)
        image_filename = f"{sanitized_title}_{unique_suffix}.jpg"

        github_path = f"{folder_name}/{image_filename}"

        api_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{github_path}"

        content_encoded = base64.b64encode(image_data).decode('utf-8')

        headers = {
            "Authorization": f"Bearer {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }

        async with aiohttp.ClientSession() as session:
            sha = None
            async with session.get(api_url, headers=headers) as resp_get:
                if resp_get.status == 200:
                    data = await resp_get.json()
                    sha = data.get('sha')
                elif resp_get.status != 404:
                    text = await resp_get.text()
                    logging.warning(f"GitHub GET returned {resp_get.status}: {text}")

            payload = {
                "message": f"Add thumbnail: {image_filename}",
                "content": content_encoded
            }
            if sha:
                payload["sha"] = sha

            async with session.put(api_url, headers=headers, json=payload) as resp_put:
                if resp_put.status in (200, 201):
                    raw_url = f"https://raw.githubusercontent.com/{repo_owner}/{repo_name}/main/{github_path}"
                    logging.info(f"Successfully uploaded thumbnail to GitHub: {raw_url}")
                    return raw_url
                else:
                    text = await resp_put.text()
                    error_msg = f"GitHub upload failed with status {resp_put.status}: {text}"
                    logging.error(error_msg)
                    raise RuntimeError(error_msg)

    except Exception as e:
        logging.error(f"Error uploading image to GitHub: {e}")
        raise


def sanitize_filename(filename: str) -> str:
    import re
    sanitized = re.sub(r'[<>:"/\\|?*]', '', filename)
    sanitized = re.sub(r'\s+', '_', sanitized)
    sanitized = sanitized[:100]
    if not sanitized:
        sanitized = f"file_{secrets.token_hex(4)}"
    return sanitized

