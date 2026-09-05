import re
import os
import asyncio
import json
import logging
from pathlib import Path
from biisal.bot import StreamBot
from biisal.utils.database import Database
from biisal.utils.human_readable import humanbytes
from biisal.vars import Var
from urllib.parse import quote_plus
from pyrogram import filters, Client
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from biisal.utils.file_properties import get_name, get_hash, get_media_from_message
from helper_func import encode, get_message_id, decode, get_messages
from biisal.utils.github_uploader import upload_image_to_github

db = Database(Var.DATABASE_URL, Var.name)
CUSTOM_CAPTION = os.environ.get("CUSTOM_CAPTION", None)
PROTECT_CONTENT = os.environ.get('PROTECT_CONTENT', "False") == "True"
DISABLE_CHANNEL_BUTTON = os.environ.get("DISABLE_CHANNEL_BUTTON", None) == 'True'
GIT_TOKEN = os.environ.get('GIT_TOKEN', '')
THUMB_API = os.environ.get('THUMB_API', '')

GITHUB_OWNER_REPO = "sunday2212/WEBREPLITX5"

# State machine for /batch and /fwd conversations: user_id -> {'state': str, 'data': dict}
_batch_sessions = {}
_fwd_sessions = {}
_fbatch_sessions = {}   # user_id → {'state': str}

_FBATCH_CHUNK = 100     # message IDs per get_messages() call
_FBATCH_DELAY = 0.4     # seconds between chunks (flood-safe)

def sanitize_caption(text: str) -> str:
    """Sanitize caption by removing HTML tags, links, @mentions, and hashtags"""
    if not text:
        return text
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Remove @mentions
    text = re.sub(r'@[\w_]+', '', text)
    # Remove all kinds of links
    text = re.sub(r'(?:https?://|t\.me/|telegram\.me/)[^\s]+', '', text)
    # Remove hashtags
    text = re.sub(r'\s*#\w+', '', text)
    # Clean up extra spaces
    text = re.sub(r'\s+', ' ', text.strip())
    return text

async def create_intermediate_link(message: Message):
    """Create intermediate link for the message and store data temporarily.
    Uses the current domain's BASE_URL for complete domain independence."""
    # Extract file information
    media = get_media_from_message(message)
    if not media:
        raise ValueError("No media found in message")

    # Get caption with fallback chain and sanitization
    caption = ""
    
    # Try message caption first
    if message.caption:
        caption = sanitize_caption(message.caption.html)
    
    # Fallback to filename if caption is empty after sanitization
    if not caption or not caption.strip():
        filename = getattr(media, 'file_name', None) or get_name(message)
        if filename:
            caption = sanitize_caption(filename)
    
    # Fallback to random name if still empty
    if not caption or not caption.strip():
        import secrets
        caption = f"file_{secrets.token_hex(4)}"
    
    # Prepare message data for temporary storage
    message_data = {
        'message_id': message.id,
        'file_name': getattr(media, 'file_name', None) or get_name(message),
        'file_size': getattr(media, 'file_size', 0),
        'mime_type': getattr(media, 'mime_type', 'application/octet-stream'),
        'caption': caption,
        'from_chat_id': message.chat.id,
        'file_unique_id': getattr(media, 'file_unique_id', '')
    }
    
    # Store with current domain for domain independence
    current_domain = Var.get_current_domain()
    token = await db.store_temp_file(message_data, domain=current_domain)
    
    # Use the current instance's BASE_URL (not hardcoded URL_WEB)
    base_url = Var.get_base_url()
    intermediate_link = f"{base_url}prepare/{token}"
    
    return intermediate_link, caption

async def create_intermediate_link_for_batch(message: Message, folder_name: str = None, client: Client = None):
    """Create intermediate links for batch processing - both stream and download, with optional thumbnail.
    
    DOMAIN INDEPENDENCE: Each deployment generates links ONLY for its own domain.
    Set SERVE_DOMAIN='web' or SERVE_DOMAIN='webx' on each Heroku instance.
    If DUAL_DOMAIN_ENABLED=True and no SERVE_DOMAIN set, generates for both (legacy mode)."""
    try:
        media = get_media_from_message(message)
        if not media:
            raise ValueError("No media found in message")

        # Get caption with fallback chain and sanitization
        caption = ""
        
        # Try message caption first
        if message.caption:
            caption = sanitize_caption(message.caption.html)
        
        # Fallback to filename if caption is empty after sanitization
        if not caption or not caption.strip():
            filename = getattr(media, 'file_name', None) or get_name(message)
            if filename:
                caption = sanitize_caption(filename)
        
        # Fallback to random name if still empty
        if not caption or not caption.strip():
            import secrets
            caption = f"file_{secrets.token_hex(4)}"
        
        message_data = {
            'message_id': message.id,
            'file_name': getattr(media, 'file_name', None) or get_name(message),
            'file_size': getattr(media, 'file_size', 0),
            'mime_type': getattr(media, 'mime_type', 'application/octet-stream'),
            'caption': caption,
            'from_chat_id': message.chat.id,
            'file_unique_id': getattr(media, 'file_unique_id', '')
        }
        
        # Grab and upload the poster for video files BEFORE storing in database.
        # Uses Telegram's own embedded video thumbnail (already attached to the message
        # metadata at upload time) instead of downloading the full video and running
        # ffmpeg on it — this is a few KB and near-instant per video, vs. downloading
        # the whole video and extracting a frame.
        mime_type = getattr(media, 'mime_type', '')
        thumbnail_url = None

        logging.info(f"Thumbnail check - mime_type: {mime_type}, folder_name: {folder_name}, THUMB_API: {'Present' if THUMB_API else 'Missing'}, client: {'Present' if client else 'Missing'}")

        if mime_type and mime_type.startswith('video/') and folder_name and THUMB_API and client:
            thumbs = getattr(media, 'thumbs', None)
            if thumbs:
                temp_thumb_path = None
                try:
                    logging.info(f"🖼️  Fetching embedded Telegram thumbnail for: {caption}")

                    temp_dir = Path("/tmp/batch_thumbs")
                    temp_dir.mkdir(exist_ok=True)
                    import secrets as sec
                    temp_thumb_path = str(temp_dir / f"thumb_{sec.token_hex(8)}.jpg")

                    # Download only the small embedded thumbnail, not the full video
                    await client.download_media(thumbs[-1].file_id, file_name=temp_thumb_path)

                    thumb_size = os.path.getsize(temp_thumb_path) if os.path.exists(temp_thumb_path) else 0
                    logging.info(f"   Thumbnail fetched ({thumb_size} bytes)")

                    logging.info(f"   Uploading thumbnail to GitHub (folder: {folder_name})...")
                    thumbnail_url = await upload_image_to_github(
                        image_path=temp_thumb_path,
                        github_token=THUMB_API,
                        folder_name=folder_name,
                        title_name=caption
                    )

                    logging.info(f"✅ Thumbnail uploaded successfully: {thumbnail_url}")

                except Exception as thumb_error:
                    thumb_err_str = str(thumb_error)
                    logging.error(f"❌ Thumbnail failed for '{caption}': {thumb_err_str}", exc_info=True)
                    if not message_data.get('_thumb_error'):
                        message_data['_thumb_error'] = thumb_err_str
                finally:
                    try:
                        if temp_thumb_path and os.path.exists(temp_thumb_path):
                            os.remove(temp_thumb_path)
                    except Exception as cleanup_error:
                        logging.error(f"Error cleaning up temp thumbnail: {cleanup_error}")
            else:
                logging.info(f"⏭️  No embedded thumbnail on message for {caption}; skipping poster")
        else:
            reasons = []
            if not mime_type or not mime_type.startswith('video/'):
                reasons.append(f"not a video (mime: {mime_type})")
            if not folder_name:
                reasons.append("no folder_name provided")
            if not THUMB_API:
                reasons.append("THUMB_API not configured")
            if not client:
                reasons.append("no client provided")
            if reasons:
                logging.info(f"⏭️  Skipping thumbnail for {caption}: {', '.join(reasons)}")
        
        if thumbnail_url:
            message_data['thumbnail_url'] = thumbnail_url
        
        # DOMAIN INDEPENDENCE: Get current domain and base URL
        current_domain = Var.get_current_domain()
        base_url = Var.get_base_url()
        
        # If SERVE_DOMAIN is set (web or webx), only create token for THIS domain
        # This ensures complete independence - each Heroku app handles its own domain
        if current_domain:
            token = await db.store_temp_file(message_data, domain=current_domain)
            stream_link = f"{base_url}prepare/{token}?type=stream"
            download_link = f"{base_url}prepare/{token}?type=download"
            
            result = {
                "title": caption,
                "streamingUrl": stream_link,
                "downloadUrl": download_link
            }
        else:
            # Legacy mode: if no SERVE_DOMAIN set, create tokens for both (backwards compatible)
            token_web = await db.store_temp_file(message_data, domain='web')
            token_webx = await db.store_temp_file(message_data, domain='webx')
            
            stream_link = f"{Var.URL_WEB}prepare/{token_web}?type=stream"
            stream_link_x = f"{Var.URL_WEBX}prepare/{token_webx}?type=stream"
            download_link = f"{Var.URL_WEB}prepare/{token_web}?type=download"
            download_link_x = f"{Var.URL_WEBX}prepare/{token_webx}?type=download"
            
            result = {
                "title": caption,
                "streamingUrl": stream_link,
                "streamingUrlx": stream_link_x,
                "downloadUrl": download_link,
                "downloadUrlx": download_link_x
            }
        
        if thumbnail_url:
            result["thumbnailUrl"] = thumbnail_url

        # Carry thumb error forward so the batch loop can surface it to the bot
        thumb_err = message_data.get('_thumb_error')
        if thumb_err:
            result["_thumb_error"] = thumb_err

        return result
    except Exception as e:
        raise ValueError(f"Failed to create intermediate links: {str(e)}")

async def create_pdf_download_links(message: Message):
    """Create download-only links for PDF files. No streaming URL, no thumbnail."""
    media = get_media_from_message(message)
    if not media:
        raise ValueError("No media found in message")

    # Title priority: caption → filename → random
    title = ""
    if message.caption:
        title = sanitize_caption(message.caption.html)
    if not title or not title.strip():
        filename = getattr(media, 'file_name', None) or get_name(message)
        if filename:
            title = sanitize_caption(filename)
    if not title or not title.strip():
        import secrets as _sec
        title = f"pdf_{_sec.token_hex(4)}"

    message_data = {
        'message_id': message.id,
        'file_name': getattr(media, 'file_name', None) or get_name(message),
        'file_size': getattr(media, 'file_size', 0),
        'mime_type': getattr(media, 'mime_type', 'application/pdf'),
        'caption': title,
        'from_chat_id': message.chat.id,
        'file_unique_id': getattr(media, 'file_unique_id', '')
    }

    current_domain = Var.get_current_domain()

    if current_domain:
        token = await db.store_temp_file(message_data, domain=current_domain)
        base_url = Var.get_base_url()
        download_link = f"{base_url}prepare/{token}?type=download"
        if current_domain == 'web':
            return {"title": title, "pdf_downloadUrl": download_link}
        else:
            return {"title": title, "pdf_downloadUrlx": download_link}
    else:
        # Legacy mode: generate for both domains
        token_web = await db.store_temp_file(message_data, domain='web')
        token_webx = await db.store_temp_file(message_data, domain='webx')
        return {
            "title": title,
            "pdf_downloadUrl": f"{Var.URL_WEB}prepare/{token_web}?type=download",
            "pdf_downloadUrlx": f"{Var.URL_WEBX}prepare/{token_webx}?type=download"
        }


async def process_message(msg, json_output, skipped_messages, folder_name=None, client=None):
    """Process individual message and create intermediate link (updated for new system with thumbnail support)"""
    try:
        # Silently skip plain text messages (no media at all)
        if not (msg.document or msg.video or msg.audio):
            return

        # PDF documents → download-only links, no streaming or thumbnail
        is_pdf = (
            msg.document and
            getattr(msg.document, 'mime_type', '') == 'application/pdf'
        )

        if is_pdf:
            pdf_data = await create_pdf_download_links(msg)
            json_output.append(pdf_data)
            return

        # Videos / audio / other documents → full streaming + download links
        intermediate_data = await create_intermediate_link_for_batch(msg, folder_name, client)
        json_output.append(intermediate_data)

    except Exception as e:
        # Capture details for skipped messages
        file_name = get_name(msg) or "Unknown"
        skipped_messages.append({
            "id": msg.id,
            "file_name": file_name,
            "reason": str(e)
        })

def generate_lecture_html(json_filename: str, github_dest_folder: str = '') -> str:
    """Generate the lecture HTML page for a given JSON filename, with dynamic relative paths.
    
    Depth rule:
      github_dest_folder = "owner/repo/user/path/parts"
      The HTML sits inside that folder, so to reach the repo root on GitHub Pages
      we need (number of user path parts) levels of '../'.
      We strip the first two components (owner + repo) to get the user sub-folders.

    Examples:
      owner/repo/1234xxx              → depth 1  → ../
      owner/repo/1234xxx/marrow       → depth 2  → ../../
      owner/repo/1234xxx/marrow/anat  → depth 3  → ../../../
    """
    parts = [p for p in github_dest_folder.strip('/').split('/') if p]
    depth = max(len(parts) - 2, 0)   # subtract owner & repo, count only sub-folders
    prefix = '../' * depth if depth > 0 else './'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Lectures - NEXTPULSE | NEET PG Preparation</title>
  <meta name="description" content="Access comprehensive video lectures. Expert faculty for NEET PG preparation.">
  <meta name="keywords" content="NEET PG, medical lectures, video lectures">
  <link rel="stylesheet" href="{prefix}styles.css">
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
  <style>
    .completion-toggle {{
      position: absolute;
      top: 1rem;
      right: 1rem;
      cursor: pointer;
      z-index: 10;
    }}

    .completion-circle {{
      width: 32px;
      height: 32px;
      border: 2px solid #e2e8f0;
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
      background: white;
      transition: all 0.3s ease;
      box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }}

    .completion-circle:hover {{
      border-color: #4299e1;
      transform: scale(1.05);
    }}

    .completion-circle.completed {{
      background: #48bb78;
      border-color: #48bb78;
    }}

    .completion-circle .fas.fa-check {{
      color: white;
      font-size: 14px;
      opacity: 0;
      transition: opacity 0.3s ease;
    }}

    .completion-circle .fas.fa-check.visible {{
      opacity: 1;
    }}

    .lecture-card {{
      position: relative;
    }}

    .video-popup {{
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background: rgba(0, 0, 0, 0.8);
      display: flex;
      align-items: center;
      justify-content: center;
      z-index: 1000;
    }}

    .video-popup-content {{
      background: white;
      border-radius: 8px;
      width: 90%;
      max-width: 800px;
      max-height: 90vh;
      overflow: auto;
      position: relative;
      padding: 20px;
    }}

    .refresh-popup {{
      position: absolute;
      top: 10px;
      left: 15px;
      background: white;
      border: none;
      border-radius: 50%;
      width: 32px;
      height: 32px;
      font-size: 16px;
      cursor: pointer;
      color: #333;
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 2px 5px rgba(0,0,0,0.2);
      transition: all 0.3s ease;
    }}

    .refresh-popup:hover {{
      background: #f0f0f0;
      transform: scale(1.1);
    }}

    .close-popup {{
      position: absolute;
      top: 10px;
      right: 15px;
      background: white;
      border: none;
      border-radius: 50%;
      width: 32px;
      height: 32px;
      font-size: 20px;
      cursor: pointer;
      color: #333;
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 2px 5px rgba(0,0,0,0.2);
      transition: all 0.3s ease;
    }}

    .close-popup:hover {{
      background: #f0f0f0;
      transform: scale(1.1);
    }}

    .video-title {{
      margin-top: 0;
      margin-bottom: 15px;
      color: #2c3e50;
    }}

    .iframe-container {{
      position: relative;
      width: 100%;
      height: 0;
      padding-bottom: 56.25%;
    }}

    .iframe-container iframe {{
      position: absolute;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      border: none;
      border-radius: 4px;
    }}

    .lecture-card {{
      background: white;
      border-radius: 8px;
      padding: 20px;
      margin-bottom: 15px;
      box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
      transition: transform 0.2s;
    }}

    .lecture-card:hover {{
      transform: translateY(-3px);
    }}

    .lecture-card h3 {{
      margin-top: 0;
      margin-bottom: 15px;
      color: #2c3e50;
    }}

    .button-container {{
      display: flex;
      gap: 10px;
    }}

    .stream-button, .download-button {{
      padding: 8px 16px;
      border: none;
      border-radius: 4px;
      cursor: pointer;
      font-weight: 600;
      display: flex;
      align-items: center;
      gap: 5px;
      transition: background-color 0.2s;
    }}

    .stream-button {{
      background-color: #3498db;
      color: white;
    }}

    .stream-button:hover {{
      background-color: #2980b9;
    }}

    .download-button {{
      background-color: #2ecc71;
      color: white;
    }}

    .download-button:hover {{
      background-color: #27ae60;
    }}
  </style>
  <script src="{prefix}access-control.js"></script>
  <script src="{prefix}block.js"></script>
  <script src="{prefix}error-handler/link-checker.js"></script>
  <script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=ca-pub-5920367457745298"
     crossorigin="anonymous"></script>
</head>
<body>
<script>
  if (!accessControl.initProtectedPage()) {{
    throw new Error('Access denied - redirecting to index.html');
  }}
</script>
  <header>
    <div class="header-content">
      <button onclick="history.back()" style="position: fixed; top: 20px; left: 20px; background: transparent; color: white; border: none; padding: 10px; cursor: pointer; z-index: 1001; font-size: 20px;">
        <i class="fas fa-arrow-left"></i>
      </button>
      <div class="search-bar">
        <i class="fas fa-search"></i>
        <input type="text" placeholder="Search lectures..." id="searchInput">
      </div>
    </div>
  </header>
  <main>
    <div class="lecture-list" id="lectureList">
      <p>Loading lectures...</p>
    </div>
  </main>

  <script>
    let lecturesData = [];

    class LectureCompletion {{
      constructor() {{
        this.storageKey = 'lectureCompletions';
      }}

      isCompleted(platform, subject, lectureTitle) {{
        const completions = this.getCompletions();
        const key = `${{platform}}-${{subject}}-${{lectureTitle}}`;
        return completions[key] || false;
      }}

      toggleCompletion(platform, subject, lectureTitle) {{
        const completions = this.getCompletions();
        const key = `${{platform}}-${{subject}}-${{lectureTitle}}`;
        completions[key] = !completions[key];
        this.saveCompletions(completions);
        return completions[key];
      }}

      getCompletions() {{
        try {{
          const stored = localStorage.getItem(this.storageKey);
          return stored ? JSON.parse(stored) : {{}};
        }} catch (error) {{
          console.error('Error reading completions from localStorage:', error);
          return {{}};
        }}
      }}

      saveCompletions(completions) {{
        try {{
          localStorage.setItem(this.storageKey, JSON.stringify(completions));
        }} catch (error) {{
          console.error('Error saving completions to localStorage:', error);
        }}
      }}

      createCompletionToggle(platform, subject, lectureTitle) {{
        const isCompleted = this.isCompleted(platform, subject, lectureTitle);

        const toggle = document.createElement('div');
        toggle.className = 'completion-toggle';
        toggle.innerHTML = `
          <div class="completion-circle ${{isCompleted ? 'completed' : ''}}">
            <i class="fas fa-check ${{isCompleted ? 'visible' : ''}}"></i>
          </div>
        `;

        toggle.onclick = (e) => {{
          e.stopPropagation();
          const newStatus = this.toggleCompletion(platform, subject, lectureTitle);
          const circle = toggle.querySelector('.completion-circle');
          const checkIcon = toggle.querySelector('.fas.fa-check');

          if (newStatus) {{
            circle.classList.add('completed');
            checkIcon.classList.add('visible');
          }} else {{
            circle.classList.remove('completed');
            checkIcon.classList.remove('visible');
          }}
        }};

        return toggle;
      }}
    }}

    const completionTracker = new LectureCompletion();

    async function loadLectures() {{
      console.log('Starting to load lectures...');
      try {{
        console.log('Fetching from {json_filename}...');
        const response = await fetch('{json_filename}');
        console.log('Fetch response status:', response.status, response.statusText);

        if (!response.ok) {{
          throw new Error(`HTTP error! status: ${{response.status}}`);
        }}

        const data = await response.json();
        console.log('JSON data received:', data);

        lecturesData = data.lectures || [];
        console.log('Loaded lectures from JSON:', lecturesData.length, 'lectures');

        if (lecturesData.length > 0) {{
          renderLectures(lecturesData);
        }} else {{
          console.log('No lectures found in JSON, using fallback');
          useFallbackLectures();
        }}
      }} catch (error) {{
        console.error('Error loading lectures:', error);
        console.log('Using fallback lectures due to error');
        useFallbackLectures();
      }}
    }}

    function useFallbackLectures() {{
      lecturesData = [
        {{ title: "Introduction", streamingUrl: "https://www.youtube.com/embed/dQw4w9WgXcQ" }},
        {{ title: "Basic Terminology", streamingUrl: "https://www.youtube.com/embed/dQw4w9WgXcQ" }}
      ];
      console.log('Using fallback lectures:', lecturesData.length, 'lectures');
      renderLectures(lecturesData);
    }}

    function escapeHtml(text) {{
      const map = {{
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;',
        '\\\\': '&#92;'
      }};
      return text.replace(/[&<>"'\\\\]/g, m => map[m]);
    }}

    function renderLectures(lectures) {{
      console.log('Rendering lectures:', lectures.length);
      const lectureList = document.getElementById('lectureList');

      if (!lectureList) {{
        console.error('lectureList element not found!');
        return;
      }}

      lectureList.innerHTML = '';

      if (lectures.length === 0) {{
        lectureList.innerHTML = '<p>No lectures available.</p>';
        return;
      }}

      lectures.forEach((lecture, index) => {{
        console.log(`Rendering lecture ${{index + 1}}:`, lecture.title);
        const lectureCard = document.createElement('div');
        lectureCard.className = 'lecture-card';

        const titleElement = document.createElement('h3');
        titleElement.textContent = lecture.title;

        const buttonContainer = document.createElement('div');
        buttonContainer.className = 'button-container';

        const openButton = document.createElement('button');
        openButton.className = 'open-button';
        openButton.innerHTML = '<i class="fas fa-play-circle"></i> Open';

        openButton.addEventListener('click', function() {{
          openStreamPlayer(
            lecture.streamingUrl || lecture.streamingUrlx || '',
            lecture.downloadUrl || lecture.downloadUrlx || lecture.streamingUrl || '',
            lecture.title
          );
        }});

        buttonContainer.appendChild(openButton);
        lectureCard.appendChild(titleElement);
        lectureCard.appendChild(buttonContainer);

        const completionToggle = completionTracker.createCompletionToggle('marrow', 'subject', lecture.title);
        lectureCard.appendChild(completionToggle);

        lectureList.appendChild(lectureCard);
      }});

      console.log('Finished rendering all lectures');
    }}

    function openVideo(title, url) {{
      const popup = document.createElement('div');
      popup.className = 'video-popup';
      popup.innerHTML = `
        <div class="video-popup-content">
          <button class="close-popup" onclick="closeVideo(this)">&times;</button>
          <h2 class="video-title">${{title}}</h2>
          <div class="iframe-container">
            <iframe src="${{url}}" allowfullscreen></iframe>
          </div>
        </div>
      `;

      popup.onclick = function(e) {{
        if (e.target === popup) {{
          closeVideo(popup.querySelector('.close-popup'));
        }}
      }};

      document.body.appendChild(popup);
    }}

    function closeVideo(button) {{
      const popup = button.closest('.video-popup');
      document.body.removeChild(popup);
    }}

    function downloadVideo(streamUrl) {{
      const downloadUrl = streamUrl.replace('/watch/', '/dl/');
      window.open(downloadUrl, '_blank');
    }}

    function openStreamPlayer(streamUrl, downloadUrl, title) {{
      if (!streamUrl) {{
        alert('Stream URL not available');
        return;
      }}

      const sanitizedTitle = (title || 'Lecture Video').trim();

      const params = new URLSearchParams({{
        stream: streamUrl,
        title: sanitizedTitle
      }});

      if (downloadUrl) {{
        params.append('download', downloadUrl);
      }}

      const currentPath = window.location.pathname;
      const pathParts = currentPath.split('/').filter(p => p && !p.includes('.html'));

      const isGitHubPages = window.location.hostname.includes('github.io');

      let streamPlayerUrl;
      if (isGitHubPages && pathParts.length > 0) {{
        const repoName = pathParts[0];
        streamPlayerUrl = `/${{repoName}}/stream-player.html?${{params.toString()}}`;
      }} else {{
        streamPlayerUrl = `/stream-player.html?${{params.toString()}}`;
      }}

      window.location.href = streamPlayerUrl;
    }}

    document.getElementById('searchInput').addEventListener('input', function(e) {{
      const query = e.target.value.toLowerCase();
      const filteredLectures = lecturesData.filter(lecture =>
        lecture.title.toLowerCase().includes(query)
      );
      renderLectures(filteredLectures);
    }});

    document.addEventListener('DOMContentLoaded', function() {{
      console.log('DOM loaded, starting lecture load...');
      loadLectures();
    }});
  </script>
  <script src="{prefix}stream-player-utils.js"></script>
  <script src="{prefix}theme.js"></script>
  <nav class="bottom-nav">
    <a href="{prefix}app.html" class="active"><i class="fas fa-lightbulb"></i><span>Home</span></a>
    <a href="{prefix}00x12345.html"><i class="fas fa-play-circle"></i><span>Videos</span></a>
    <a href="{prefix}searchx.html"><i class="fas fa-search"></i><span>Search</span></a>
    <a href="{prefix}quizx/index.html"><i class="fas fa-question-circle"></i><span>Q Bank</span></a>
  </nav>
</body>
</html>"""


async def upload_to_github(file_content: str, file_path: str, commit_message: str, token: str, branch: str = None):
    """Upload JSON file to GitHub repository.

    file_path: expected format "owner/repo/path/to/file.json"
    Returns (True, None) on success, (False, error_detail) on failure.
    """
    import base64
    import aiohttp

    try:
        if not token:
            return False, "GIT_TOKEN is empty or not set"

        # Normalize and split path
        normalized = file_path.strip().lstrip('/').rstrip('/')
        parts = normalized.split('/', 2)
        if len(parts) < 3:
            return False, f"Invalid path format: '{file_path}' — expected owner/repo/path/to/file.json"

        owner, repo, path = parts[0], parts[1], parts[2]
        api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"

        content_encoded = base64.b64encode(file_content.encode('utf-8')).decode('utf-8')

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.v3+json"
        }

        async with aiohttp.ClientSession() as session:
            params = {}
            if branch:
                params['ref'] = branch

            sha = None
            get_warning = None
            async with session.get(api_url, headers=headers, params=params) as resp_get:
                if resp_get.status == 200:
                    data = await resp_get.json()
                    sha = data.get('sha')
                elif resp_get.status == 404:
                    sha = None  # file doesn't exist yet, will create
                elif resp_get.status == 401:
                    return False, "GitHub token is invalid or expired (401 Unauthorized)"
                elif resp_get.status == 403:
                    get_text = await resp_get.text()
                    return False, f"GitHub access forbidden (403) — token may lack 'repo' scope. Detail: {get_text[:300]}"
                else:
                    get_text = await resp_get.text()
                    get_warning = f"GET {resp_get.status}: {get_text[:200]}"

            payload = {
                "message": commit_message or "Add file via bot",
                "content": content_encoded
            }
            if sha:
                payload["sha"] = sha
            if branch:
                payload["branch"] = branch

            async with session.put(api_url, headers=headers, json=payload) as resp_put:
                resp_text = await resp_put.text()
                if resp_put.status in (200, 201):
                    return True, None
                elif resp_put.status == 401:
                    return False, "GitHub token invalid/expired (401). Re-check GIT_TOKEN."
                elif resp_put.status == 403:
                    return False, f"GitHub 403 Forbidden — token likely missing 'repo' write scope.\nRepo: {owner}/{repo}\nDetail: {resp_text[:300]}"
                elif resp_put.status == 404:
                    return False, f"GitHub 404 — repo '{owner}/{repo}' not found or token has no access to it.\nURL: {api_url}"
                elif resp_put.status == 422:
                    return False, f"GitHub 422 Unprocessable — possibly wrong branch or SHA conflict.\nDetail: {resp_text[:300]}"
                else:
                    detail = f"HTTP {resp_put.status}\nURL: {api_url}\nResponse: {resp_text[:400]}"
                    if get_warning:
                        detail = f"GET warning: {get_warning}\n{detail}"
                    return False, detail

    except Exception as e:
        return False, f"Exception during upload: {type(e).__name__}: {e}"

@StreamBot.on_message(filters.private & filters.user(list(Var.ADMIN_IDS)) & filters.command('batch'))
async def batch_command(client: Client, message: Message):
    user_id = message.from_user.id
    # Clear any previous session and start fresh
    _batch_sessions[user_id] = {'state': 'waiting_folder', 'data': {}}
    await message.reply_text(
        "📁 Enter the destination folder path:\n\n"
        "Format: path/to/folder\n"
        "Example: 1234xxx/marrow/anatomy\n\n"
        "This is where JSON files will be uploaded."
    )


@StreamBot.on_message(
    filters.private & filters.user(list(Var.ADMIN_IDS)) & filters.text
    & ~filters.command(['batch', 'fbatch', 'fwd', 'start', 'gen', 'users', 'broadcast', 'ping', 'root', 'checkenv'])
)
async def batch_conversation_handler(client: Client, message: Message):
    user_id = message.from_user.id

    # ── /batch state machine ──────────────────────────────────────────────────
    batch_session = _batch_sessions.get(user_id)
    if batch_session:
        if batch_session['state'] == 'waiting_folder':
            folder_path = message.text.strip()
            github_dest_folder = f"{GITHUB_OWNER_REPO}/{folder_path}"
            _batch_sessions[user_id] = {
                'state': 'waiting_links',
                'data': {'github_dest_folder': github_dest_folder}
            }
            await message.reply_text(
                "📝 Send the links with subjects in this format:\n\n"
                "ANATOMY\n"
                "F - https://t.me/c/2024354927/237364\n"
                "L - https://t.me/c/2024354927/237366\n\n"
                "BIOCHEMISTRY\n"
                "F - https://t.me/c/2024354927/237460\n"
                "L - https://t.me/c/2024354927/237462\n\n"
                "Each subject should have F (first) and L (last) message links."
            )
        elif batch_session['state'] == 'waiting_links':
            github_dest_folder = batch_session['data']['github_dest_folder']
            del _batch_sessions[user_id]
            await _run_batch_processing(client, message, github_dest_folder)
        return

    # ── /fbatch state machine ─────────────────────────────────────────────────
    fbatch_session = _fbatch_sessions.get(user_id)
    if fbatch_session:
        if fbatch_session['state'] == 'waiting_range':
            del _fbatch_sessions[user_id]
            await _run_fbatch_scan(client, message)
        return


async def _run_batch_processing(client: Client, message: Message, github_dest_folder: str):
    """Run the actual batch processing after collecting both inputs."""
    try:
        links_text = message.text.strip()
        subjects_data = []
        current_subject = None
        current_first = None
        current_last = None

        for line in links_text.split('\n'):
            line = line.strip()
            if not line:
                continue
            if not line.startswith('F -') and not line.startswith('L -'):
                if current_subject and current_first and current_last:
                    subjects_data.append({
                        'subject': current_subject,
                        'first': current_first,
                        'last': current_last
                    })
                current_subject = line
                current_first = None
                current_last = None
            elif line.startswith('F -'):
                current_first = line.replace('F -', '').strip()
            elif line.startswith('L -'):
                current_last = line.replace('L -', '').strip()

        if current_subject and current_first and current_last:
            subjects_data.append({
                'subject': current_subject,
                'first': current_first,
                'last': current_last
            })

        if not subjects_data:
            await message.reply("❌ No valid subjects found in the input. Please check the format.")
            return

        git_token = os.environ.get('GIT_TOKEN', '')
        if not git_token:
            await message.reply(
                "❌ GIT_TOKEN not found in environment variables.\n\n"
                "Please add your GitHub Personal Access Token with repo permissions:\n"
                "1. Go to GitHub Settings → Developer settings → Personal access tokens\n"
                "2. Generate new token (classic) with 'repo' scope\n"
                "3. Add GIT_TOKEN to your environment variables"
            )
            return

        # status_msg is used ONLY for progress — never edited to show errors
        status_msg = await message.reply_text(
            f"🚀 Starting batch processing for {len(subjects_data)} subjects...\n"
            f"📁 Repo: sunday2212/webreadme4\n"
            f"📂 Folder: {github_dest_folder.split('/', 2)[2] if '/' in github_dest_folder else github_dest_folder}"
        )

        success_count = 0
        fail_count = 0

        for idx, subject_info in enumerate(subjects_data, 1):
            subject_name = subject_info['subject']
            json_output = []
            skipped_messages = []

            try:
                class MockMessage:
                    def __init__(self, text):
                        self.text = text
                        self.forward_from_chat = None
                        self.forward_sender_name = None

                f_msg_id = await get_message_id(client, MockMessage(subject_info['first']))
                s_msg_id = await get_message_id(client, MockMessage(subject_info['last']))

                if not f_msg_id or not s_msg_id:
                    fail_count += 1
                    # Send as NEW message so it is never overwritten
                    await message.reply_text(
                        f"❌ [{idx}/{len(subjects_data)}] {subject_name}\n"
                        f"Invalid message IDs — could not resolve links:\n"
                        f"F: {subject_info['first']}\n"
                        f"L: {subject_info['last']}"
                    )
                    await status_msg.edit_text(
                        f"⏳ Progress: {idx}/{len(subjects_data)} | ✅ {success_count} | ❌ {fail_count}"
                    )
                    continue

                start_id = min(f_msg_id, s_msg_id)
                end_id = max(f_msg_id, s_msg_id)
                total_messages = end_id - start_id + 1

                await status_msg.edit_text(
                    f"🔄 [{idx}/{len(subjects_data)}] Processing: {subject_name}\n"
                    f"Messages: {total_messages} | ✅ {success_count} | ❌ {fail_count}"
                )

                batch_size = 50
                processed_count = 0
                thumb_warning = None

                for batch_start in range(start_id, end_id + 1, batch_size):
                    batch_end = min(batch_start + batch_size - 1, end_id)
                    msg_ids = list(range(batch_start, batch_end + 1))

                    try:
                        messages = await get_messages(client, msg_ids)
                    except Exception:
                        messages = []
                        for msg_id in msg_ids:
                            try:
                                msg = (await get_messages(client, [msg_id]))[0]
                                messages.append(msg)
                            except:
                                messages.append(None)

                    for msg in messages:
                        processed_count += 1
                        if not msg:
                            skipped_messages.append({
                                "id": "Unknown",
                                "file_name": "Unknown",
                                "reason": "Message not found"
                            })
                            continue

                        thumbnail_folder = subject_name.lower().replace(" ", "_")
                        await process_message(msg, json_output, skipped_messages, thumbnail_folder, client)

                        if json_output:
                            last_entry = json_output[-1]
                            if not thumb_warning and '_thumb_error' in last_entry:
                                thumb_warning = last_entry.pop('_thumb_error')
                            elif '_thumb_error' in last_entry:
                                last_entry.pop('_thumb_error')

                clean_output = [{k: v for k, v in e.items() if k != '_thumb_error'} for e in json_output]

                output_data = {
                    "subjectName": subject_name.lower().replace(" ", ""),
                    "lectures": clean_output,
                    "skipped": skipped_messages
                }

                json_filename = f"{subject_name}.json"
                json_content = json.dumps(output_data, indent=4, ensure_ascii=False)
                github_file_path = f"{github_dest_folder}/{json_filename}".replace('//', '/')
                commit_msg_json = f"Add {json_filename} - {len(clean_output)} lectures"

                logging.info(f"Uploading JSON to GitHub: {github_file_path}")
                upload_success, upload_error = await upload_to_github(
                    json_content,
                    github_file_path,
                    commit_msg_json,
                    git_token
                )

                html_filename = f"{subject_name}.html"
                html_content = generate_lecture_html(json_filename, github_dest_folder)
                github_html_path = f"{github_dest_folder}/{html_filename}".replace('//', '/')
                logging.info(f"Uploading HTML to GitHub: {github_html_path}")
                html_upload_success, html_upload_error = await upload_to_github(
                    html_content,
                    github_html_path,
                    f"Add {html_filename}",
                    git_token
                )

                if upload_success:
                    success_count += 1
                    # Build success note with any warnings
                    notes = []
                    if thumb_warning:
                        notes.append(f"⚠️ Thumb: {thumb_warning[:150]}")
                    if not html_upload_success:
                        notes.append(f"⚠️ HTML upload failed: {(html_upload_error or '')[:150]}")
                    note_text = "\n" + "\n".join(notes) if notes else ""
                    await status_msg.edit_text(
                        f"✅ [{idx}/{len(subjects_data)}] {subject_name} done!\n"
                        f"Lectures: {len(clean_output)} | Skipped: {len(skipped_messages)}\n"
                        f"JSON: ✅  HTML: {'✅' if html_upload_success else '❌'}"
                        f"{note_text}\n\n"
                        f"Overall: ✅ {success_count} | ❌ {fail_count}"
                    )
                else:
                    fail_count += 1
                    error_detail = upload_error or "Unknown error"
                    logging.error(f"GitHub JSON upload failed for {subject_name}: {error_detail}")
                    # Send error as a NEW separate message — it will NOT be overwritten
                    await message.reply_text(
                        f"❌ [{idx}/{len(subjects_data)}] {subject_name} — JSON upload failed\n\n"
                        f"📁 Path: {github_file_path}\n\n"
                        f"🔍 Error:\n{error_detail}"
                    )
                    if not html_upload_success and html_upload_error:
                        logging.error(f"GitHub HTML upload also failed for {subject_name}: {html_upload_error}")
                        await message.reply_text(
                            f"❌ [{idx}/{len(subjects_data)}] {subject_name} — HTML upload also failed\n\n"
                            f"📁 Path: {github_html_path}\n\n"
                            f"🔍 Error:\n{html_upload_error}"
                        )
                    await status_msg.edit_text(
                        f"⏳ [{idx}/{len(subjects_data)}] {subject_name} failed — see error above\n\n"
                        f"Overall: ✅ {success_count} | ❌ {fail_count}"
                    )

                await asyncio.sleep(1)

            except Exception as e:
                fail_count += 1
                err_text = f"{type(e).__name__}: {e}"
                logging.error(f"Exception processing {subject_name}: {err_text}", exc_info=True)
                # Send as NEW message so it stays visible
                await message.reply_text(
                    f"❌ [{idx}/{len(subjects_data)}] {subject_name} — Exception\n\n{err_text}"
                )
                await status_msg.edit_text(
                    f"⏳ [{idx}/{len(subjects_data)}] {subject_name} failed — see error above\n\n"
                    f"Overall: ✅ {success_count} | ❌ {fail_count}"
                )
                continue

        # Final summary — always a new message so it appears after all error messages
        summary = (
            f"🏁 Batch complete!\n"
            f"Total: {len(subjects_data)} | ✅ Success: {success_count} | ❌ Failed: {fail_count}"
        )
        await message.reply_text(summary)
        await status_msg.edit_text(f"✅ Done — {success_count}/{len(subjects_data)} uploaded successfully.")

    except Exception as e:
        logging.error(f"Fatal error in _run_batch_processing: {e}", exc_info=True)
        await message.reply(f"❌ Fatal error: {type(e).__name__}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  /fbatch — Forum supergroup topic scanner
# ─────────────────────────────────────────────────────────────────────────────

def _raw_chat(chat_id: int) -> str:
    """Convert -1002932205861 → '2932205861' for t.me URL building."""
    s = str(abs(chat_id))
    return s[3:] if s.startswith("100") else s


def _supergroup_msg_url(chat_id: int, topic_id: int, msg_id: int) -> str:
    return f"https://t.me/c/{_raw_chat(chat_id)}/{topic_id}/{msg_id}"


def _get_topic_id_from_msg(msg) -> "int | None":
    """Extract forum topic ID from a Pyrogram/pyrofork Message.
    Tries all known attribute paths across builds."""
    if getattr(msg, "forum_topic_created", None) is not None:
        return msg.id
    for attr in ("reply_to_top_message_id", "message_thread_id"):
        tid = getattr(msg, attr, None)
        if tid:
            return int(tid)
    reply_to = getattr(msg, "reply_to", None)
    if reply_to:
        for attr in ("reply_to_top_id", "reply_to_msg_id"):
            tid = getattr(reply_to, attr, None)
            if tid:
                return int(tid)
    return None


def _parse_fbatch_range(raw: str):
    """Parse 'START_LINK-END_LINK' input.

    Supported link formats:
      2-part: https://t.me/c/CHAT_ID/MSG_ID
              → topic_id = msg_id (the number IS the topic creation msg ID)
      3-part: https://t.me/c/CHAT_ID/TOPIC_ID/MSG_ID
              → topic_id and msg_id are separate

    Separator: '-' immediately before 'https'.
    Returns (chat_id, start_topic, scan_start, end_topic, scan_end) or None.
    """
    raw = raw.strip()
    parts = re.split(r'-(?=https?://)', raw, maxsplit=1)
    if len(parts) != 2:
        return None
    start_link, end_link = parts[0].strip(), parts[1].strip()

    def _extract(link: str):
        """Return (chat_id, topic_id, msg_id) from a t.me/c link."""
        # 3-part: CHAT/TOPIC/MSG
        m = re.match(r"https?://t\.me/c/(\d+)/(\d+)/(\d+)", link)
        if m:
            return int(f"-100{m.group(1)}"), int(m.group(2)), int(m.group(3))
        # 2-part: CHAT/MSG — treat the single number as the topic ID too
        m = re.match(r"https?://t\.me/c/(\d+)/(\d+)", link)
        if m:
            n = int(m.group(2))
            return int(f"-100{m.group(1)}"), n, n
        return None, None, None

    s_chat, s_topic, s_msg = _extract(start_link)
    e_chat, e_topic, e_msg = _extract(end_link)
    if s_chat is None or e_chat is None:
        return None
    if s_chat != e_chat:
        return None
    return s_chat, s_topic, s_msg, e_topic, e_msg


async def _get_chat_latest_msg_id(client: Client, chat_id: int) -> int:
    """Return the latest message ID in a chat, or 0 on failure."""
    try:
        async for msg in client.get_chat_history(chat_id, limit=1):
            return msg.id
    except Exception:
        pass
    return 0


async def _scan_forum_topics(
    client: Client,
    chat_id: int,
    start_topic: int,
    scan_start: int,
    end_topic: int,
    scan_end: int,
    status_msg,
) -> "dict[int, dict]":
    """
    Two-phase forum topic scan.

    Phase 1 — narrow pass over [start_topic, end_topic]:
        Collect every forum_topic_created service message whose ID falls in that
        range.  These IDs ARE the topic IDs.

    Phase 2 — wide pass over [scan_start, wide_end]:
        Fetch all regular (non-service) messages and group them by their
        reply_to_top_message_id (= topic ID).  Only keeps topics discovered in
        Phase 1.  Tracks the first and last content message for each topic.

    Returns { topic_id: {'min': int, 'max': int, 'name': None} }
    """
    topics: dict = {}
    valid_topic_ids: set = set()

    # ─────────────────────────────────────────────────────────────────────────
    # IMPORTANT: get_messages(chat_id, [ids]) does NOT populate
    # forum_topic_created or reply_to_top_message_id in pyrofork when fetching
    # by specific ID list.  get_chat_history() returns all message types with
    # full attributes, so we use it for both phases.
    # ─────────────────────────────────────────────────────────────────────────

    # ── Phase 1: use get_chat_history to find topic-creation service messages ─
    try:
        await status_msg.edit_text(
            f"🔍 Phase 1/2 — discovering topics {start_topic} → {end_topic}…"
        )
    except Exception:
        pass

    phase1_end = max(end_topic, scan_end)
    phase1_limit = phase1_end - start_topic + 100

    try:
        async for msg in client.get_chat_history(
            chat_id, limit=phase1_limit, offset_id=phase1_end + 1
        ):
            if msg.id < start_topic:
                break
            # topic-creation service messages: their .id IS the topic ID
            is_topic_creation = (
                getattr(msg, "forum_topic_created", None) is not None
                or getattr(msg, "new_forum_topic", None) is not None
            )
            if is_topic_creation and start_topic <= msg.id <= end_topic:
                valid_topic_ids.add(msg.id)
                topics[msg.id] = {"min": None, "max": None, "name": None}
    except Exception as exc:
        logging.warning(f"fbatch phase1 get_chat_history error: {exc}")

    if not valid_topic_ids:
        logging.warning("fbatch: no topic-creation messages found in phase1; "
                        "phase2 will use reply_to range-filter as fallback")

    # ── Phase 2: scan wide range via get_chat_history, collect first/last msg ─
    latest_id = await _get_chat_latest_msg_id(client, chat_id)
    if latest_id <= 0:
        latest_id = scan_end + 3000

    wide_start = min(start_topic, scan_start)
    wide_end   = max(scan_end, latest_id)

    try:
        await status_msg.edit_text(
            f"🔍 Phase 2/2 — scanning {wide_start}→{wide_end} "
            f"for {len(valid_topic_ids) or '?'} topic(s)…"
        )
    except Exception:
        pass

    # Iterate newest→oldest in chunks via get_chat_history
    offset_id = wide_end + 1
    processed = 0
    total_est = max(1, wide_end - wide_start + 1)
    last_pct  = -10

    while offset_id > wide_start:
        batch = []
        try:
            async for msg in client.get_chat_history(
                chat_id, limit=_FBATCH_CHUNK, offset_id=offset_id
            ):
                batch.append(msg)
                if msg.id <= wide_start:
                    break
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 1)
            continue
        except Exception as exc:
            logging.warning(f"fbatch phase2 get_chat_history error: {exc}")
            break

        if not batch:
            break

        for msg in batch:
            if msg.id < wide_start:
                continue

            # Skip topic-creation service messages (no content)
            if (getattr(msg, "forum_topic_created", None) is not None
                    or getattr(msg, "new_forum_topic", None) is not None):
                continue

            # Skip other service/empty messages that carry no media or text
            has_content = bool(
                msg.text or msg.media or msg.document or msg.video
                or msg.audio or msg.photo or msg.voice
                or msg.video_note or msg.sticker or msg.animation
            )
            if not has_content:
                continue

            tid = _get_topic_id_from_msg(msg)
            if tid is None:
                continue

            # Filter: Phase-1 topics take priority
            if valid_topic_ids:
                if tid not in valid_topic_ids:
                    continue
            else:
                # Fallback: accept any topic whose ID falls in [start, end]
                if not (start_topic <= tid <= end_topic):
                    continue

            mid = msg.id
            if tid not in topics:
                topics[tid] = {"min": mid, "max": mid, "name": None}
            else:
                if mid < topics[tid]["min"]:
                    topics[tid]["min"] = mid
                if mid > topics[tid]["max"]:
                    topics[tid]["max"] = mid

        processed += len(batch)
        pct = min(99, processed * 100 // total_est)
        if pct >= last_pct + 10:
            last_pct = pct
            active = sum(1 for v in topics.values() if v["min"] is not None)
            try:
                await status_msg.edit_text(
                    f"🔍 Phase 2/2 — {pct}% (~{processed} msgs)\n"
                    f"Topics with content: {active}"
                )
            except Exception:
                pass

        # Move the window: next batch starts just below the oldest msg in this batch
        oldest_id = batch[-1].id
        if oldest_id <= wide_start:
            break
        offset_id = oldest_id
        await asyncio.sleep(_FBATCH_DELAY)

    # Drop any topic entries where no content message was found
    return {tid: info for tid, info in topics.items() if info["min"] is not None}


async def _fetch_topic_names(client: Client, chat_id: int, topics: dict) -> None:
    """Fetch topic title by reading the topic-header service message for each topic."""
    for tid in list(topics.keys()):
        try:
            msg = await client.get_messages(chat_id, tid)
            if msg and not getattr(msg, "empty", True):
                ftc = getattr(msg, "forum_topic_created", None)
                if ftc:
                    name = getattr(ftc, "name", None) or getattr(ftc, "title", None)
                    if name:
                        topics[tid]["name"] = name
        except Exception:
            pass
        await asyncio.sleep(0.2)


@StreamBot.on_message(filters.private & filters.user(list(Var.ADMIN_IDS)) & filters.command('fbatch'))
async def fbatch_command(client: Client, message: Message):
    user_id = message.from_user.id
    _fbatch_sessions[user_id] = {'state': 'waiting_range'}
    await message.reply_text(
        "📋 Forum Topic Scanner\n\n"
        "Send the **first topic link** and **last topic link** of the range:\n\n"
        "FORMAT:  FIRST_TOPIC_LINK-LAST_TOPIC_LINK\n\n"
        "2-part (topic creation link):\n"
        "`https://t.me/c/3950094573/5-https://t.me/c/3950094573/9`\n\n"
        "3-part (specific message in topic):\n"
        "`https://t.me/c/2932205861/116/117-https://t.me/c/2932205861/1040/1642`\n\n"
        "The bot will find all topics created between those two IDs, then scan "
        "the entire chat history to find the first and last message in each topic."
    )


async def _run_fbatch_scan(client: Client, message: Message):
    """Core logic: parse range → 2-phase scan → build topic map → send result."""
    raw = message.text.strip()

    parsed = _parse_fbatch_range(raw)
    if not parsed:
        await message.reply_text(
            "❌ Could not parse that link range.\n\n"
            "Accepted formats:\n"
            "• `https://t.me/c/CHATID/TOPIC1-https://t.me/c/CHATID/TOPIC2`\n"
            "• `https://t.me/c/CHATID/TOPIC1/MSG1-https://t.me/c/CHATID/TOPIC2/MSG2`"
        )
        return

    chat_id, start_topic, scan_start, end_topic, scan_end = parsed

    if end_topic < start_topic:
        await message.reply_text("❌ End topic ID must be ≥ start topic ID.")
        return

    status_msg = await message.reply_text(
        f"🔍 Forum Topic Scanner started\n\n"
        f"Chat   : -100{_raw_chat(chat_id)}\n"
        f"Topics : {start_topic} → {end_topic}\n"
        f"Msgs   : {scan_start} → {scan_end}\n\n"
        f"⏳ Phase 1: discovering topics…"
    )

    try:
        topics = await _scan_forum_topics(
            client, chat_id,
            start_topic, scan_start,
            end_topic, scan_end,
            status_msg,
        )
    except Exception as exc:
        logging.error(f"fbatch scan error: {exc}", exc_info=True)
        await status_msg.edit_text(f"❌ Scan failed: {type(exc).__name__}: {exc}")
        return

    if not topics:
        await status_msg.edit_text(
            f"⚠️ No forum topics found in range {start_topic} → {end_topic}.\n\n"
            "• Confirm the bot can read this supergroup.\n"
            "• Confirm Topics are enabled in the group.\n"
            "• All messages in range may be deleted."
        )
        return

    try:
        await status_msg.edit_text(
            f"✅ Found {len(topics)} topic(s) — fetching names…"
        )
        await _fetch_topic_names(client, chat_id, topics)
    except Exception as exc:
        logging.warning(f"fbatch name fetch error: {exc}")

    # ── Build output ──────────────────────────────────────────────────────────
    sorted_topics = sorted(topics.items(), key=lambda kv: kv[0])

    header_lines = [
        f"✅ {len(topics)} topics found in topic range {start_topic} → {end_topic}",
        f"{'─' * 50}",
        "",
    ]

    topic_blocks = []
    for tid, info in sorted_topics:
        name = info["name"] or f"Topic {tid}"
        f_link = _supergroup_msg_url(chat_id, tid, info["min"])
        l_link = _supergroup_msg_url(chat_id, tid, info["max"])
        topic_blocks.append(
            f"{name}\n"
            f"F - {f_link}\n"
            f"L - {l_link}"
        )

    full_text = "\n".join(header_lines) + "\n\n".join(topic_blocks)

    # ── Send as groups of topics (batch-style: name:- F_link-L_link) ─────────
    # Group messages so each fits in one Telegram message
    group_lines = [f"✅ {len(topics)} topics | range {start_topic}→{end_topic}\n"]
    for tid, info in sorted_topics:
        name = info["name"] or f"Topic {tid}"
        f_link = _supergroup_msg_url(chat_id, tid, info["min"])
        l_link = _supergroup_msg_url(chat_id, tid, info["max"])
        group_lines.append(f"{name}:- {f_link}-{l_link}")

    # Split into chunks that fit Telegram's 4096-char limit
    chunk_msgs = []
    current_chunk = []
    current_len = 0
    for line in group_lines:
        if current_len + len(line) + 1 > 4000 and current_chunk:
            chunk_msgs.append("\n".join(current_chunk))
            current_chunk = [line]
            current_len = len(line)
        else:
            current_chunk.append(line)
            current_len += len(line) + 1
    if current_chunk:
        chunk_msgs.append("\n".join(current_chunk))

    for chunk in chunk_msgs:
        try:
            await message.reply_text(chunk, disable_web_page_preview=True)
            await asyncio.sleep(0.5)
        except Exception as exc:
            logging.error(f"fbatch send chunk error: {exc}")

    # ── Also send as .txt file ────────────────────────────────────────────────
    import tempfile, os as _os
    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(suffix=".txt", prefix="fbatch_")
        _os.close(fd)
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(full_text)
        caption = f"✅ {len(topics)} topics | range {start_topic}→{end_topic}"
        await client.send_document(
            chat_id=message.chat.id,
            document=tmp_path,
            caption=caption,
            file_name=f"topics_{_raw_chat(chat_id)}_{start_topic}_{end_topic}.txt"
        )
    except Exception as exc:
        logging.error(f"fbatch send file error: {exc}")
    finally:
        if tmp_path and _os.path.exists(tmp_path):
            try:
                _os.remove(tmp_path)
            except Exception:
                pass

    try:
        await status_msg.delete()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────

def parse_tme_link(link: str):
    """Parse a t.me/c link — supports both channel and supergroup (topic) links.
    
    Channel:    https://t.me/c/CHAT_ID/MSG_ID          → (chat_id, msg_id)
    Supergroup: https://t.me/c/CHAT_ID/TOPIC_ID/MSG_ID → (chat_id, msg_id)

    Returns (chat_id_int, msg_id_int) or raises ValueError.
    """
    link = link.strip()
    # 3-part path first (supergroup with topic): CHAT/TOPIC/MSG
    m3 = re.match(r"https?://t\.me/c/(\d+)/(\d+)/(\d+)", link)
    if m3:
        chat_id = int(f"-100{m3.group(1)}")
        msg_id = int(m3.group(3))
        return chat_id, msg_id
    # 2-part path (regular channel): CHAT/MSG
    m2 = re.match(r"https?://t\.me/c/(\d+)/(\d+)", link)
    if m2:
        chat_id = int(f"-100{m2.group(1)}")
        msg_id = int(m2.group(2))
        return chat_id, msg_id
    raise ValueError(f"Cannot parse link: {link}")


def db_channel_short_id(db_channel: int) -> str:
    """Convert -1002024354927 → '2024354927' for use in t.me links."""
    s = str(db_channel)
    if s.startswith("-100"):
        return s[4:]
    return s.lstrip("-")


@StreamBot.on_message(filters.private & filters.user(list(Var.ADMIN_IDS)) & filters.command('fwd'))
async def fwd_command(client: Client, message: Message):
    """Forward messages from a source channel to the DB channel and return new F/L links."""
    try:
        links_msg = await client.ask(
            text="📝 Send subjects with F/L links from the *source* channel:\n\n"
                 "cbbanatomy\n"
                 "F - https://t.me/c/SOURCE_CHANNEL/237\n"
                 "L - https://t.me/c/SOURCE_CHANNEL/251\n\n"
                 "cbbpyt\n"
                 "F - https://t.me/c/SOURCE_CHANNEL/460\n"
                 "L - https://t.me/c/SOURCE_CHANNEL/469\n\n"
                 "Bot must be admin in the source channel.",
            chat_id=message.from_user.id,
            filters=filters.text,
            timeout=120
        )

        links_text = links_msg.text.strip()
        subjects_data = []
        current_subject = None
        current_first = None
        current_last = None

        for line in links_text.split('\n'):
            line = line.strip()
            if not line:
                continue
            if not line.startswith('F -') and not line.startswith('L -'):
                if current_subject and current_first and current_last:
                    subjects_data.append({
                        'subject': current_subject,
                        'first': current_first,
                        'last': current_last
                    })
                current_subject = line
                current_first = None
                current_last = None
            elif line.startswith('F -'):
                current_first = line.replace('F -', '').strip()
            elif line.startswith('L -'):
                current_last = line.replace('L -', '').strip()

        if current_subject and current_first and current_last:
            subjects_data.append({
                'subject': current_subject,
                'first': current_first,
                'last': current_last
            })

        if not subjects_data:
            await message.reply("❌ No valid subjects found. Check the format and try again.")
            return

        status_msg = await message.reply_text(
            f"🚀 Starting forward of {len(subjects_data)} subject(s) to DB channel..."
        )

        db_short = db_channel_short_id(Var.DB_CHANNEL)
        result_lines = []

        for idx, subject_info in enumerate(subjects_data, 1):
            subject_name = subject_info['subject']
            try:
                src_chat_id, first_msg_id = parse_tme_link(subject_info['first'])
                _, last_msg_id = parse_tme_link(subject_info['last'])

                start_id = min(first_msg_id, last_msg_id)
                end_id = max(first_msg_id, last_msg_id)
                total = end_id - start_id + 1

                await status_msg.edit_text(
                    f"📤 Forwarding {subject_name}...\n"
                    f"Subject {idx}/{len(subjects_data)} | Messages: {total}"
                )

                fwd_first_id = None
                fwd_last_id = None
                forwarded_count = 0
                failed_count = 0
                skipped_count = 0   # service/empty messages — not a real failure
                first_error = None

                # Forward one message at a time — batch forwarding fails entirely
                # if any single message in the batch is missing/deleted.
                for msg_id in range(start_id, end_id + 1):
                    try:
                        # ── Pre-fetch to detect and skip service/empty messages ──
                        src_msg = await client.get_messages(src_chat_id, msg_id)

                        if src_msg is None or getattr(src_msg, 'empty', True):
                            skipped_count += 1
                            await asyncio.sleep(0.2)
                            continue

                        # Service messages (topic creation, pinned, etc.) can't be
                        # copied — skip silently so they don't pollute failed_count
                        has_content = bool(
                            src_msg.text or src_msg.media or src_msg.document
                            or src_msg.video or src_msg.audio or src_msg.photo
                            or src_msg.voice or src_msg.video_note
                            or src_msg.sticker or src_msg.animation
                        )
                        if not has_content:
                            skipped_count += 1
                            await asyncio.sleep(0.2)
                            continue

                        # ── Copy the message to DB channel ──────────────────────
                        copied = await client.copy_message(
                            chat_id=Var.DB_CHANNEL,
                            from_chat_id=src_chat_id,
                            message_id=msg_id
                        )

                        if copied and getattr(copied, 'id', None):
                            if fwd_first_id is None or copied.id < fwd_first_id:
                                fwd_first_id = copied.id
                            if fwd_last_id is None or copied.id > fwd_last_id:
                                fwd_last_id = copied.id
                            forwarded_count += 1
                        else:
                            # copy_message returned but no valid ID — treat as failure
                            err_str = f"copy_message returned no ID for msg {msg_id}"
                            logging.warning(err_str)
                            if first_error is None:
                                first_error = err_str
                            failed_count += 1

                        await asyncio.sleep(0.3)

                    except FloodWait as e:
                        await asyncio.sleep(e.value + 2)
                        try:
                            copied = await client.copy_message(
                                chat_id=Var.DB_CHANNEL,
                                from_chat_id=src_chat_id,
                                message_id=msg_id
                            )
                            if copied and getattr(copied, 'id', None):
                                if fwd_first_id is None or copied.id < fwd_first_id:
                                    fwd_first_id = copied.id
                                if fwd_last_id is None or copied.id > fwd_last_id:
                                    fwd_last_id = copied.id
                                forwarded_count += 1
                        except Exception as retry_err:
                            err_str = f"{type(retry_err).__name__}: {retry_err}"
                            logging.error(f"Retry failed msg {msg_id} in {subject_name}: {err_str}")
                            if first_error is None:
                                first_error = err_str
                            failed_count += 1
                    except Exception as msg_err:
                        err_str = f"{type(msg_err).__name__}: {msg_err}"
                        logging.error(f"Copy error msg {msg_id} in {subject_name}: {err_str}")
                        if first_error is None:
                            first_error = err_str
                        failed_count += 1

                    # Update status every 20 messages
                    if (msg_id - start_id + 1) % 20 == 0:
                        await status_msg.edit_text(
                            f"📤 {subject_name}: {forwarded_count} copied, "
                            f"{skipped_count} skipped, {failed_count} failed\n"
                            f"Progress: {msg_id - start_id + 1}/{total} | "
                            f"Subject {idx}/{len(subjects_data)}"
                        )

                if fwd_first_id and fwd_last_id:
                    f_link = f"https://t.me/c/{db_short}/{fwd_first_id}"
                    l_link = f"https://t.me/c/{db_short}/{fwd_last_id}"
                    result_lines.append(
                        f"{subject_name}\n"
                        f"F - {f_link}\n"
                        f"L - {l_link}"
                    )
                    await status_msg.edit_text(
                        f"✅ {subject_name} done!\n"
                        f"Copied: {forwarded_count} | Skipped: {skipped_count} | Failed: {failed_count}\n"
                        f"F - {f_link}\n"
                        f"L - {l_link}\n\n"
                        f"Progress: {idx}/{len(subjects_data)}"
                    )
                else:
                    if first_error:
                        error_hint = f"\n🔍 Error: {first_error}"
                    elif skipped_count == total:
                        error_hint = (
                            f"\n🔍 All {skipped_count} messages were service/empty messages "
                            f"(topic creation, pinned notices, etc.) — no media to forward.\n"
                            f"Use the correct F/L links pointing to actual media messages."
                        )
                    else:
                        error_hint = (
                            f"\n🔍 {skipped_count} skipped (service msgs), "
                            f"{failed_count} failed — check bot permissions in source chat."
                        )
                    result_lines.append(f"{subject_name}\n❌ No messages forwarded{error_hint}")
                    await status_msg.edit_text(
                        f"❌ {subject_name}: nothing forwarded\n"
                        f"Attempted: {total} | Skipped: {skipped_count} | Failed: {failed_count}"
                        f"{error_hint}\n\n"
                        f"Progress: {idx}/{len(subjects_data)}"
                    )

                await asyncio.sleep(1)

            except Exception as e:
                logging.error(f"Error forwarding {subject_name}: {e}", exc_info=True)
                result_lines.append(f"{subject_name}\n❌ Error: {str(e)}")
                await status_msg.edit_text(
                    f"❌ Error on {subject_name}: {str(e)}\n\n"
                    f"Progress: {idx}/{len(subjects_data)}"
                )

        # Send final summary with all new F/L links
        final_text = "✅ Forward complete! New DB channel links:\n\n" + "\n\n".join(result_lines)
        await message.reply_text(final_text, disable_web_page_preview=True)

    except asyncio.TimeoutError:
        await message.reply("⏱️ Request timeout. Please try again.")
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}")


@StreamBot.on_message((filters.private) & (filters.document | filters.audio | filters.photo), group=3)
async def private_receive_handler(c: Client, m: Message):
    try:
        # Create intermediate link instead of immediate stream generation
        intermediate_link, caption = await create_intermediate_link(m)
        
        # Create button with intermediate link
        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("GENERATE STREAM 🎬", url=intermediate_link)]])
        
        # Send response with intermediate link
        response_text = f"📁 <b>{caption}</b>\n\n🔗 Click the button below to generate your stream link:"
        await m.reply_text(text=response_text, reply_markup=reply_markup, disable_web_page_preview=True, quote=True)
        
    except Exception as e:
        await m.reply_text(f"❌ Error processing file: {str(e)}", quote=True)
        print(f"Error in private_receive_handler: {e}")

@StreamBot.on_message((filters.private) & (filters.video | filters.audio | filters.photo), group=4)
async def private_receive_handler_video(c: Client, m: Message):
    try:
        # Create intermediate link instead of immediate stream generation
        intermediate_link, caption = await create_intermediate_link(m)
        
        # Create button with intermediate link
        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("GENERATE STREAM 🎬", url=intermediate_link)]])
        
        # Send response with intermediate link
        response_text = f"🎥 <b>{caption}</b>\n\n🔗 Click the button below to generate your stream link:"
        await m.reply_text(text=response_text, reply_markup=reply_markup, disable_web_page_preview=True, quote=True)
        
    except Exception as e:
        await m.reply_text(f"❌ Error processing file: {str(e)}", quote=True)
        print(f"Error in private_receive_handler_video: {e}")
