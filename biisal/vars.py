import os
import random
from os import getenv, environ
from dotenv import load_dotenv

load_dotenv()


class Var(object):
    MULTI_CLIENT = False
    API_ID = int(getenv('API_ID', ''))
    API_HASH = str(getenv('API_HASH', ''))
    BOT_TOKEN = str(getenv('BOT_TOKEN', ''))
    GIT_TOKEN = str(getenv('GIT_TOKEN', ''))
    THUMB_API = str(getenv('THUMB_API', ''))
    name = str(getenv('name', 'Nobita-Stream-Bot'))
    SLEEP_THRESHOLD = int(getenv('SLEEP_THRESHOLD', '60'))
    WORKERS = int(getenv('WORKERS', '4'))
    BIN_CHANNEL = int(getenv('BIN_CHANNEL', ''))
    DB_CHANNEL = int(getenv('DB_CHANNEL', ''))
    PORT = int(getenv('PORT', '8080'))
    BIND_ADRESS = str(getenv('WEB_SERVER_BIND_ADDRESS', '0.0.0.0'))
    PING_INTERVAL = int(environ.get("PING_INTERVAL", "1200"))
    OWNER_ID = set(int(x) for x in os.environ.get("OWNER_ID", "").split() if x.isdigit())
    ADMIN_IDS = OWNER_ID | set(int(x) for x in os.environ.get("ADMIN_IDS", "").split() if x.isdigit())
    NO_PORT = bool(getenv('NO_PORT', False))
    APP_NAME = None
    OWNER_USERNAME = str(getenv('OWNER_USERNAME', 'NobiDeveloperr'))
    PROTECT_CONTENT = os.environ.get('PROTECT_CONTENT', "True") == "True"
    CUSTOM_CAPTION = os.environ.get("CUSTOM_CAPTION", None)
    DISABLE_CHANNEL_BUTTON = os.environ.get("DISABLE_CHANNEL_BUTTON", None) == 'True'

    if 'DYNO' in environ:
        ON_HEROKU = True
        APP_NAME = str(getenv('APP_NAME'))
    else:
        ON_HEROKU = False

    DUAL_DOMAIN_WEB = str(getenv('DUAL_DOMAIN_WEB', 'web.afrahtafreeh.site'))
    DUAL_DOMAIN_WEBX = str(getenv('DUAL_DOMAIN_WEBX', 'webx.afrahtafreeh.site'))
    DUAL_DOMAIN_ENABLED = os.environ.get('DUAL_DOMAIN_ENABLED', 'True') == 'True'
    SERVE_DOMAIN = str(getenv('SERVE_DOMAIN', '')).lower().strip()
    HAS_SSL = bool(getenv('HAS_SSL', True))

    _base_fqdn = str(getenv('FQDN', BIND_ADRESS)) if not ('DYNO' in environ) or getenv('FQDN') else (getenv('APP_NAME', '') + '.herokuapp.com' if getenv('APP_NAME') else BIND_ADRESS)

    if SERVE_DOMAIN == 'web':
        FQDN = DUAL_DOMAIN_WEB
    elif SERVE_DOMAIN == 'webx':
        FQDN = DUAL_DOMAIN_WEBX
    else:
        FQDN = _base_fqdn

    if HAS_SSL:
        URL = "https://{}/".format(FQDN)
        URL_WEB = "https://{}/".format(DUAL_DOMAIN_WEB)
        URL_WEBX = "https://{}/".format(DUAL_DOMAIN_WEBX)
    else:
        URL = "http://{}/".format(FQDN)
        URL_WEB = "http://{}/".format(DUAL_DOMAIN_WEB)
        URL_WEBX = "http://{}/".format(DUAL_DOMAIN_WEBX)

    DATABASE_URL = str(getenv('DATABASE_URL', ''))
    RECAPTCHA_SITE_KEY = str(getenv('RECAPTCHA_SITE_KEY', '6LdCK_crAAAAAD702QCUelFDiZPr5wqL-3qbgk2u'))
    RECAPTCHA_SECRET_KEY = str(getenv('RECAPTCHA_SECRET_KEY', '6LdCK_crAAAAAMiFPR9Pk5u3Zvnj6G8rNEORAsEV'))

    @classmethod
    def get_fqdn(cls):
        if cls.SERVE_DOMAIN == 'web':
            return cls.DUAL_DOMAIN_WEB
        elif cls.SERVE_DOMAIN == 'webx':
            return cls.DUAL_DOMAIN_WEBX
        else:
            return cls.FQDN

    @classmethod
    def get_base_url(cls):
        if cls.SERVE_DOMAIN == 'web':
            return cls.URL_WEB
        elif cls.SERVE_DOMAIN == 'webx':
            return cls.URL_WEBX
        else:
            return cls.URL

    @classmethod
    def get_current_domain(cls):
        if cls.SERVE_DOMAIN in ('web', 'webx'):
            return cls.SERVE_DOMAIN
        return None

    @classmethod
    def get_url_for_file(cls, file_id: str) -> str:
        protocol = "https" if cls.HAS_SSL else "http"
        return f"{protocol}://{cls.get_fqdn()}/"

    @classmethod
    def get_dual_urls(cls):
        return {
            'web': cls.URL_WEB,
            'webx': cls.URL_WEBX
        }

    @classmethod
    def reset_batch(cls):
        pass


Var = Var()
