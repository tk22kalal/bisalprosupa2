from ..vars import Var
from os import getcwd
from .custom_client import CustomClient

StreamBot = CustomClient(
    db_channel=Var.DB_CHANNEL,
    name='Web Streamer',
    api_id=Var.API_ID,
    api_hash=Var.API_HASH,
    bot_token=Var.BOT_TOKEN,
    sleep_threshold=Var.SLEEP_THRESHOLD,
    workers=Var.WORKERS
)

multi_clients = {}
work_loads = {}
