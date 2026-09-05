# (c) @adarsh-goel
# (c) @biisal
import os
import time
import string
import random
import asyncio
import aiofiles
import datetime
from biisal.utils.broadcast_helper import send_msg
from biisal.utils.database import Database
from biisal.bot import StreamBot
from biisal.vars import Var
from pyrogram import filters, Client
from pyrogram.types import Message

db = Database(Var.DATABASE_URL, Var.name)
Broadcast_IDs = {}


@StreamBot.on_message(filters.command("users") & filters.private & filters.user(list(Var.ADMIN_IDS)))
async def sts(c: Client, m: Message):
    total_users = await db.total_users_count()
    await m.reply_text(text=f"Total Users in DB: {total_users}", quote=True)


@StreamBot.on_message(filters.command("broadcast") & filters.private & filters.user(list(Var.OWNER_ID)))
async def broadcast_(c, m):
    out = await m.reply_text(
        text="Broadcast initiated! You will be notified with log file when all users are notified."
    )
    all_users = await db.get_all_users()
    broadcast_msg = m.reply_to_message
    while True:
        broadcast_id = ''.join([random.choice(string.ascii_letters) for i in range(3)])
        if not Broadcast_IDs.get(broadcast_id):
            break
    start_time = time.time()
    total_users = await db.total_users_count()
    done = 0
    failed = 0
    success = 0
    Broadcast_IDs[broadcast_id] = dict(
        total=total_users,
        current=done,
        failed=failed,
        success=success
    )
    async with aiofiles.open('broadcast.txt', 'w') as broadcast_log_file:
        async for user in all_users:
            sts_code, msg = await send_msg(
                user_id=int(user['id']),
                message=broadcast_msg
            )
            if msg is not None:
                await broadcast_log_file.write(msg)
            if sts_code == 200:
                success += 1
            else:
                failed += 1
            if sts_code == 400:
                await db.delete_user(user['id'])
            done += 1
            if Broadcast_IDs.get(broadcast_id) is None:
                break
            else:
                Broadcast_IDs[broadcast_id].update(
                    dict(
                        current=done,
                        failed=failed,
                        success=success
                    )
                )
    if Broadcast_IDs.get(broadcast_id):
        Broadcast_IDs.pop(broadcast_id)
    completed_in = datetime.timedelta(seconds=int(time.time() - start_time))
    await asyncio.sleep(3)
    await out.delete()
    if failed == 0:
        await m.reply_text(
            text=f"Broadcast completed in `{completed_in}`\n\nTotal users {total_users}.\nDone {done}, {success} success, {failed} failed.",
            quote=True
        )
    else:
        await m.reply_document(
            document='broadcast.txt',
            caption=f"Broadcast completed in `{completed_in}`\n\nTotal users {total_users}.\nDone {done}, {success} success, {failed} failed.",
            quote=True
        )
    os.remove('broadcast.txt')


@StreamBot.on_message(filters.command("ping") & filters.private)
async def ping(c: Client, m: Message):
    start = time.time()
    reply = await m.reply_text("Pinging...")
    end = time.time()
    await reply.edit_text(f"Pong! `{round((end - start) * 1000, 3)}ms`")


@StreamBot.on_message(filters.command("checkenv") & filters.private & filters.user(list(Var.ADMIN_IDS)))
async def check_env(c: Client, m: Message):
    env_vars = {
        "API_ID": bool(Var.API_ID),
        "API_HASH": bool(Var.API_HASH),
        "BOT_TOKEN": bool(Var.BOT_TOKEN),
        "BIN_CHANNEL": bool(Var.BIN_CHANNEL),
        "DB_CHANNEL": bool(Var.DB_CHANNEL),
        "OWNER_ID": bool(Var.OWNER_ID),
        "ADMIN_IDS": bool(Var.ADMIN_IDS),
        "GIT_TOKEN": bool(os.environ.get('GIT_TOKEN', '')),
        "DATABASE_URL": bool(Var.DATABASE_URL),
        "DUAL_DOMAIN_WEB": bool(Var.DUAL_DOMAIN_WEB),
        "DUAL_DOMAIN_WEBX": bool(Var.DUAL_DOMAIN_WEBX),
        "SERVE_DOMAIN": Var.SERVE_DOMAIN or "not set",
        "FQDN": Var.FQDN,
        "HAS_SSL": Var.HAS_SSL,
        "MULTI_CLIENT": Var.MULTI_CLIENT,
    }

    lines = ["**Environment Check:**\n"]
    for key, val in env_vars.items():
        if isinstance(val, bool):
            icon = "✅" if val else "❌"
            lines.append(f"{icon} `{key}`")
        else:
            lines.append(f"ℹ️ `{key}`: `{val}`")

    await m.reply_text("\n".join(lines))
