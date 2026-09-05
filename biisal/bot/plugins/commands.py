# (c) @biisal @adarsh

from biisal.bot import StreamBot
from biisal.vars import Var
import logging
logger = logging.getLogger(__name__)
from biisal.utils.human_readable import humanbytes
from biisal.utils.database import Database
from pyrogram import filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from biisal.utils.file_properties import get_name, get_hash, get_media_file_size

db = Database(Var.DATABASE_URL, Var.name)


@StreamBot.on_message(filters.command('ban') & filters.user(list(Var.ADMIN_IDS)))
async def do_ban(bot, message):
    userid = message.text.split(" ", 2)[1] if len(message.text.split(" ", 1)) > 1 else None
    reason = message.text.split(" ", 2)[2] if len(message.text.split(" ", 2)) > 2 else None
    if not userid:
        return await message.reply('<b>Please add a valid user/channel ID with this command\n\nEx: /ban (user/channel_id) (reason[optional])\nEx: <code>/ban 1234567899</code></b>')
    text = await message.reply("<b>Checking...</b>")
    banSts = await db.ban_user(userid)
    if banSts == True:
        await text.edit(
            text=f"<b><code>{userid}</code> has been banned successfully\n\nShould I send an alert to the banned user?</b>",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("Yes ✅", callback_data=f"sendAlert_{userid}_{reason if reason else 'no reason provided'}"),
                        InlineKeyboardButton("No ❌", callback_data=f"noAlert_{userid}"),
                    ],
                ]
            ),
        )
    else:
        await text.edit(f"<b><code>{userid}</code> is already banned!</b>")
    return


@StreamBot.on_message(filters.command('unban') & filters.user(list(Var.ADMIN_IDS)))
async def do_unban(bot, message):
    userid = message.text.split(" ", 2)[1] if len(message.text.split(" ", 1)) > 1 else None
    if not userid:
        return await message.reply('Give me an ID\nEx: <code>/unban 1234567899</code>')
    text = await message.reply("<b>Checking...</b>")
    unban_chk = await db.is_unbanned(userid)
    if unban_chk == True:
        await text.edit(
            text=f'<b><code>{userid}</code> is unbanned\nShould I send an alert to the unbanned user?</b>',
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("Yes ✅", callback_data=f"sendUnbanAlert_{userid}"),
                        InlineKeyboardButton("No ❌", callback_data=f"NoUnbanAlert_{userid}"),
                    ],
                ]
            ),
        )
    elif unban_chk == False:
        await text.edit('<b>User is not banned yet.</b>')
    else:
        await text.edit(f"<b>Failed to unban user/channel.\nReason: {unban_chk}</b>")


@StreamBot.on_callback_query()
async def cb_handler(client, query):
    data = query.data

    if data == "close_data":
        await query.message.delete()

    elif data.startswith("sendAlert"):
        user_id = (data.split("_")[1])
        user_id = int(user_id.replace(' ', ''))
        if len(str(user_id)) == 10:
            reason = str(data.split("_")[2])
            try:
                await client.send_message(user_id, f'<b>You are banned by admin.\nReason: {reason}</b>')
                await query.message.edit(f"<b>Alert sent to <code>{user_id}</code>\nReason: {reason}</b>")
            except Exception as e:
                await query.message.edit(f"<b>Error: {e}</b>")
        else:
            await query.message.edit(f"<b>Process not completed - invalid user ID (may be a channel ID)</b>")

    elif data.startswith('noAlert'):
        user_id = (data.split("_")[1])
        user_id = int(user_id.replace(' ', ''))
        await query.message.edit(f"<b>Ban on <code>{user_id}</code> was executed silently.</b>")

    elif data.startswith('sendUnbanAlert'):
        user_id = (data.split("_")[1])
        user_id = int(user_id.replace(' ', ''))
        if len(str(user_id)) == 10:
            try:
                unban_text = '<b>You are unbanned by admin.</b>'
                await client.send_message(user_id, unban_text)
                await query.message.edit(f"<b>Unbanned alert sent to <code>{user_id}</code></b>")
            except Exception as e:
                await query.message.edit(f"<b>Error: {e}</b>")
        else:
            await query.message.edit(f"<b>Process not completed - invalid user ID</b>")

    elif data.startswith('NoUnbanAlert'):
        user_id = (data.split("_")[1])
        user_id = int(user_id.replace(' ', ''))
        await query.message.edit(f"Unban on <code>{user_id}</code> was executed silently.")


@StreamBot.on_message(filters.private & filters.user(list(Var.ADMIN_IDS)) & filters.command('root'))
async def root_command(bot, message):
    """Send the GitHub file index link to the requesting admin."""
    base_url = Var.URL.rstrip('/')
    tree_url = f"{base_url}/root-tree"
    await message.reply_text(
        "👋 Hi Admin!\n\n"
        "📁 Tap the button below to browse the file index.\n"
        "Folders are expandable — click any folder to open it.\n"
        "Files are listed without the .html extension.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📂 Open File Index", web_app=WebAppInfo(url=tree_url))
        ]]),
        quote=True
    )
