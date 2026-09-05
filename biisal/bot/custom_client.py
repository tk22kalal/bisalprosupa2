from pyrogram import Client
from pyrogram.errors import FloodWait
import asyncio


class CustomClient(Client):
    def __init__(self, db_channel, *args, **kwargs):
        self.db_channel = db_channel
        super().__init__(*args, **kwargs)

    async def forward_message_to_all_users(self, message):
        for member in await self.get_chat_members(self.db_channel):
            try:
                await message.forward(member.user.id)
                await asyncio.sleep(0.5)
            except FloodWait as e:
                print(f"Sleeping for {str(e.value)}s")
                await asyncio.sleep(e.value)
            except Exception as e:
                print(f"Error forwarding message to user {member.user.id}: {e}")
