import datetime
import motor.motor_asyncio
import secrets
import time


class Database:
    def __init__(self, uri, database_name):
        self.enabled = bool(uri and uri.strip() and (uri.startswith('mongodb://') or uri.startswith('mongodb+srv://')))
        if self.enabled:
            try:
                self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
                self.db = self._client[database_name]
                self.col = self.db.users
                self.bannedList = self.db.bannedList
                self.temp_files = self.db.temp_files
            except Exception as e:
                print(f"Database initialization error: {e}")
                self.enabled = False
                self._client = None
                self.db = None
                self.col = None
                self.bannedList = None
                self.temp_files = None
                self._memory_temp_files = {}
        else:
            self._client = None
            self.db = None
            self.col = None
            self.bannedList = None
            self.temp_files = None
            self._memory_temp_files = {}
            print("Database disabled - no DATABASE_URL provided, using in-memory storage")

    def new_user(self, id):
        return dict(
            id=id,
            join_date=datetime.date.today().isoformat()
        )

    async def add_user(self, id):
        if not self.enabled:
            return
        user = self.new_user(id)
        await self.col.insert_one(user)

    async def add_user_pass(self, id, ag_pass):
        if not self.enabled:
            return
        await self.add_user(int(id))
        await self.col.update_one({'id': int(id)}, {'$set': {'ag_p': ag_pass}})

    async def get_user_pass(self, id):
        if not self.enabled:
            return None
        user_pass = await self.col.find_one({'id': int(id)})
        return user_pass.get("ag_p", None) if user_pass else None

    async def is_user_exist(self, id):
        if not self.enabled:
            return False
        user = await self.col.find_one({'id': int(id)})
        return True if user else False

    async def total_users_count(self):
        if not self.enabled:
            return 0
        count = await self.col.count_documents({})
        return count

    async def get_all_users(self):
        if not self.enabled:
            return []
        all_users = self.col.find({})
        return all_users

    async def delete_user(self, user_id):
        if not self.enabled:
            return
        await self.col.delete_many({'id': int(user_id)})

    async def ban_user(self, user_id):
        if not self.enabled:
            return False
        user = await self.bannedList.find_one({'banId': int(user_id)})
        if user:
            return False
        else:
            await self.bannedList.insert_one({'banId': int(user_id)})
            return True

    async def is_banned(self, user_id):
        if not self.enabled:
            return False
        user = await self.bannedList.find_one({'banId': int(user_id)})
        return True if user else False

    async def is_unbanned(self, user_id):
        if not self.enabled:
            return False
        try:
            if await self.bannedList.find_one({'banId': int(user_id)}):
                await self.bannedList.delete_one({'banId': int(user_id)})
                return True
            else:
                return False
        except Exception as e:
            e = f'Failed to unban. Reason: {e}'
            print(e)
            return e

    async def store_temp_file(self, message_data, domain=None):
        token = secrets.token_urlsafe(16)

        if not self.enabled:
            temp_data = {
                'token': token,
                'domain': domain,
                'message_id': message_data['message_id'],
                'file_name': message_data['file_name'],
                'file_size': message_data['file_size'],
                'mime_type': message_data['mime_type'],
                'caption': message_data['caption'],
                'from_chat_id': message_data['from_chat_id'],
                'file_unique_id': message_data['file_unique_id'],
                'thumbnail_url': message_data.get('thumbnail_url'),
                'created_at': time.time()
            }
            self._memory_temp_files[token] = temp_data
            return token

        temp_data = {
            'token': token,
            'domain': domain,
            'message_id': message_data['message_id'],
            'file_name': message_data['file_name'],
            'file_size': message_data['file_size'],
            'mime_type': message_data['mime_type'],
            'caption': message_data['caption'],
            'from_chat_id': message_data['from_chat_id'],
            'file_unique_id': message_data['file_unique_id'],
            'thumbnail_url': message_data.get('thumbnail_url'),
            'created_at': time.time()
        }
        await self.temp_files.insert_one(temp_data)
        return token

    async def get_temp_file(self, token, serve_domain=None):
        if not self.enabled:
            data = self._memory_temp_files.get(token)
            if data and serve_domain and data.get('domain') and data['domain'] != serve_domain:
                return None
            return data

        query = {'token': token}
        if serve_domain:
            query['domain'] = serve_domain
        temp_data = await self.temp_files.find_one(query)
        return temp_data

    async def delete_temp_file(self, token):
        if not self.enabled:
            if token in self._memory_temp_files:
                del self._memory_temp_files[token]
            return
        await self.temp_files.delete_one({'token': token})

    async def cleanup_expired_temp_files(self):
        return
