# database.py (UPDATED FOR DUPLICATE CHECKING)

import motor.motor_asyncio
from config import Config

class Database:
    def __init__(self):
        self._client = None
        self.db = None
        self.collection = None
        if not Config.DATABASE_URL:
            print("WARNING: DATABASE_URL not set. Links will not be permanent.")

    async def connect(self):
        """Database se connection banata hai."""
        if Config.DATABASE_URL:
            print("Connecting to the database...")
            self._client = motor.motor_asyncio.AsyncIOMotorClient(Config.DATABASE_URL)
            self.db = self._client["StreamLinksDB"]
            self.collection = self.db["links"]
            print("✅ Database connection established.")
        else:
            self.db = None
            self.collection = None

    async def disconnect(self):
        """Database connection ko band karta hai."""
        if self._client:
            self._client.close()
            print("Database connection closed.")

    # unique_id (link id), message_id (storage id), and file_unique_id (telegram file id)
    async def save_link(self, unique_id, message_id, file_unique_id):
        if self.collection is not None:
            await self.collection.insert_one({
                '_id': unique_id, 
                'message_id': message_id,
                'file_unique_id': file_unique_id  # Yeh duplicate check ke liye zaroori hai
            })

    async def get_link(self, unique_id):
        if self.collection is not None:
            doc = await self.collection.find_one({'_id': unique_id})
            return doc.get('message_id') if doc else None
        return None

    # Naya function duplicate check karne ke liye
    async def find_file(self, file_unique_id):
        if self.collection is not None:
            return await self.collection.find_one({'file_unique_id': file_unique_id})
        return None

db = Database()
