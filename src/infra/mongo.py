"""
MongoDB Database Connection.

Provides connection management and access to MongoDB collections.
"""

from typing import Optional
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection


class MongoDatabase:
    """
    MongoDB database connection manager.

    Handles connection lifecycle and provides access to collections.
    """

    def __init__(self, uri: str, database_name: str, logger):
        """
        Initialize MongoDB connection.

        :param uri: MongoDB connection URI
        :param database_name: Database name
        :param logger: Logger instance (DI)
        """
        self._uri = uri
        self._database_name = database_name
        self.logger = logger

        self._client: Optional[MongoClient] = None
        self._db: Optional[Database] = None

    @property
    def uri(self) -> str:
        """MongoDB URI."""
        return self._uri

    @property
    def database_name(self) -> str:
        """Database name."""
        return self._database_name

    def connect(self) -> None:
        """Establish connection to MongoDB."""
        if self._client is not None:
            return

        try:
            self._client = MongoClient(self.uri)
            self._db = self._client[self.database_name]

            # Ping to verify connection
            self._client.admin.command("ping")
            self.logger.info(f"💾 Connected to MongoDB: {self.database_name}")
        except Exception as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def disconnect(self) -> None:
        """Close MongoDB connection."""
        if self._client is not None:
            self._client.close()
            self._client = None
            self._db = None
            self.logger.info("Disconnected from MongoDB")

    @property
    def client(self) -> MongoClient:
        """Get MongoDB client (connects if needed)."""
        if self._client is None:
            self.connect()
        return self._client

    @property
    def db(self) -> Database:
        """Get database instance (connects if needed)."""
        if self._db is None:
            self.connect()
        return self._db

    def get_collection(self, name: str) -> Collection:
        """
        Get a collection by name.

        :param name: Collection name
        :return: MongoDB collection
        """
        return self.db[name]

    def __enter__(self) -> "MongoDatabase":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.disconnect()
