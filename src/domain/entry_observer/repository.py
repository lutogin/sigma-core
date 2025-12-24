"""
Entry Observer Repository.

MongoDB persistence for watch candidates.
"""

from datetime import datetime, timezone
from typing import List, Optional

from src.infra.mongo import MongoDatabase
from src.domain.entry_observer.models import WatchCandidate, WatchStatus


class EntryObserverRepository:
    """
    Repository for watch candidate persistence in MongoDB.

    Stores active watches so they can be restored after deployment/restart.
    """

    COLLECTION_NAME = "entry_observer_watches"

    def __init__(self, mongo_db: MongoDatabase, logger):
        """
        Initialize repository.

        Args:
            mongo_db: MongoDB database connection.
            logger: Application logger.
        """
        self._db = mongo_db
        self._logger = logger

    def save_watch(self, watch: WatchCandidate) -> None:
        """
        Save or update a watch in MongoDB.

        Uses upsert to handle both new and existing watches.

        Args:
            watch: WatchCandidate to save.
        """
        collection = self._db.get_collection(self.COLLECTION_NAME)
        data = watch.to_dict()

        # Upsert by coin_symbol
        collection.update_one(
            {"coin_symbol": watch.coin_symbol},
            {"$set": data},
            upsert=True,
        )

        self._logger.debug(f"Saved watch to MongoDB: {watch.coin_symbol}")

    def delete_watch(self, coin_symbol: str) -> bool:
        """
        Delete a watch from MongoDB.

        Args:
            coin_symbol: Symbol to delete.

        Returns:
            True if deleted, False if not found.
        """
        collection = self._db.get_collection(self.COLLECTION_NAME)
        result = collection.delete_one({"coin_symbol": coin_symbol})

        if result.deleted_count > 0:
            self._logger.debug(f"Deleted watch from MongoDB: {coin_symbol}")
            return True
        return False

    def get_active_watches(self) -> List[WatchCandidate]:
        """
        Get all active watches from MongoDB.

        Returns:
            List of WatchCandidate objects with status=WATCHING.
        """
        collection = self._db.get_collection(self.COLLECTION_NAME)
        cursor = collection.find({"status": WatchStatus.WATCHING.value})
        return [WatchCandidate.from_dict(doc) for doc in cursor]

    def get_watch_by_symbol(self, coin_symbol: str) -> Optional[WatchCandidate]:
        """
        Get a specific watch by symbol.

        Args:
            coin_symbol: Symbol to find.

        Returns:
            WatchCandidate or None.
        """
        collection = self._db.get_collection(self.COLLECTION_NAME)
        doc = collection.find_one({"coin_symbol": coin_symbol})
        return WatchCandidate.from_dict(doc) if doc else None

    def delete_expired_watches(self, timeout_seconds: float) -> int:
        """
        Delete watches older than timeout.

        Args:
            timeout_seconds: Maximum age in seconds.

        Returns:
            Number of deleted watches.
        """
        collection = self._db.get_collection(self.COLLECTION_NAME)
        cutoff = datetime.now(timezone.utc).timestamp() - timeout_seconds

        # Find watches created before cutoff
        # created_at is stored as ISO string, need to compare
        cursor = collection.find({"status": WatchStatus.WATCHING.value})

        deleted_count = 0
        for doc in cursor:
            try:
                created_at = datetime.fromisoformat(doc["created_at"])
                if created_at.timestamp() < cutoff:
                    collection.delete_one({"_id": doc["_id"]})
                    deleted_count += 1
                    self._logger.debug(
                        f"Deleted expired watch: {doc['coin_symbol']} "
                        f"(age: {(datetime.now(timezone.utc) - created_at).total_seconds() / 60:.1f} min)"
                    )
            except Exception as e:
                self._logger.warning(f"Error checking watch expiry: {e}")

        if deleted_count > 0:
            self._logger.info(f"Deleted {deleted_count} expired watches from MongoDB")

        return deleted_count

    def delete_all_watches(self) -> int:
        """
        Delete all watches (for cleanup).

        Returns:
            Number of deleted watches.
        """
        collection = self._db.get_collection(self.COLLECTION_NAME)
        result = collection.delete_many({})
        return result.deleted_count
