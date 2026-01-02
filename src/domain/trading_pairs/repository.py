"""
Trading Pairs Repository.

MongoDB persistence for trading pairs configuration.
"""

from datetime import datetime, timezone
from typing import List, Optional

from src.infra.mongo import MongoDatabase
from src.domain.trading_pairs.models import TradingPair


class TradingPairRepository:
    """
    Repository for trading pairs persistence in MongoDB.

    Collection: trading_pairs

    Provides methods to manage which pairs are available for trading,
    allowing dynamic configuration without code changes.
    """

    COLLECTION_NAME = "trading_pairs"

    def __init__(self, mongo_db: MongoDatabase, logger):
        """
        Initialize repository.

        Args:
            mongo_db: MongoDB database connection.
            logger: Application logger.
        """
        self._db = mongo_db
        self._logger = logger

    # =========================================================================
    # CRUD Operations
    # =========================================================================

    def save(self, pair: TradingPair) -> str:
        """
        Save a new trading pair to MongoDB.

        Args:
            pair: TradingPair to save.

        Returns:
            Inserted document ID as string.
        """
        collection = self._db.get_collection(self.COLLECTION_NAME)
        data = pair.to_dict()

        # Remove id field for insert (MongoDB generates _id)
        data.pop("id", None)

        result = collection.insert_one(data)
        pair.id = str(result.inserted_id)

        self._logger.debug(f"Saved trading pair: {pair.symbol}")
        return pair.id

    def upsert(self, pair: TradingPair) -> str:
        """
        Save or update a trading pair (upsert by symbol).

        Args:
            pair: TradingPair to save/update.

        Returns:
            Document ID as string.
        """
        collection = self._db.get_collection(self.COLLECTION_NAME)
        pair.updated_at = datetime.now(timezone.utc)
        data = pair.to_dict()
        data.pop("id", None)

        result = collection.update_one(
            {"symbol": pair.symbol},
            {"$set": data},
            upsert=True,
        )

        self._logger.debug(f"Upserted trading pair: {pair.symbol}")

        if result.upserted_id:
            return str(result.upserted_id)
        return pair.id or ""

    def get_by_symbol(self, symbol: str) -> Optional[TradingPair]:
        """
        Get trading pair by symbol.

        Args:
            symbol: Symbol to find (e.g., "LINK/USDT:USDT").

        Returns:
            TradingPair if found, None otherwise.
        """
        collection = self._db.get_collection(self.COLLECTION_NAME)
        doc = collection.find_one({"symbol": symbol})
        return TradingPair.from_dict(doc) if doc else None

    def get_all(self) -> List[TradingPair]:
        """
        Get all trading pairs (active and inactive).

        Returns:
            List of all TradingPair objects.
        """
        collection = self._db.get_collection(self.COLLECTION_NAME)
        cursor = collection.find({})
        return [TradingPair.from_dict(doc) for doc in cursor]

    def get_active(self) -> List[TradingPair]:
        """
        Get all active trading pairs.

        Returns:
            List of active TradingPair objects.
        """
        collection = self._db.get_collection(self.COLLECTION_NAME)
        cursor = collection.find({"is_active": True})
        return [TradingPair.from_dict(doc) for doc in cursor]

    def get_active_symbols(self) -> List[str]:
        """
        Get list of active trading pair symbols.

        This is the main method for getting CONSISTENT_PAIRS equivalent.

        Returns:
            List of symbol strings (e.g., ["LINK/USDT:USDT", "UNI/USDT:USDT"]).
        """
        pairs = self.get_active()
        return [pair.symbol for pair in pairs]

    def get_by_ecosystem(self, ecosystem: str) -> List[TradingPair]:
        """
        Get trading pairs by ecosystem.

        Args:
            ecosystem: Ecosystem name (e.g., "ETH").

        Returns:
            List of TradingPair objects in the ecosystem.
        """
        collection = self._db.get_collection(self.COLLECTION_NAME)
        cursor = collection.find({"ecosystem": ecosystem, "is_active": True})
        return [TradingPair.from_dict(doc) for doc in cursor]

    def activate(self, symbol: str) -> bool:
        """
        Activate a trading pair.

        Args:
            symbol: Symbol to activate.

        Returns:
            True if pair was found and activated.
        """
        collection = self._db.get_collection(self.COLLECTION_NAME)
        result = collection.update_one(
            {"symbol": symbol},
            {
                "$set": {
                    "is_active": True,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            },
        )

        if result.modified_count > 0:
            self._logger.info(f"Activated trading pair: {symbol}")
            return True
        return False

    def deactivate(self, symbol: str) -> bool:
        """
        Deactivate a trading pair.

        Args:
            symbol: Symbol to deactivate.

        Returns:
            True if pair was found and deactivated.
        """
        collection = self._db.get_collection(self.COLLECTION_NAME)
        result = collection.update_one(
            {"symbol": symbol},
            {
                "$set": {
                    "is_active": False,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            },
        )

        if result.modified_count > 0:
            self._logger.info(f"Deactivated trading pair: {symbol}")
            return True
        return False

    def delete(self, symbol: str) -> bool:
        """
        Delete a trading pair.

        Args:
            symbol: Symbol to delete.

        Returns:
            True if pair was found and deleted.
        """
        collection = self._db.get_collection(self.COLLECTION_NAME)
        result = collection.delete_one({"symbol": symbol})

        if result.deleted_count > 0:
            self._logger.info(f"Deleted trading pair: {symbol}")
            return True
        return False

    def count_active(self) -> int:
        """
        Count active trading pairs.

        Returns:
            Number of active pairs.
        """
        collection = self._db.get_collection(self.COLLECTION_NAME)
        return collection.count_documents({"is_active": True})

    def count_all(self) -> int:
        """
        Count all trading pairs.

        Returns:
            Total number of pairs.
        """
        collection = self._db.get_collection(self.COLLECTION_NAME)
        return collection.count_documents({})

    # =========================================================================
    # Bulk Operations
    # =========================================================================

    def bulk_upsert(self, pairs: List[TradingPair]) -> int:
        """
        Bulk upsert multiple trading pairs.

        Args:
            pairs: List of TradingPair objects to upsert.

        Returns:
            Number of pairs upserted.
        """
        if not pairs:
            return 0

        from pymongo import UpdateOne

        collection = self._db.get_collection(self.COLLECTION_NAME)
        now = datetime.now(timezone.utc)

        operations = []
        for pair in pairs:
            pair.updated_at = now
            data = pair.to_dict()
            data.pop("id", None)

            operations.append(
                UpdateOne({"symbol": pair.symbol}, {"$set": data}, upsert=True)
            )

        result = collection.bulk_write(operations)
        count = result.upserted_count + result.modified_count

        self._logger.info(f"Bulk upserted {count} trading pairs")
        return count

    def seed_from_list(self, symbols: List[str], ecosystem: str = "ETH") -> int:
        """
        Seed database from a list of symbols.

        Useful for initial setup or migration from config.

        Args:
            symbols: List of symbol strings.
            ecosystem: Ecosystem name (default: "ETH").

        Returns:
            Number of pairs seeded.
        """
        pairs = [
            TradingPair(symbol=symbol, ecosystem=ecosystem, is_active=True)
            for symbol in symbols
        ]
        return self.bulk_upsert(pairs)

    # =========================================================================
    # Indexes
    # =========================================================================

    def create_indexes(self) -> None:
        """Create MongoDB indexes for efficient queries."""
        collection = self._db.get_collection(self.COLLECTION_NAME)

        # Unique index on symbol
        collection.create_index("symbol", unique=True)

        # Index for active pairs query
        collection.create_index("is_active")

        # Compound index for ecosystem + active
        collection.create_index([("ecosystem", 1), ("is_active", 1)])

        self._logger.info("Trading pairs indexes created")

