"""
Position State Repository.

MongoDB persistence for spread positions and cooldowns.
"""

from datetime import datetime, timezone
from typing import List, Optional

from src.infra.mongo import MongoDatabase
from src.domain.position_state.models import SpreadPosition, SymbolCooldown


class PositionStateRepository:
    """
    Repository for position state persistence in MongoDB.

    Collections:
    - spread_positions: Active spread positions
    - symbol_cooldowns: Symbol cooldown entries
    """

    POSITIONS_COLLECTION = "spread_positions"
    COOLDOWNS_COLLECTION = "symbol_cooldowns"

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
    # Positions
    # =========================================================================

    def save_position(self, position: SpreadPosition) -> str:
        """
        Save a new position to MongoDB.

        Args:
            position: SpreadPosition to save.

        Returns:
            Inserted document ID as string.
        """
        collection = self._db.get_collection(self.POSITIONS_COLLECTION)
        data = position.to_dict()

        # Remove id field for insert (MongoDB generates _id)
        data.pop("id", None)

        result = collection.insert_one(data)
        position.id = str(result.inserted_id)

        self._logger.debug(
            f"Saved position: {position.coin_symbol} ({position.side.value})"
        )
        return position.id

    def get_active_positions(self) -> List[SpreadPosition]:
        """
        Get all active positions.

        Returns:
            List of active SpreadPosition objects.
        """
        collection = self._db.get_collection(self.POSITIONS_COLLECTION)
        cursor = collection.find({"is_active": True})
        return [SpreadPosition.from_dict(doc) for doc in cursor]

    def get_position_by_symbol(self, coin_symbol: str) -> Optional[SpreadPosition]:
        """
        Get active position for a specific coin symbol.

        Args:
            coin_symbol: Coin symbol (e.g., "ADA/USDT:USDT").

        Returns:
            SpreadPosition if found, None otherwise.
        """
        collection = self._db.get_collection(self.POSITIONS_COLLECTION)
        doc = collection.find_one(
            {
                "coin_symbol": coin_symbol,
                "is_active": True,
            }
        )
        return SpreadPosition.from_dict(doc) if doc else None

    def deactivate_position(self, coin_symbol: str) -> bool:
        """
        Mark position as inactive (closed).

        Args:
            coin_symbol: Coin symbol to deactivate.

        Returns:
            True if position was found and deactivated.
        """
        collection = self._db.get_collection(self.POSITIONS_COLLECTION)
        result = collection.update_one(
            {"coin_symbol": coin_symbol, "is_active": True},
            {"$set": {"is_active": False}},
        )

        if result.modified_count > 0:
            self._logger.debug(f"Deactivated position: {coin_symbol}")
            return True
        return False

    def count_active_positions(self) -> int:
        """
        Count active positions.

        Returns:
            Number of active positions.
        """
        collection = self._db.get_collection(self.POSITIONS_COLLECTION)
        return collection.count_documents({"is_active": True})

    def get_active_symbols(self) -> set:
        """
        Get set of all active symbols (both coin and primary).

        Returns:
            Set of symbol strings.
        """
        positions = self.get_active_positions()
        symbols = set()
        for pos in positions:
            symbols.add(pos.coin_symbol.lower())
            symbols.add(pos.primary_symbol.lower())
        return symbols

    # =========================================================================
    # Cooldowns
    # =========================================================================

    def save_cooldown(self, cooldown: SymbolCooldown) -> str:
        """
        Save or update a cooldown entry.

        Uses upsert to replace existing cooldown for the same symbol.

        Args:
            cooldown: SymbolCooldown to save.

        Returns:
            Document ID as string.
        """
        collection = self._db.get_collection(self.COOLDOWNS_COLLECTION)
        data = cooldown.to_dict()

        # Upsert: replace existing cooldown for same symbol
        result = collection.update_one(
            {"symbol": cooldown.symbol},
            {"$set": data},
            upsert=True,
        )

        self._logger.debug(
            f"Saved cooldown: {cooldown.symbol} until {cooldown.expires_at}"
        )

        if result.upserted_id:
            return str(result.upserted_id)
        return cooldown.id or ""

    def get_cooldown(self, symbol: str) -> Optional[SymbolCooldown]:
        """
        Get cooldown entry for a symbol.

        Args:
            symbol: Symbol to check.

        Returns:
            SymbolCooldown if found, None otherwise.
        """
        collection = self._db.get_collection(self.COOLDOWNS_COLLECTION)
        doc = collection.find_one({"symbol": symbol})
        return SymbolCooldown.from_dict(doc) if doc else None

    def is_in_cooldown(self, symbol: str, now: Optional[datetime] = None) -> bool:
        """
        Check if a symbol is currently in cooldown.

        Args:
            symbol: Symbol to check.
            now: Current time (defaults to UTC now).

        Returns:
            True if symbol is in active cooldown.
        """
        if now is None:
            now = datetime.now(timezone.utc)

        cooldown = self.get_cooldown(symbol)
        if cooldown is None:
            return False

        return not cooldown.is_expired(now)

    def get_active_cooldowns(
        self, now: Optional[datetime] = None
    ) -> List[SymbolCooldown]:
        """
        Get all active (non-expired) cooldowns.

        Args:
            now: Current time (defaults to UTC now).

        Returns:
            List of active SymbolCooldown objects.
        """
        if now is None:
            now = datetime.now(timezone.utc)

        collection = self._db.get_collection(self.COOLDOWNS_COLLECTION)
        # Query for cooldowns that haven't expired yet
        cursor = collection.find({"expires_at": {"$gt": now.isoformat()}})
        return [SymbolCooldown.from_dict(doc) for doc in cursor]

    def remove_cooldown(self, symbol: str) -> bool:
        """
        Remove a cooldown entry.

        Args:
            symbol: Symbol to remove cooldown for.

        Returns:
            True if cooldown was found and removed.
        """
        collection = self._db.get_collection(self.COOLDOWNS_COLLECTION)
        result = collection.delete_one({"symbol": symbol})
        return result.deleted_count > 0

    def cleanup_expired_cooldowns(self, now: Optional[datetime] = None) -> int:
        """
        Remove all expired cooldowns.

        Args:
            now: Current time (defaults to UTC now).

        Returns:
            Number of cooldowns removed.
        """
        if now is None:
            now = datetime.now(timezone.utc)

        collection = self._db.get_collection(self.COOLDOWNS_COLLECTION)
        result = collection.delete_many({"expires_at": {"$lte": now.isoformat()}})

        if result.deleted_count > 0:
            self._logger.debug(f"Cleaned up {result.deleted_count} expired cooldowns")

        return result.deleted_count

    # =========================================================================
    # Initialization
    # =========================================================================

    def create_indexes(self) -> None:
        """Create MongoDB indexes for efficient queries."""
        # Positions indexes
        positions = self._db.get_collection(self.POSITIONS_COLLECTION)
        positions.create_index("coin_symbol")
        positions.create_index("is_active")
        positions.create_index([("coin_symbol", 1), ("is_active", 1)])

        # Cooldowns indexes
        cooldowns = self._db.get_collection(self.COOLDOWNS_COLLECTION)
        cooldowns.create_index("symbol", unique=True)
        cooldowns.create_index("expires_at")

        self._logger.info("Position state indexes created")
