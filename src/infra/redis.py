"""
Redis client module with CRUD operations and TTL support.

Provides Redis caching functionality with standard CRUD methods
and configurable TTL (Time To Live) for cached data.
"""

import json
import logging
from typing import Any, Callable, Optional, Union

try:
    import redis.asyncio as redis
    from redis.asyncio import Redis
except ImportError:
    redis = None
    Redis = None  # type: ignore


class RedisCache:
    """
    Redis cache client with CRUD operations and TTL support.

    Provides standard CRUD methods:
    - Create/Set: Store data with optional TTL
    - Read/Get: Retrieve data by key
    - Update: Update existing data with new TTL
    - Delete: Remove data by key
    """

    def __init__(
        self,
        redis_url: str,
        logger=None,
        decode_responses: bool = True,
    ):
        """
        Initialize Redis cache client.

        Args:
            redis_url: Redis connection URL
            logger: Optional logger instance
            decode_responses: Whether to decode responses as strings
        """
        if redis is None:
            raise ImportError(
                "redis package not installed. Install with: pip install redis"
            )

        self.redis_url = redis_url
        self.logger = logger or logging.getLogger(__name__)
        self.decode_responses = decode_responses
        self._client: Any = None

    async def connect(self) -> None:
        """Connect to Redis server."""
        try:
            self._client = redis.from_url(
                self.redis_url,
                decode_responses=self.decode_responses,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
            )
            # Test connection
            await self._client.ping()
            self.logger.info("💾 Connected to Redis successfully")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def disconnect(self) -> None:
        """Disconnect from Redis server."""
        if self._client:
            try:
                await self._client.close()
                self.logger.info("Disconnected from Redis")
            except Exception as e:
                self.logger.warning(f"Error disconnecting from Redis: {e}")
            finally:
                self._client = None

    def _ensure_client(self) -> Any:
        """Ensure Redis client is connected."""
        if self._client is None:
            raise RuntimeError("Redis client not connected. Call connect() first.")
        return self._client

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[Union[int, float]] = None,
        json_encode: bool = True,
    ) -> bool:
        """
        Set a key-value pair in Redis with optional TTL.

        Args:
            key: Redis key
            value: Value to store
            ttl: Time to live in seconds (optional)
            json_encode: Whether to JSON-encode complex objects

        Returns:
            True if successful, False otherwise
        """
        try:
            client = self._ensure_client()

            # Serialize complex objects
            if json_encode and not isinstance(value, (str, int, float, bool)):
                value = json.dumps(value, default=str)

            # Set value with optional TTL
            if ttl is not None:
                result = await client.setex(key, int(ttl), value)
            else:
                result = await client.set(key, value)

            return bool(result)
        except Exception as e:
            self.logger.error(f"Error setting key '{key}': {e}")
            return False

    async def get(
        self,
        key: str,
        json_decode: bool = True,
        default: Any = None,
    ) -> Any:
        """
        Get value by key from Redis.

        Args:
            key: Redis key
            json_decode: Whether to JSON-decode complex objects
            default: Default value if key not found

        Returns:
            Value associated with key or default
        """
        try:
            client = self._ensure_client()
            value = await client.get(key)

            if value is None:
                return default

            # Deserialize JSON objects
            if json_decode and isinstance(value, str):
                try:
                    return json.loads(value)
                except (json.JSONDecodeError, ValueError):
                    # Not JSON, return as string
                    return value

            return value
        except Exception as e:
            self.logger.error(f"Error getting key '{key}': {e}")
            return default

    async def delete(self, *keys: str) -> int:
        """
        Delete one or more keys from Redis.

        Args:
            keys: One or more keys to delete

        Returns:
            Number of keys deleted
        """
        try:
            client = self._ensure_client()
            result = await client.delete(*keys)
            return int(result) if result else 0
        except Exception as e:
            self.logger.error(f"Error deleting keys {keys}: {e}")
            return 0

    async def update(
        self,
        key: str,
        value: Any,
        ttl: Optional[Union[int, float]] = None,
        json_encode: bool = True,
    ) -> bool:
        """
        Update existing key-value pair in Redis.

        This is essentially the same as set() but explicitly for updates.

        Args:
            key: Redis key
            value: New value to store
            ttl: Time to live in seconds (optional)
            json_encode: Whether to JSON-encode complex objects

        Returns:
            True if successful, False otherwise
        """
        return await self.set(key, value, ttl, json_encode)

    async def exists(self, key: str) -> bool:
        """
        Check if key exists in Redis.

        Args:
            key: Redis key to check

        Returns:
            True if key exists, False otherwise
        """
        try:
            client = self._ensure_client()
            result = await client.exists(key)
            return bool(result)
        except Exception as e:
            self.logger.error(f"Error checking existence of key '{key}': {e}")
            return False

    async def expire(self, key: str, ttl: Union[int, float]) -> bool:
        """
        Set TTL for existing key.

        Args:
            key: Redis key
            ttl: Time to live in seconds

        Returns:
            True if TTL was set, False otherwise
        """
        try:
            client = self._ensure_client()
            result = await client.expire(key, int(ttl))
            return bool(result)
        except Exception as e:
            self.logger.error(f"Error setting TTL for key '{key}': {e}")
            return False

    async def ttl(self, key: str) -> Optional[int]:
        """
        Get TTL for key in seconds.

        Args:
            key: Redis key

        Returns:
            TTL in seconds, None if key doesn't exist or has no TTL
        """
        try:
            client = self._ensure_client()
            ttl = await client.ttl(key)
            return int(ttl) if ttl != -2 else None  # -2 means key doesn't exist
        except Exception as e:
            self.logger.error(f"Error getting TTL for key '{key}': {e}")
            return None

    async def keys(self, pattern: str = "*") -> list[str]:
        """
        Get all keys matching pattern.

        Args:
            pattern: Redis key pattern (default: all keys)

        Returns:
            List of matching keys
        """
        try:
            client = self._ensure_client()
            keys = await client.keys(pattern)
            return [key for key in keys if key]  # Filter out empty strings
        except Exception as e:
            self.logger.error(f"Error getting keys with pattern '{pattern}': {e}")
            return []

    async def flushdb(self) -> bool:
        """
        Clear all keys in current database.

        Returns:
            True if successful, False otherwise
        """
        try:
            client = self._ensure_client()
            await client.flushdb()
            return True
        except Exception as e:
            self.logger.error(f"Error flushing database: {e}")
            return False

    async def info(self) -> Optional[dict]:
        """
        Get Redis server information.

        Returns:
            Redis info dictionary or None if error
        """
        try:
            client = self._ensure_client()
            info = await client.info()
            return info
        except Exception as e:
            self.logger.error(f"Error getting Redis info: {e}")
            return None

    @property
    def is_connected(self) -> bool:
        """Check if Redis client is connected."""
        return self._client is not None


# Convenience functions for common cache operations


async def cache_get_or_set(
    cache: RedisCache,
    key: str,
    fetch_func: Callable[..., Any],
    ttl: Optional[Union[int, float]] = None,
    json_encode: bool = True,
) -> Any:
    """
    Get value from cache or fetch and set if not found.

    Args:
        cache: RedisCache instance
        key: Cache key
        fetch_func: Function to call if key not found in cache
        ttl: Time to live in seconds
        json_encode: Whether to JSON-encode the result

    Returns:
        Cached or freshly fetched value
    """
    # Try to get from cache first
    cached_value = await cache.get(key)
    if cached_value is not None:
        return cached_value

    # Fetch fresh value
    if callable(fetch_func):
        fresh_value = await fetch_func()
    else:
        fresh_value = fetch_func

    # Cache the result
    if fresh_value is not None:
        await cache.set(key, fresh_value, ttl, json_encode)

    return fresh_value


async def cache_invalidate_pattern(
    cache: RedisCache,
    pattern: str,
) -> int:
    """
    Invalidate all cache keys matching pattern.

    Args:
        cache: RedisCache instance
        pattern: Key pattern to match

    Returns:
        Number of keys deleted
    """
    keys = await cache.keys(pattern)
    if keys:
        return await cache.delete(*keys)
    return 0
