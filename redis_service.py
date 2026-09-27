import json
import os
from typing import Any, Optional

from dotenv import load_dotenv
from redis import Redis

# Load environment variables
load_dotenv()


class RedisService:
    def __init__(self):
        self.redis_client = Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.getenv("REDIS_DB", 0)),
            password=os.getenv("REDIS_PASSWORD", None),
            decode_responses=True,
        )

    def set_key(self, key: str, value: Any, expire: Optional[int] = None) -> bool:
        """Set a key-value pair in Redis.

        Args:
            key: The key to set
            value: The value to set (will be JSON serialized)
            expire: Optional expiration time in seconds

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            serialized_value = json.dumps(value)
            if expire:
                return self.redis_client.setex(key, expire, serialized_value)
            return self.redis_client.set(key, serialized_value)
        except Exception as e:
            print(f"Error setting Redis key: {e}")
            return False

    def get_key(self, key: str) -> Optional[Any]:
        """Get a value from Redis by key.

        Args:
            key: The key to get

        Returns:
            The deserialized value if found, None otherwise
        """
        try:
            value = self.redis_client.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            print(f"Error getting Redis key: {e}")
            return None

    def delete_key(self, key: str) -> bool:
        """Delete a key from Redis.

        Args:
            key: The key to delete

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            return bool(self.redis_client.delete(key))
        except Exception as e:
            print(f"Error deleting Redis key: {e}")
            return False

    def exists(self, key: str) -> bool:
        """Check if a key exists in Redis.

        Args:
            key: The key to check

        Returns:
            bool: True if key exists, False otherwise
        """
        try:
            return bool(self.redis_client.exists(key))
        except Exception as e:
            print(f"Error checking Redis key existence: {e}")
            return False

    def set_hash(self, name: str, mapping: dict, expire: Optional[int] = None) -> bool:
        """Set a hash in Redis.

        Args:
            name: The name of the hash
            mapping: Dictionary of key-value pairs
            expire: Optional expiration time in seconds

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            serialized_mapping = {k: json.dumps(v) for k, v in mapping.items()}
            result = self.redis_client.hset(name, mapping=serialized_mapping)
            if expire:
                self.redis_client.expire(name, expire)
            return bool(result)
        except Exception as e:
            print(f"Error setting Redis hash: {e}")
            return False

    def get_hash(self, name: str) -> Optional[dict]:
        """Get a hash from Redis.

        Args:
            name: The name of the hash

        Returns:
            dict: The hash data if found, None otherwise
        """
        try:
            data = self.redis_client.hgetall(name)
            if data:
                return {k: json.loads(v) for k, v in data.items()}
            return None
        except Exception as e:
            print(f"Error getting Redis hash: {e}")
            return None

    def delete_hash(self, name: str) -> bool:
        """Delete a hash from Redis.

        Args:
            name: The name of the hash to delete

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            return bool(self.redis_client.delete(name))
        except Exception as e:
            print(f"Error deleting Redis hash: {e}")
            return False


# Create a singleton instance
redis_service = RedisService()
