import hashlib
import json
import os
import secrets
from pathlib import Path
from typing import Dict, Optional


class PasswordManager:
    def __init__(self):
        """Initialize the password manager."""
        # In a real application, this would be a database
        # For this example, we'll use a simple JSON file
        self.db_file = Path("user_passwords.json")
        self.users: Dict[str, str] = self._load_users()

    def _load_users(self) -> Dict[str, str]:
        """Load user data from the JSON file."""
        if self.db_file.exists():
            with open(self.db_file, "r") as f:
                return json.load(f)
        return {}

    def _save_users(self):
        """Save user data to the JSON file."""
        with open(self.db_file, "w") as f:
            json.dump(self.users, f, indent=2)

    def _hash_password(self, password: str) -> str:
        """Hash a password using SHA-256 and a salt."""
        # In a real application, use a proper password hashing algorithm like bcrypt
        salt = secrets.token_hex(16)
        hash_obj = hashlib.sha256((password + salt).encode())
        return f"{salt}:{hash_obj.hexdigest()}"

    async def reset_password(self, email: str, new_password: str) -> bool:
        """Reset a user's password."""
        try:
            # Hash the new password
            hashed_password = self._hash_password(new_password)

            # Store the new password
            self.users[email] = hashed_password
            self._save_users()

            return True

        except Exception as e:
            print(f"Error resetting password: {e}")
            return False


# Create a singleton instance
_password_manager: Optional[PasswordManager] = None


def get_password_manager() -> PasswordManager:
    """Get or create the password manager singleton instance."""
    global _password_manager
    if _password_manager is None:
        _password_manager = PasswordManager()
    return _password_manager


async def reset_password(email: str, new_password: str) -> bool:
    """Reset a user's password."""
    manager = get_password_manager()
    return await manager.reset_password(email, new_password)
