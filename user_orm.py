"""Database queries."""
import sqlite3
import logging
from typing import Any

from .constants import DATABASE
from .exceptions import (FieldDoesNotExistError, ObjectDoesNotExistError,
                         ValidationError)


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

USERS_TABLE_FIELDS = ('chat_id', 'lang', 'is_main_message',)


def create_database() -> None:
    """Note: Run once, to create database."""
    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    chat_id INTEGER PRIMARY KEY,
                    lang TEXT,
                    is_main_message INTEGER DEFAULT 0
                )
            ''')
            logger.info("Database and 'users' table created or verified.")
        except sqlite3.Error as e:
            logger.error(f"Error while creating database: {e}")


def create_user(chat_id: int) -> None:
    """Record user in the database."""
    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO users (chat_id) VALUES (?)
                ''', (chat_id,))
            conn.commit()
            logger.info(f"User with chat_id {chat_id} created or updated.")
        except sqlite3.Error as e:
            logger.error(f"Error while creating/updating user {chat_id}: {e}")


class User:
    """Users' records manager.

    chat_id: int - User's chat_id
    """

    def __init__(self, chat_id: int) -> None:
        """Initialize class data."""
        self.chat_id = chat_id
        self.database_path = DATABASE
        logger.info(f"Initializing User class for chat_id: {chat_id}")
        self._ensure_object_exists(chat_id)

    def _ensure_object_exists(self, pk) -> None:
        """Ensure object exists."""
        with sqlite3.connect(DATABASE) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM users WHERE chat_id = ?',
                           (pk,))
            result = cursor.fetchone()
        if result is None:
            logger.error(f"User with chat_id {pk} does not exist.")
            raise ObjectDoesNotExistError(
                'Object with this chat_id does not exist.')
        logger.info(f"User with chat_id {pk} exists.")

    def ensure_field_exists(self, field: str) -> bool:
        """Ensure field exists, if not raise FieldDoesNotExistsError.

        This step prevents SQL injections.
        """
        if field in USERS_TABLE_FIELDS:
            logger.info(f"Field '{field}' exists in 'users' table.")
            return True
        logger.error(f"Field '{field}' does not exist in db.")
        raise FieldDoesNotExistError(f'Field "{field}" does not exist in db.')

    def validate_field_name(self, field_name) -> None:
        """Validate field name parameter."""
        if field_name == 'chat_id':
            logger.error('Attempted to edit protected field "chat_id".')
            raise ValidationError('Name "chat_id" is not allowed to edit.')
        logger.info(f"Field name '{field_name}' validated.")

    def get_field(self, field_name) -> Any:
        """Get field by field name."""
        self.ensure_field_exists(field_name)
        with sqlite3.connect(DATABASE) as conn:
            cursor = conn.cursor()
            cursor.execute(f'SELECT {field_name} FROM users WHERE chat_id = ?',
                           (self.chat_id,))
            result = cursor.fetchone()
            if result:
                logger.info(f"Retrieved '{field_name}' for chat_id {self.chat_id}: {result[0]}")
                return result[0]
            logger.warning(f"No value found for '{field_name}' for chat_id {self.chat_id}")
            return None

    def edit_field(self, field_name, new_value) -> Any:
        """Edit field by field name."""
        self.ensure_field_exists(field_name)
        self.validate_field_name(field_name)
        with sqlite3.connect(DATABASE) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f'UPDATE users SET {field_name} = ? WHERE chat_id = ?',
                    (new_value, self.chat_id))
                conn.commit()
                logger.info(f"Field '{field_name}' updated for chat_id {self.chat_id} to {new_value}.")
            except sqlite3.Error as e:
                logger.error(f"Error while updating field '{field_name}' for chat_id {self.chat_id}: {e}")
