import logging
from typing import Any

from bson import Binary
from pymongo import MongoClient, timeout
from pymongo.errors import PyMongoError

from pymongo_change_stream_reader.models import SavedToken


default_logger = logging.Logger(__name__, logging.INFO)


class TokenSaving:
    def __init__(
        self,
        token_mongo_client: MongoClient,
        token_database: str,
        token_collection: str,
        logger: logging.Logger = default_logger,
        write_timeout: float = 5.0,
        on_timeout_retry_count: int = 3,
    ):
        self._token_mongo_client = token_mongo_client
        self._token_database = token_database
        self._token_collection = token_collection
        self._logger = logger
        self._write_timeout = write_timeout
        self._on_timeout_retry_count = on_timeout_retry_count

        self._collection = self._token_mongo_client.get_database(
            self._token_database
        ).get_collection(self._token_collection)

    def start(self):
        self._logger.info("Connecting to token mongo")
        token_server_info = self._token_mongo_client.server_info()
        self._logger.info(f"Connected to mongo token server: {token_server_info}")
        self._logger.info(f"Create index on token collection")
        self._collection.create_index(
            keys="stream_reader_name",
            name="stream_reader_name",
            unique=True,
        )
        self._logger.info(f"Index created on token collection")

    def stop(self):
        self._token_mongo_client.close()

    @staticmethod
    def _transform_to_dict(token_data: SavedToken) -> dict[str, Any]:
        return {
            'stream_reader_name': token_data.stream_reader_name,
            'token': Binary(token_data.token, subtype=0),
            'date': token_data.date
        }

    def save(self, token_data: SavedToken):
        replacement = self._transform_to_dict(token_data)

        count = 0
        while count < self._on_timeout_retry_count:
            try:
                self._save(
                    replacement=replacement,
                    count=count,
                )
                break
            except PyMongoError as exc:
                if exc.timeout:
                    self._logger.warning(f"Timeout when trying to save token: {exc!r}")
                    count += 1
                    if count >= self._on_timeout_retry_count:
                        self._logger.error(
                            f"Max retry count exceed when trying to save token: {exc!r}"
                        )
                        raise
                else:
                    self._logger.error(
                      f"Failed to save token with non-timeout error: {exc!r}"
                    )
                    raise

    def _save(self, replacement: dict, count: int):
        with timeout(self._write_timeout*(count+1)):
            self._collection.replace_one(
                filter={"stream_reader_name": replacement["stream_reader_name"]},
                replacement=replacement,
                upsert=True
            )
