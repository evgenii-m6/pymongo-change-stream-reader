import logging
from typing import Any

from bson import Binary
from pymongo import MongoClient

from pymongo_change_stream_reader.models import SavedToken


default_logger = logging.Logger(__name__, logging.INFO)


class TokenSaving:
    def __init__(
        self,
        token_mongo_client: MongoClient,
        token_database: str,
        token_collection: str,
        logger: logging.Logger = default_logger,
    ):
        self._token_mongo_client = token_mongo_client
        self._token_database = token_database
        self._token_collection = token_collection
        self._logger = logger

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
        self._collection.replace_one(
            filter={"stream_reader_name": token_data.stream_reader_name},
            replacement=replacement,
            upsert=True
        )
