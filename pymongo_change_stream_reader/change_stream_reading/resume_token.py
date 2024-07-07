import logging
from typing import Mapping, Any

from bson import decode
from pymongo import MongoClient

from pymongo_change_stream_reader.models import SavedToken

default_logger = logging.Logger(__name__, logging.INFO)


class RetrieveResumeToken:
    def __init__(
        self,
        stream_reader_name: str,
        token_mongo_client: MongoClient,
        token_database: str,
        token_collection: str,
        logger=logging.Logger(__name__, logging.INFO)
    ):
        self._stream_reader_name = stream_reader_name
        self._token_database = token_database
        self._token_collection = token_collection
        self._token_mongo_client = token_mongo_client
        self._logger = logger

    def start(self):
        self._logger.info("Connecting to token mongo")
        token_server_info = self._token_mongo_client.server_info()
        self._logger.info(f"Connected to mongo token server: {token_server_info}")

    def stop(self):
        self._token_mongo_client.close()

    def get_token(self) -> Mapping[str, Any] | None:
        received_saved_token = self._get_token_from_db()
        self.stop()

        if received_saved_token:
            decoded_token = self._parse_token_model(received_saved_token)
            self._logger.info(
                f"Token decoded {decoded_token} "
                f"for stream_reader_name={self._stream_reader_name}"
            )

        else:
            self._logger.info(
                f"Last token for stream_reader_name={self._stream_reader_name} "
                f"wasn't found"
            )
            decoded_token = None
        return decoded_token

    def _get_token_from_db(self) -> Mapping[str, Any] | None:
        token_collection = self._token_mongo_client.get_database(
            self._token_database
        ).get_collection(self._token_collection)

        self._logger.info(
            f"Request last token for stream_reader_name={self._stream_reader_name}"
        )
        received_saved_token: dict | None = token_collection.find_one(filter={
            "stream_reader_name": self._stream_reader_name
        })
        self._logger.debug(
            f"Got token document: {received_saved_token} "
            f"for stream_reader_name={self._stream_reader_name}"
        )

        self._logger.info(f"Close connection to mongo token server")
        return

    def _parse_token_model(
        self,
        data: Mapping[str, Any]
    ) -> Mapping[str, Any] | None:
        saved_token = SavedToken.parse_obj(data)
        return self._decode_resume_token(saved_token.token)

    @staticmethod
    def _decode_resume_token(resume_token: bytes | None) -> Mapping[str, Any] | None:
        if resume_token:
            return decode(resume_token)
        else:
            return None
