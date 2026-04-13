import motor.motor_asyncio
from typing import Union
from ..config import Config

from .models import *


class Database:
    _instance = None
    __connection: motor.motor_asyncio.AsyncIOMotorClient = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def init(self):
        if self.__connection is None:
            self.__connection = motor.motor_asyncio.AsyncIOMotorClient(
                Config.MONGODB_CONNECTION_STRING, uuidRepresentation="pythonLegacy"
            )
        return self

    async def get(
        self, database: str = Config.MONGODB_MAIN_DB, table: Union[None, str] = None
    ):
        if self.__connection is None:
            await (
                self.init()
            )  # Ensure connection is initialized if get is called directly

        if table is None:
            return self.__connection[database]
        else:
            return self.__connection[database][table]

    async def close(self):
        if self.__connection:
            self.__connection.close()
            self.__connection = (
                None  # Reset connection after closing for potential re-init
            )
        return

    async def get_connection(self):
        return self.__connection
