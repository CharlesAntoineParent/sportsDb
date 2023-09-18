"""This class is the base class for all mongoDb classes in the sportsdb package."""
import logging
import math
from abc import ABC
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

from pydantic import BaseModel
from pymongo import MongoClient
from pymongo.cursor import Cursor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

RT = TypeVar("RT")


def check_db_connection(func: Callable[..., RT]) -> Callable[..., RT]:
    """This wrapper checks if a db connection is established and if not, it establishes one.

    Args:
        func (callable): Method to be wrapped

    Returns:
        callable: Method wrapped with a db connection check
    """

    def wrapper(self: MongoDB, *args: Any, **kwargs: Any) -> RT:
        if not self.db:
            logging.info("No db connection, connecting to db")
            self.connect("NbaDb")
        return func(self, *args, **kwargs)

    return wrapper


class MongoDB(BaseModel, ABC):
    """This class implements base method for a mongoDb.

    Args:
        host (str): Path to the host
        db_name (str): Name of the db on the host
    """

    host: str
    db_name: str

    @check_db_connection
    def get_table_names(self) -> List[str]:
        """This method returns all table names in the db.

        Returns:
            List[str]: List of table names in the connected db
        """
        return self.db.list_collection_names()

    @check_db_connection
    def post_data(self, table_name: str, data: Dict[str, Any]) -> None:
        """This method post on document to a collection.

        Args:
            table_name (str): Name of the collection in the db
            data (Dict): Document to be posted
        """
        collection = self.db[table_name]
        collection.insert_one(data)

    @check_db_connection
    def read_all_table(self, table_name: str) -> Cursor[Any]:
        """This method reads all documents in a collection.

        Args:
            table_name (str): Name og the collection in the db

        Returns:
            PyMongo.Cursor: Cursor to iterate over the documents
        """
        return self.query_data(table_name, {})

    @check_db_connection
    def query_data(self, table_name: str, query: Any) -> Cursor[Any]:
        """This method queries a collection.

        Args:
            table_name (str): _description_
            query (Any): Pymongo query to exectute

        Returns:
            Cursor: Pointer to the corresponding documents
        """
        collection = self.db[table_name]
        return collection.find(query)

    @check_db_connection
    def post_multiple_data(self, table_name: str, data_list: List[Dict[str, Any]]) -> None:
        """This method posts multiple documents to a collection.

        Args:
            table_name (str): Name of the collection in the db
            data_list (List[Dict[str, Any]]): List of documents to be posted
        """
        in_table, not_in_table = self._split_data_duplicate_key(
            table_name=table_name, data_list=data_list
        )
        logging.info(
            "Collection: %s, New Entries: %s, Already in table: %s",
            table_name,
            not_in_table,
            in_table,
        )
        collection = self.db[table_name]
        if len(not_in_table):
            collection.insert_many(not_in_table)

    @check_db_connection
    def are_ids_already_in_table(
        self, table_name: str, ids: List[int]
    ) -> Tuple[List[int], List[int]]:
        """This method split ids based on if they are in a given collections.

        Args:
            table_name (str): Name of the collections
            ids (List[int]): List of ids to be checked

        Returns:
            Tuple[List[int], List[int]]: First list contains ids that are in the collection,
            second list contains ids that are not in the collection
        """
        existing_ids_cursor = self.query_data(table_name, {"_id": {"$in": ids}}, {"_id": 1})
        existing_ids = [doc["_id"] for doc in existing_ids_cursor]
        in_table = []
        not_in_table = []
        for current_id in ids:
            if current_id in existing_ids:
                in_table.append(current_id)
            else:
                not_in_table.append(current_id)
        return in_table, not_in_table

    def _split_data_duplicate_key(
        self, table_name: str, data_list: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """This method split ids based on if they are in a given collections.

        Args:
            table_name (str): Name of the collections
            data_list (List[Dict[str, Any]]): List of documents to be checked

        Returns:
            Tuple[List[int], List[int]]: First list contains ids that are in the collection,
            second list contains ids that are not in the collection
        """
        collection = self.db[table_name]
        documents_existing_ids = collection.find(
            {"_id": {"$in": [doc["_id"] for doc in data_list]}}, {"_id": 1}
        )
        existing_ids = [doc["_id"] for doc in documents_existing_ids]
        in_table = []
        not_in_table = []
        for doc in data_list:
            if doc["_id"] in existing_ids:
                in_table.append(doc)
            else:
                not_in_table.append(doc)
        return in_table, not_in_table

    @check_db_connection
    def create_collection(
        self, table_name: str, secondary_keys: Optional[List[str]] = None
    ) -> None:
        """This method creates a collection in the db.

        Args:
            table_name (str): Name of the collection
            secondary_keys (Optional[List[str]], optional): List of secondary keys. Defaults to None
        """
        if secondary_keys is None:
            secondary_keys = []
        self.db.create_collection(table_name)
        for sec_key in secondary_keys:
            self.db[table_name].create_index(sec_key)

    @check_db_connection
    def create_collection_if_necessary(
        self, table_name: str, secondary_keys: Optional[List[str]] = None
    ) -> None:
        """Create collection if it does not exist.

        Args:
            table_name (str): Name of the collection
            secondary_keys (List[str], optional): Name of the secondary indexed columns. Defaults to
            None.
        """
        if secondary_keys is None:
            secondary_keys = []
        if table_name not in self.get_table_names():
            self.create_collection(table_name, secondary_keys)

    @check_db_connection
    def log_collections(self) -> None:
        """Log collection names and number of entries in each collection."""
        for collection_name in self.get_table_names():
            collection = self.db[collection_name]
            entry_count = collection.estimated_document_count()
            logging.info(
                "Collection: %s, Entries: %s",
                collection_name,
                entry_count,
            )

    @check_db_connection
    def get_min_value(self, collection_name: str, column_name: str) -> Any:
        """This function returns the minimum value of a column in a collection.

        Args:
            collection_name (str): Collection name
            column_name (str): Column name

        Returns:
            float: Minimum value of the column
        """
        assert not self.is_collection_empty(collection_name)
        pipeline_existing_value = {"$match": {f"{column_name}": {"$exists": True, "$ne": math.nan}}}
        pipeline_min_value = {"$group": {"_id": {}, "min_value": {"$min": f"${column_name}"}}}
        min_value_docs = self.db[collection_name].aggregate(
            [pipeline_existing_value, pipeline_min_value]
        )
        minimum_value_doc = next(iter(min_value_docs))
        return minimum_value_doc["min_value"]

    @check_db_connection
    def get_max_value(self, collection_name: str, column_name: str) -> Any:
        """This function returns the maximum value of a column in a collection.

        Args:
            collection_name (str): Collection name
            column_name (str): Column name

        Returns:
            float: Maximum value of the column
        """
        assert not self.is_collection_empty(collection_name)
        pipeline_existing_value = {"$match": {f"{column_name}": {"$exists": True, "$ne": math.nan}}}
        pipeline_max_value = {"$group": {"_id": {}, "max_value": {"$max": f"${column_name}"}}}
        max_value_docs = self.db[collection_name].aggregate(
            [pipeline_existing_value, pipeline_max_value]
        )
        max_value_doc = next(iter(max_value_docs))
        return max_value_doc["min_value"]

    def connect(self, db_name: str) -> None:
        """This method connects to a db.

        Args:
            db_name (str): Name of the db on the given host
        """
        self.client: MongoClient[Dict[str, Any]] = MongoClient(self.host)
        self.db = self.client[db_name]
        self.log_collections()

    def is_collection_empty(self, collection_name: str) -> bool:
        """Check if a collection is empty.

        Args:
            collection_name (str): Name of the collection

        Returns:
            bool: Boolean indicating if the collection is empty
        """
        return self.db[collection_name].count_documents({}) == 0

    def get_last_update_time(self, collection_name: str) -> None:
        """Not implemented yet. But this functions needs to check if db exists.

        Args:
            collection_name (str): Name of the collection
        """
        # TODO: Implement
