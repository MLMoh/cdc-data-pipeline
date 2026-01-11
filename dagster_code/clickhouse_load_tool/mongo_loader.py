from typing import Dict, List, Optional, Any, Generator, Set
import pymongo
from bson.objectid import ObjectId
from bson import json_util
from datetime import datetime
import psutil
from .base_loader import DataSourceLoader


class MongoToClickhouseLoader(DataSourceLoader):
    """Loader for extracting data from MongoDB to ClickHouse via S3."""

    def __init__(
        self,
        mongo_uri: str,
        mongo_database: str,
        clickhouse_host: str,
        clickhouse_port: int,
        clickhouse_user: str,
        clickhouse_password: str,
        clickhouse_database: str,
        s3_bucket: str,
        s3_access_key: str,
        s3_secret_key: str,
        s3_endpoint: Optional[str] = None,
        tracking_column: str = "updated_at",
        upsert_key: str = "_id",
        batch_size: int = 10000,
    ):
        super().__init__(
            clickhouse_host=clickhouse_host,
            clickhouse_port=clickhouse_port,
            clickhouse_user=clickhouse_user,
            clickhouse_password=clickhouse_password,
            clickhouse_database=clickhouse_database,
            s3_bucket=s3_bucket,
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
            s3_endpoint=s3_endpoint,
            tracking_column=tracking_column,
            upsert_key=upsert_key,
            batch_size=batch_size,
        )

        self.mongo_uri = mongo_uri
        self.mongo_database = mongo_database
        self.mongo_client = None
        self.mongo_db = None

    def connect_source(self) -> None:
        try:
            self.logger.info(
                f"Connecting to MongoDB at {self.mongo_uri.split('@')[1].replace('/', '')}"
            )
            self.mongo_client = pymongo.MongoClient(self.mongo_uri)
            self.mongo_db = self.mongo_client[self.mongo_database]
            self.logger.info("Successfully connected to MongoDB")
        except Exception as e:
            self.logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise

    def _delete_fields_from_doc(
        self, doc: Dict[str, Any], fields_to_delete: Set[str]
    ) -> Dict[str, Any]:
        result = doc.copy()

        for field_path in fields_to_delete:
            parts = field_path.split(".")

            if len(parts) == 1:
                if parts[0] in result:
                    del result[parts[0]]
                continue

            current = result
            for i, part in enumerate(parts[:-1]):
                if part not in current or not isinstance(current[part], dict):
                    break
                if i == len(parts) - 2:
                    if parts[-1] in current[part]:
                        del current[part][parts[-1]]
                else:
                    current = current[part]

        return result

    def _process_mongo_document(self, doc, fields_to_delete=None, flatten_nested=False):
        if fields_to_delete:
            doc = self._delete_fields_from_doc(doc, fields_to_delete)

        doc = self._convert_objectid(doc)
        doc = self._convert_datetime(doc)

        if flatten_nested:
            doc = self._flatten_document(doc)

        json_str = json_util.dumps(doc)
        json_str = json_str.replace("$", "")

        return json_util.loads(json_str)

    def _convert_objectid(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, dict):
            return {k: self._convert_objectid(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_objectid(item) for item in obj]
        else:
            return obj

    def _convert_datetime(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(obj, dict):
            return {k: self._convert_datetime(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_datetime(item) for item in obj]
        else:
            return obj

    def _flatten_document(self, doc):
        flattened = {}
        for key, value in doc.items():
            if isinstance(value, dict):
                for nested_key, nested_value in value.items():
                    flattened[f"{key}_{nested_key}"] = nested_value
            else:
                flattened[key] = value
        return flattened

    def extract_data(
        self,
        collection_name: str,
        last_value: Optional[str] = None,
        source_schema: Optional[str] = None,
        **kwargs,
    ) -> Generator[Dict[str, Any], None, None]:
        query_filter = kwargs.get("query_filter")
        projection = kwargs.get("projection")
        fields_to_delete = kwargs.get("fields_to_delete")
        flatten_nested = kwargs.get("flatten_nested", False)

        if self.mongo_db is None:
            self.connect_source()

        try:
            collection = self.mongo_db[collection_name]

            query = query_filter.copy() if query_filter else {}

            if last_value is not None:
                try:
                    iso_string = last_value.isoformat()
                    last_datetime = datetime.fromisoformat(iso_string)
                    query[self.tracking_column] = {"$gte": last_datetime}
                except ValueError:
                    query[self.tracking_column] = {"$gte": last_value}

            total_count = collection.count_documents(query)
            self.logger.info(
                f"Query will return approximately {total_count} documents from {collection_name}"
            )

            self.logger.info(f"Executing query on {collection_name}")
            cursor = collection.find(query, projection=projection).sort(
                [(self.tracking_column, pymongo.ASCENDING)]
            )

            fields_to_delete_set = set(fields_to_delete) if fields_to_delete else None

            batch_size = self.batch_size
            batch_num = 0
            total_docs = 0
            current_batch = []

            for doc in cursor:
                current_batch.append(doc)

                if len(current_batch) >= batch_size:
                    batch_num += 1
                    total_docs += len(current_batch)

                    self.logger.info(
                        f"Processing batch {batch_num} ({total_docs}/{total_count} docs)"
                    )

                    for doc in current_batch:
                        processed_doc = self._process_mongo_document(
                            doc,
                            fields_to_delete=fields_to_delete_set,
                            flatten_nested=flatten_nested,
                        )
                        yield processed_doc

                    try:
                        process = psutil.Process()
                        memory_info = process.memory_info()
                        self.logger.info(
                            f"Memory usage: {memory_info.rss / (1024 * 1024):.2f} MB"
                        )
                    except Exception as e:
                        self.logger.warning(f"Failed to get memory usage: {str(e)}")

                    current_batch = []

            if current_batch:
                batch_num += 1
                total_docs += len(current_batch)

                self.logger.info(
                    f"Processing final batch {batch_num} ({total_docs} docs total)"
                )

                for doc in current_batch:
                    processed_doc = self._process_mongo_document(
                        doc,
                        fields_to_delete=fields_to_delete_set,
                        flatten_nested=flatten_nested,
                    )
                    yield processed_doc

            self.logger.info(
                f"Finished extracting {total_docs} documents from {collection_name}"
            )

        except Exception as e:
            self.logger.error(f"Failed to extract data from MongoDB: {str(e)}")
            raise

    def close_connections(self) -> None:
        super().close_connections()
        if self.mongo_client:
            try:
                self.mongo_client.close()
                self.logger.info("Closed MongoDB connection")
            except Exception as e:
                self.logger.error(f"Error closing MongoDB connection: {str(e)}")
