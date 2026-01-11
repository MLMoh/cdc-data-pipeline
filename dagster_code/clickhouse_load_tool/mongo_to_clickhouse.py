from typing import Dict, Any, List, Optional
from .clickhouse_load_tool.mongo_loader import MongoToClickhouseLoader


def create_loader(config: Dict[str, Any]) -> MongoToClickhouseLoader:
    return MongoToClickhouseLoader(
        mongo_uri=config.get("mongo_uri"),
        mongo_database=config.get("mongo_database"),
        clickhouse_host=config.get("clickhouse_host"),
        clickhouse_port=int(config.get("clickhouse_port", 8123)),
        clickhouse_user=config.get("clickhouse_user"),
        clickhouse_password=config.get("clickhouse_password"),
        clickhouse_database=config.get("clickhouse_db", "default"),
        s3_bucket=config.get("s3_bucket"),
        s3_access_key=config.get("s3_access_key", ""),
        s3_secret_key=config.get("s3_secret_key", ""),
        s3_endpoint=config.get("s3_endpoint"),
        tracking_column=config.get("tracking_column", "updated_at"),
        upsert_key=config.get("upsert_key", "_id"),
        batch_size=int(config.get("batch_size", 10000)),
    )


def extract_mongo_to_object_storage(
    config: Dict[str, Any],
    source_collection: str,
    target_table: str,
    query_filter: str = None,
    load_type: str = "incremental",
    destination: str = "s3",
    output_key: str = None,
    projection: Optional[Dict[str, int]] = None,
    fields_to_delete: Optional[List[str]] = None,
    flatten_nested: bool = False,
) -> str:
    loader = create_loader(config)

    extraction_params = {
        "projection": projection,
        "fields_to_delete": fields_to_delete,
        "flatten_nested": flatten_nested,
        "query_filter": query_filter,
    }

    object_key = loader.extract_to_storage(
        source_table=source_collection,
        target_table=target_table,
        destination=destination,
        load_type=load_type,
        output_key=output_key,
        **extraction_params
    )

    return object_key


def extract_mongo_to_s3(
    config: Dict[str, Any],
    source_collection: str,
    target_table: str,
    query_filter: str = None,
    load_type: str = "incremental",
    output_s3_key: str = None,
    projection: Optional[Dict[str, int]] = None,
    fields_to_delete: Optional[List[str]] = None,
    flatten_nested: bool = False,
) -> str:
    loader = create_loader(config)

    extraction_params = {
        "projection": projection,
        "fields_to_delete": fields_to_delete,
        "flatten_nested": flatten_nested,
        "query_filter": query_filter,
    }

    s3_key = loader.extract_to_s3(
        source_table=source_collection,
        target_table=target_table,
        load_type=load_type,
        output_s3_key=output_s3_key,
        **extraction_params
    )

    return s3_key


def load_data_to_clickhouse(
    config: Dict[str, Any],
    file_key: str,
    target_table: str,
    source: str = "s3",
    load_type: str = "incremental",
    format: str = "JSONEachRow",
    derived_column: str = None,
) -> int:
    if not file_key:
        return 0

    loader = create_loader(config)

    rows_loaded = loader.load_to_clickhouse(
        file_key=file_key,
        target_table=target_table,
        source=source,
        load_type=load_type,
        format=format,
        derived_column=derived_column,
    )

    return rows_loaded
