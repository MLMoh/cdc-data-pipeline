from typing import Dict, Any
from .clickhouse_load_tool.postgres_loader import PostgresToClickhouseLoader


def create_loader(config: Dict[str, Any]) -> PostgresToClickhouseLoader:
    return PostgresToClickhouseLoader(
        postgres_host=config.get('postgres_host'),
        postgres_port=int(config.get('postgres_port')),
        postgres_user=config.get('postgres_user'),
        postgres_password=config.get('postgres_password'),
        postgres_database=config.get('postgres_db'),
        clickhouse_host=config.get('clickhouse_host'),
        clickhouse_port=int(config.get('clickhouse_port', 8123)),
        clickhouse_user=config.get('clickhouse_user'),
        clickhouse_password=config.get('clickhouse_password'),
        clickhouse_database=config.get('clickhouse_db', 'default'),
        s3_bucket=config.get('s3_bucket'),
        s3_access_key=config.get('s3_access_key', ''),
        s3_secret_key=config.get('s3_secret_key', ''),
        s3_endpoint=config.get('s3_endpoint'),
        tracking_column=config.get('tracking_column', 'updated_at'),
        upsert_key=config.get('upsert_key', 'id'),
        batch_size=int(config.get('batch_size', 10000))
    )


def extract_postgres_to_object_storage(
    config: Dict[str, Any], 
    source_table: str, 
    source_schema: str,
    target_table: str, 
    destination: str = 's3',
    load_type: str = 'incremental', 
    output_key: str = None
) -> str:
    loader = create_loader(config)
    
    object_key = loader.extract_to_storage(
        source_table=source_table,
        target_table=target_table,
        destination=destination,
        source_schema=source_schema,
        load_type=load_type,
        output_key=output_key
    )
    
    return object_key


def load_data_to_clickhouse(
    config: Dict[str, Any], 
    file_key: str, 
    target_table: str, 
    source: str = 's3',
    load_type: str = 'incremental', 
    derived_column: str = None
) -> int:
    if not file_key:
        return 0
        
    loader = create_loader(config)
    
    rows_loaded = loader.load_to_clickhouse(
        file_key=file_key,
        target_table=target_table,
        source=source,
        load_type=load_type,
        derived_column=derived_column
    )
    
    return rows_loaded