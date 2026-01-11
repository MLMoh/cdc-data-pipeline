from typing import Dict, Optional, Any, Generator
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date
import psutil
from decimal import Decimal

from .base_loader import DataSourceLoader


class PostgresToClickhouseLoader(DataSourceLoader):
    """Loader for extracting data from PostgreSQL to ClickHouse via S3."""

    def __init__(
        self,
        postgres_host: str,
        postgres_port: int,
        postgres_user: str,
        postgres_password: str,
        postgres_database: str,
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
        upsert_key: str = "id",
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

        self.postgres_host = postgres_host
        self.postgres_port = postgres_port
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.postgres_database = postgres_database
        self.postgres_conn = None

    def connect_source(self) -> None:
        try:
            self.logger.info(
                f"Connecting to PostgreSQL at {self.postgres_host}:{self.postgres_port}"
            )
            self.postgres_conn = psycopg2.connect(
                host=self.postgres_host,
                port=self.postgres_port,
                user=self.postgres_user,
                password=self.postgres_password,
                database=self.postgres_database,
            )
            self.logger.info("Successfully connected to PostgreSQL")
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise

    def extract_data(
        self,
        table_name: str,
        last_value: Optional[str] = None,
        source_schema: str = "public",
    ) -> Generator[Dict[str, Any], None, None]:
        if not self.postgres_conn:
            self.connect_source()

        try:
            with self.postgres_conn.cursor() as cursor:
                count_query = f"SELECT COUNT(*) FROM {source_schema}.{table_name}"
                count_params = []

                if last_value is not None:
                    count_query += f" WHERE {self.tracking_column} > %s"
                    count_params.append(last_value)

                cursor.execute(count_query, count_params)
                total_count = cursor.fetchone()[0]
                self.logger.info(f"Query will return approximately {total_count} rows")

            with self.postgres_conn.cursor(
                name="large_result_cursor", cursor_factory=RealDictCursor
            ) as cursor:
                query = f"SELECT * FROM {source_schema}.{table_name}"
                params = []

                if last_value is not None:
                    query += f" WHERE {self.tracking_column} > %s"
                    params.append(last_value)

                self.logger.info(f"Executing query on {source_schema}.{table_name}")
                cursor.execute(query, params)
                self.logger.info("Query executed. Starting to fetch results...")

                batch_size = self.batch_size
                batch_num = 0
                total_rows = 0

                while True:
                    batch = cursor.fetchmany(batch_size)

                    if not batch:
                        self.logger.info(f"Finished extracting {total_rows} rows total")
                        break

                    batch_num += 1
                    batch_row_count = len(batch)
                    total_rows += batch_row_count

                    self.logger.info(
                        f"Processing batch {batch_num} ({total_rows} rows so far)"
                    )

                    for row in batch:
                        processed_row = {}
                        for key, value in dict(row).items():
                            if isinstance(value, datetime):
                                processed_row[key] = value.strftime("%Y-%m-%d %H:%M:%S")
                            elif isinstance(value, date):
                                processed_row[key] = value.strftime("%Y-%m-%d")
                            elif isinstance(value, Decimal):
                                processed_row[key] = float(value)
                            else:
                                processed_row[key] = value

                        yield processed_row

                    try:
                        process = psutil.Process()
                        memory_info = process.memory_info()
                        self.logger.info(
                            f"Memory usage: {memory_info.rss / (1024 * 1024):.2f} MB"
                        )
                    except Exception as e:
                        self.logger.warning(f"Memory check failed: {str(e)}")

        except Exception as e:
            self.logger.error(f"Failed to extract data from PostgreSQL: {str(e)}")
            raise

    def close_connections(self) -> None:
        super().close_connections()

        if self.postgres_conn:
            try:
                self.postgres_conn.close()
                self.logger.info("Closed PostgreSQL connection")
            except Exception as e:
                self.logger.error(f"Error closing PostgreSQL connection: {str(e)}")
