import os
import uuid
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Any
import re
import json
import boto3
import clickhouse_connect
import tempfile

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class DataSourceLoader(ABC):
    """Abstract base class for loading data from various sources to ClickHouse via S3."""

    def __init__(
        self,
        clickhouse_host: str,
        clickhouse_port: int,
        clickhouse_user: str,
        clickhouse_password: str,
        clickhouse_database: str,
        s3_bucket: Optional[str] = None,
        s3_access_key: Optional[str] = None,
        s3_secret_key: Optional[str] = None,
        s3_endpoint: Optional[str] = None,
        tracking_column: str = "updated_at",
        upsert_key: str = "id",
        batch_size: int = 10000,
    ):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.clickhouse_host = clickhouse_host
        self.clickhouse_port = clickhouse_port
        self.clickhouse_user = clickhouse_user
        self.clickhouse_password = clickhouse_password
        self.clickhouse_database = clickhouse_database

        self.s3_bucket = s3_bucket
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.s3_endpoint = s3_endpoint

        self.tracking_column = tracking_column
        self.upsert_key = upsert_key
        self.batch_size = batch_size

        self.clickhouse_client = None
        self.s3_client = None

    def connect_clickhouse(self) -> None:
        try:
            self.logger.info(
                f"Connecting to ClickHouse at {self.clickhouse_host}:{self.clickhouse_port}"
            )
            self.clickhouse_client = clickhouse_connect.get_client(
                host=self.clickhouse_host,
                port=self.clickhouse_port,
                username=self.clickhouse_user,
                password=self.clickhouse_password,
                database=self.clickhouse_database,
            )
            self.logger.info("Successfully connected to ClickHouse")
        except Exception as e:
            self.logger.error(f"Failed to connect to ClickHouse: {str(e)}")
            raise

    def connect_s3(self) -> None:
        try:
            self.logger.info("Connecting to S3")
            s3_kwargs = {
                "aws_access_key_id": self.s3_access_key,
                "aws_secret_access_key": self.s3_secret_key,
            }

            if self.s3_endpoint:
                s3_kwargs["endpoint_url"] = self.s3_endpoint

            self.s3_client = boto3.client("s3", **s3_kwargs)
            self.logger.info("Successfully connected to S3")
        except Exception as e:
            self.logger.error(f"Failed to connect to S3: {str(e)}")
            raise

    def get_clickhouse_table_schema(self, table_name: str) -> Dict[str, str]:
        if not self.clickhouse_client:
            self.connect_clickhouse()

        try:
            query = f"DESCRIBE TABLE {table_name}"
            result = self.clickhouse_client.query(query)

            schema = {}
            for row in result.named_results():
                schema[row["name"]] = row["type"]

            return schema
        except Exception as e:
            self.logger.error(f"Failed to get schema for table {table_name}: {str(e)}")
            raise

    def upload_to_s3(self, data, s3_key: str) -> str:
        if not self.s3_client:
            self.connect_s3()

        try:
            with tempfile.NamedTemporaryFile(mode="w+", delete=False) as temp_file:
                temp_filename = temp_file.name
                self.logger.info(f"Created temporary file {temp_filename}")

                temp_file.write("[")

                is_generator = hasattr(data, "__next__")

                if is_generator:
                    first_item = True
                    count = 0

                    for item in data:
                        if not first_item:
                            temp_file.write(",")
                        else:
                            first_item = False

                        json.dump(item, temp_file)
                        count += 1

                        if count % 1000 == 0:
                            temp_file.flush()
                            self.logger.info(f"Processed {count} items")

                    self.logger.info(f"Processed total of {count} items")
                else:
                    count = len(data)
                    if count > 0:
                        json.dump(data[0], temp_file)
                        for item in data[1:]:
                            temp_file.write(",")
                            json.dump(item, temp_file)

                    self.logger.info(f"Processed {count} items")

                temp_file.write("]")
                temp_file.flush()

            with open(temp_filename, "rb") as f:
                self.s3_client.upload_fileobj(f, self.s3_bucket, s3_key)

            self.logger.info(
                f"Successfully uploaded data to S3: s3://{self.s3_bucket}/{s3_key}"
            )

            os.unlink(temp_filename)
            self.logger.info(f"Deleted temporary file {temp_filename}")

            return s3_key

        except Exception as e:
            self.logger.error(f"Failed to upload data to S3: {str(e)}")
            raise

    def download_from_s3(self, s3_key: str) -> List[Dict[str, Any]]:
        if not self.s3_client:
            self.connect_s3()

        try:
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            json_data = response["Body"].read().decode("utf-8")
            data = json.loads(json_data)

            self.logger.info(
                f"Successfully downloaded data from S3: s3://{self.s3_bucket}/{s3_key}"
            )
            return data
        except Exception as e:
            self.logger.error(f"Failed to download data from S3: {str(e)}")
            raise

    def load_to_clickhouse(
        self,
        file_key: str,
        target_table: str,
        source: str = "s3",
        load_type: str = "incremental",
        format: str = "JSONEachRow",
        derived_column: str = None,
    ) -> int:
        if not self.clickhouse_client:
            self.connect_clickhouse()

        self._configure_clickhouse_json_settings()

        try:
            self.logger.info(f"Checking if table exists...")
            table_exists_result = self.table_exists(target_table)

            if not table_exists_result:
                self.logger.info(
                    f"Table '{target_table}' does not exist. Creating from file..."
                )
                self._create_table_from_file(
                    file_key, target_table, source, derived_column=derived_column
                )
                self.logger.info(f"Table '{target_table}' created successfully.")
            else:
                self.logger.info(f"Table '{target_table}' exists.")

            schema = self.get_clickhouse_table_schema(target_table)
            columns = list(schema.keys())

            if derived_column and derived_column not in columns:
                raise ValueError(
                    f"Derived column '{derived_column}' not found in table schema. "
                    f"Available columns: {columns}"
                )

            if source.lower() == "s3":
                table_function = self._create_s3_table_function(file_key, format)
            elif source.lower() == "gcs":
                table_function = self._create_gcs_table_function(file_key, format)
            else:
                raise ValueError(f"Unsupported source: {source}. Must be 's3' or 'gcs'")

            if load_type.lower() == "incremental":
                return self._perform_incremental_load(
                    table_function, target_table, columns, derived_column, source
                )
            elif load_type.lower() == "special":
                return self._perform_incremental_load_special(
                    table_function, target_table, columns, derived_column, source
                )
            elif load_type.lower() == "full":
                return self._perform_full_load(
                    table_function, target_table, columns, derived_column, source
                )
            elif load_type.lower() == "snapshot":
                if not derived_column:
                    raise ValueError(
                        "Snapshot loads require a 'derived_column' parameter (e.g., 'snapshot_date') "
                        "to ensure idempotency."
                    )
                return self._perform_snapshot_load(
                    table_function, target_table, columns, derived_column, source
                )
            else:
                raise ValueError(
                    f"Unsupported load type: {load_type}. Must be 'incremental', 'full', or 'snapshot'"
                )

        except Exception as e:
            self.logger.error(
                f"Failed to load data to ClickHouse from {source.upper()}: {str(e)}"
            )
            raise

    def _create_s3_table_function(self, s3_key: str, format: str) -> str:
        s3_access_key = self.s3_access_key
        s3_secret_key = self.s3_secret_key
        s3_bucket = self.s3_bucket

        if self.s3_endpoint:
            s3_uri = f"{self.s3_endpoint}/{self.s3_bucket}/{s3_key}"
        else:
            s3_uri = f"https://{s3_bucket}.s3.eu-west-1.amazonaws.com/{s3_key}"

        self.logger.info(f"Using S3 URL: {s3_uri}")

        return f"s3('{s3_uri}','{s3_access_key}','{s3_secret_key}','{format}')"

    def _perform_incremental_load(
        self,
        table_function: str,
        table_name: str,
        columns: List[str],
        derived_column: str = None,
        source: str = None,
    ) -> int:
        temp_table = f"temp_{table_name}_{uuid.uuid4().hex[:8]}"
        create_temp_table_query = (
            f"CREATE TABLE {temp_table} AS {table_name} ENGINE = Memory"
        )
        self.clickhouse_client.command(create_temp_table_query)

        try:
            columns_str = ", ".join(columns)

            select_columns = []
            for col in columns:
                if derived_column and col == derived_column:
                    select_columns.append(f"today() as {derived_column}")
                elif col in columns:
                    select_columns.append(col)

            select_columns_str = ", ".join(select_columns)

            insert_query = f"""
            INSERT INTO {temp_table} ({columns_str})
            SELECT {select_columns_str} FROM {table_function}
            """
            self.logger.info(f"Insert query: {insert_query}")
            self.clickhouse_client.command(insert_query)

            count_query = f"SELECT count() FROM {temp_table}"
            result = self.clickhouse_client.query(count_query)
            row_count = result.result_rows[0][0]

            if row_count == 0:
                self.logger.info("No data to load")
                return 0

            if self.upsert_key:
                delete_query = f"""
                DELETE FROM {table_name} WHERE {self.upsert_key} IN 
                (SELECT {self.upsert_key} FROM {temp_table})
                """
                self.clickhouse_client.command(delete_query)
            else:
                self.logger.warning("No upsert key specified, skipping upsert")

            insert_from_temp_query = f"""
            INSERT INTO {table_name} ({columns_str})
            SELECT {columns_str} FROM {temp_table}
            """
            self.clickhouse_client.command(insert_from_temp_query)

            self.logger.info(
                f"Successfully loaded {row_count} rows incrementally into {table_name} from {source.upper()}"
            )
            return row_count

        finally:
            drop_temp_table_query = f"DROP TABLE IF EXISTS {temp_table}"
            self.clickhouse_client.command(drop_temp_table_query)

    def _perform_incremental_load_special(
        self,
        table_function: str,
        table_name: str,
        columns: List[str],
        derived_column: str = None,
        source: str = None,
    ) -> int:
        temp_table = f"temp_{table_name}_{uuid.uuid4().hex[:8]}"
        create_temp_table_query = (
            f"CREATE TABLE {temp_table} AS {table_name} ENGINE = Memory"
        )
        self.clickhouse_client.command(create_temp_table_query)

        try:
            columns_str = ", ".join(columns)

            select_columns = []
            for col in columns:
                if derived_column and col == derived_column:
                    select_columns.append(f"today() as {derived_column}")
                elif col in columns:
                    select_columns.append(col)

            select_columns_str = ", ".join(select_columns)

            insert_query = f"""
            INSERT INTO {temp_table} ({columns_str})
            SELECT {select_columns_str} FROM {table_function}
            """
            self.logger.info(f"Insert query: {insert_query}")
            self.clickhouse_client.command(insert_query)

            count_query = f"SELECT count() FROM {temp_table}"
            result = self.clickhouse_client.query(count_query)
            temp_row_count = result.result_rows[0][0]

            if temp_row_count == 0:
                self.logger.info("No data to load")
                return 0

            if self.upsert_key:
                delete_query = f"""
                DELETE FROM {table_name} WHERE {self.upsert_key} IN 
                (SELECT {self.upsert_key} FROM {temp_table})
                """
                self.clickhouse_client.command(delete_query)
            else:
                self.logger.warning("No upsert key specified, skipping upsert")

            insert_from_temp_query = f"""
            INSERT INTO {table_name} ({columns_str})
            SELECT {columns_str} FROM {temp_table}
            """
            self.clickhouse_client.command(insert_from_temp_query)
            self.logger.info(f"Inserted {temp_row_count} rows into {table_name}")

            final_row_count = temp_row_count

            if (
                self.upsert_key
                and self.tracking_column
                and self.upsert_key in columns
                and self.tracking_column in columns
            ):
                self.logger.info(
                    f"Starting deduplication: keeping latest {self.tracking_column} per {self.upsert_key}"
                )

                duplicate_check_query = f"""
                SELECT count() as duplicate_groups
                FROM (
                    SELECT {self.upsert_key}, count() as cnt
                    FROM {table_name}
                    GROUP BY {self.upsert_key}
                    HAVING cnt > 1
                ) duplicates
                """

                result = self.clickhouse_client.query(duplicate_check_query)
                duplicate_groups = result.result_rows[0][0]

                if duplicate_groups > 0:
                    self.logger.info(
                        f"Found {duplicate_groups} groups with duplicates, cleaning up..."
                    )

                    dedup_delete_query = f"""
                    DELETE FROM {table_name}
                    WHERE ({self.upsert_key}, {self.tracking_column}) NOT IN (
                        SELECT {self.upsert_key}, MAX({self.tracking_column})
                        FROM {table_name}
                        GROUP BY {self.upsert_key}
                    )
                    """

                    self.clickhouse_client.command(dedup_delete_query)

                    final_count_query = f"SELECT count() FROM {table_name}"
                    result = self.clickhouse_client.query(final_count_query)
                    final_row_count = result.result_rows[0][0]

                    duplicates_removed = temp_row_count - final_row_count
                    self.logger.info(f"Removed {duplicates_removed} duplicate records")
                    self.logger.info(f"Final state: {final_row_count} unique records")
                else:
                    self.logger.info("No duplicates found")
            else:
                missing_requirements = []
                if not self.upsert_key:
                    missing_requirements.append("upsert_key not configured")
                elif self.upsert_key not in columns:
                    missing_requirements.append(
                        f"upsert_key '{self.upsert_key}' not in table columns"
                    )

                if not self.tracking_column:
                    missing_requirements.append("tracking_column not configured")
                elif self.tracking_column not in columns:
                    missing_requirements.append(
                        f"tracking_column '{self.tracking_column}' not in table columns"
                    )

                self.logger.info(
                    f"Skipping deduplication: {', '.join(missing_requirements)}"
                )

            source_name = source.upper() if source else "SOURCE"
            self.logger.info(
                f"Successfully loaded {final_row_count} rows incrementally into {table_name} from {source_name}"
            )
            return final_row_count

        finally:
            drop_temp_table_query = f"DROP TABLE IF EXISTS {temp_table}"
            self.clickhouse_client.command(drop_temp_table_query)

    def _perform_full_load(
        self,
        table_function: str,
        table_name: str,
        columns: List[str],
        derived_column: str = None,
        source: str = None,
    ) -> int:
        count_query = f"SELECT count() FROM {table_function}"
        result = self.clickhouse_client.query(count_query)
        row_count = result.result_rows[0][0]

        if row_count == 0:
            self.logger.info("No data to load")
            return 0

        truncate_query = f"TRUNCATE TABLE {table_name}"
        self.clickhouse_client.command(truncate_query)

        columns_str = ", ".join(columns)

        select_columns = []
        for col in columns:
            if derived_column and col == derived_column:
                select_columns.append(f"today() as {derived_column}")
            elif col in columns:
                select_columns.append(col)

        select_columns_str = ", ".join(select_columns)

        insert_query = f"""
        INSERT INTO {table_name} ({columns_str})
        SELECT {select_columns_str} FROM {table_function}
        """
        self.clickhouse_client.command(insert_query)

        self.logger.info(
            f"Successfully loaded {row_count} rows into {table_name} (full load) from {source.upper()}"
        )
        return row_count

    def _perform_snapshot_load(
        self,
        table_function: str,
        table_name: str,
        columns: List[str],
        derived_column: str = None,
        source: str = None,
    ) -> int:
        if not derived_column:
            self.logger.warning(
                "Snapshot load requires a derived_column parameter to ensure idempotency"
            )
            raise ValueError("derived_column must be specified for snapshot loads")

        temp_table = f"temp_{table_name}_{uuid.uuid4().hex[:8]}"
        create_temp_table_query = (
            f"CREATE TABLE {temp_table} AS {table_name} ENGINE = Memory"
        )
        self.clickhouse_client.command(create_temp_table_query)

        try:
            columns_str = ", ".join(columns)

            select_columns = []
            for col in columns:
                if col == derived_column:
                    select_columns.append(f"today() as {derived_column}")
                else:
                    select_columns.append(col)

            select_columns_str = ", ".join(select_columns)

            insert_query = f"""
            INSERT INTO {temp_table} ({columns_str})
            SELECT {select_columns_str} FROM {table_function}
            """
            self.logger.info(f"Insert query: {insert_query}")
            self.clickhouse_client.command(insert_query)

            count_query = f"SELECT count() FROM {temp_table}"
            result = self.clickhouse_client.query(count_query)
            row_count = result.result_rows[0][0]

            if row_count == 0:
                self.logger.info("No data to load")
                return 0

            delete_query = f"""
            ALTER TABLE {table_name} DELETE WHERE {derived_column} = today()
            """
            self.clickhouse_client.command(delete_query)
            self.logger.info(f"Deleted existing records for today from {table_name}")

            insert_from_temp_query = f"""
            INSERT INTO {table_name} ({columns_str})
            SELECT {columns_str} FROM {temp_table}
            """
            self.clickhouse_client.command(insert_from_temp_query)

            self.logger.info(
                f"Successfully loaded {row_count} rows as snapshot into {table_name} from {source.upper()}"
            )
            return row_count

        finally:
            drop_temp_table_query = f"DROP TABLE IF EXISTS {temp_table}"
            self.clickhouse_client.command(drop_temp_table_query)

    def get_last_loaded_value(self, table_name: str) -> Optional[str]:
        if not self.clickhouse_client:
            self.connect_clickhouse()

        if not self.table_exists(table_name):
            self.logger.warning(
                f"Table {table_name} does not exist yet. Fresh load — no last value."
            )
            return None

        try:
            query = (
                f"SELECT MAX({self.tracking_column}) as last_value FROM {table_name}"
            )
            result = self.clickhouse_client.query(query)

            if result.row_count > 0:
                last_value = result.first_row[0]
                return last_value
            return None
        except Exception as e:
            self.logger.error(f"Failed to get last loaded value: {str(e)}")
            raise

    def close_connections(self) -> None:
        if self.clickhouse_client:
            try:
                self.clickhouse_client.close()
                self.logger.info("Closed ClickHouse connection")
            except Exception as e:
                self.logger.error(f"Error closing ClickHouse connection: {str(e)}")

    @abstractmethod
    def extract_data(
        self,
        source_table: str,
        last_value: Optional[str] = None,
        source_schema: str = None,
    ) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def connect_source(self) -> None:
        pass

    def extract_to_storage(
        self,
        source_table: str,
        target_table: str,
        destination: str = "s3",
        source_schema: str = None,
        load_type: str = "incremental",
        output_key: str = None,
        **kwargs,
    ) -> str:
        try:
            self.logger.info(
                f"Starting {load_type} extraction from {source_table} to {destination.upper()}"
            )

            if destination.lower() not in ["s3", "gcs"]:
                raise ValueError(
                    f"Unsupported destination: {destination}. Must be 's3' or 'gcs'"
                )

            self.connect_source()

            last_value = None
            if load_type.lower() in ["incremental", "special"]:
                self.connect_clickhouse()
                last_value = self.get_last_loaded_value(target_table)
                self.logger.info(
                    f"Last loaded value of {self.tracking_column}: {last_value}"
                )

            data_generator = self.extract_data(
                source_table, last_value, source_schema, **kwargs
            )

            if output_key:
                storage_key = output_key
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                storage_key = (
                    f"{load_type}/{source_table}_to_{target_table}_{timestamp}.json"
                )

            if destination.lower() == "s3":
                storage_key = self.upload_to_s3(data_generator, storage_key)
                self.logger.info(
                    f"Data extracted to S3: s3://{self.s3_bucket}/{storage_key}"
                )
            elif destination.lower() == "gcs":
                storage_key = self.upload_to_gcs(data_generator, storage_key)
                self.logger.info(
                    f"Data extracted to GCS: gs://{self.gcs_bucket}/{storage_key}"
                )

            return storage_key

        except Exception as e:
            self.logger.error(
                f"Error during data extraction to {destination.upper()}: {str(e)}"
            )
            raise
        finally:
            self.close_connections()

    def extract_to_s3(
        self,
        source_table: str,
        target_table: str,
        source_schema: str = None,
        load_type: str = "incremental",
        output_s3_key: str = None,
        **kwargs,
    ) -> str:
        return self.extract_to_storage(
            source_table=source_table,
            target_table=target_table,
            destination="s3",
            source_schema=source_schema,
            load_type=load_type,
            output_key=output_s3_key,
            **kwargs,
        )

    def _configure_clickhouse_json_settings(self):
        try:
            self.clickhouse_client.command(
                "SET input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects = 1"
            )
            self.clickhouse_client.command("SET input_format_skip_unknown_fields = 1")
            self.logger.info("Applied ClickHouse JSON settings")
        except Exception as e:
            self.logger.warning(f"Could not apply ClickHouse JSON settings: {e}")

    def table_exists(self, table_name: str) -> bool:
        self._configure_clickhouse_json_settings()

        query = f"""
        SELECT count() 
        FROM system.tables 
        WHERE database = '{self.clickhouse_database}' 
          AND name = '{table_name}'
        """

        result = self.clickhouse_client.query(query)
        count = result.result_rows[0][0]

        return count > 0

    def _create_table_from_file(
        self,
        file_key: str,
        target_table: str,
        source: str = "s3",
        derived_column: str = None,
    ) -> None:
        try:
            self._configure_clickhouse_json_settings()

            if source == "s3":
                if not self.s3_client:
                    self.connect_s3()
                response = self.s3_client.get_object(
                    Bucket=self.s3_bucket, Key=file_key
                )
                content = response["Body"].read().decode("utf-8")
            else:
                if not self.gcs_storage_client:
                    self.connect_to_gcs()
                bucket = self.gcs_storage_client.bucket(self.gcs_bucket)
                blob = bucket.blob(file_key)
                content = blob.download_as_text()

            lines = content.strip().split("\n")
            sample_data = []
            for line in lines[:1000]:
                if line.strip():
                    try:
                        parsed = json.loads(line)
                        if isinstance(parsed, dict):
                            sample_data.append(parsed)
                        elif isinstance(parsed, list):
                            sample_data.extend(
                                [item for item in parsed if isinstance(item, dict)]
                            )
                    except json.JSONDecodeError:
                        continue

            if not sample_data:
                raise ValueError("No valid sample data found for schema inference.")

            schema = {}
            all_columns = set()
            for row in sample_data:
                all_columns.update(row.keys())

            for col in all_columns:
                values = [
                    row.get(col) for row in sample_data if row.get(col) is not None
                ]
                if not values:
                    schema[col] = "Nullable(String)"
                    continue

                value_types = set()
                for val in values[:100]:
                    if isinstance(val, dict):
                        value_types.add("object")
                    elif isinstance(val, list):
                        value_types.add("array")
                    elif isinstance(val, str):
                        value_types.add("string")
                    elif isinstance(val, bool):
                        value_types.add("bool")
                    elif isinstance(val, int):
                        value_types.add("int")
                    elif isinstance(val, float):
                        value_types.add("float")

                if len(value_types) > 1:
                    self.logger.warning(
                        f"Column '{col}' has mixed types. Using String type."
                    )
                    schema[col] = "String"
                    continue

                datetime_count = 0
                date_count = 0
                for val in values[:20]:
                    if isinstance(val, str):
                        if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", val):
                            datetime_count += 1
                        elif re.match(r"^\d{4}-\d{2}-\d{2}$", val):
                            date_count += 1

                total_checked = min(20, len(values))
                if total_checked and datetime_count / total_checked > 0.8:
                    schema[col] = "DateTime('UTC')"
                elif total_checked and date_count / total_checked > 0.8:
                    schema[col] = "Date"
                else:
                    sample = values[0]
                    if isinstance(sample, bool):
                        schema[col] = "Bool"
                    elif isinstance(sample, int):
                        schema[col] = "Int64"
                    elif isinstance(sample, float):
                        schema[col] = "Float64"
                    elif isinstance(sample, (dict, list)):
                        schema[col] = "String"
                    else:
                        schema[col] = "String"

            if derived_column:
                self.logger.info(f"Adding derived column '{derived_column}' to schema")
                schema[derived_column] = "Date"

            columns_ddl = [f"`{k}` {v}" for k, v in schema.items()]
            ddl = f"""
            CREATE TABLE {self.clickhouse_database}.{target_table} (
                {', '.join(columns_ddl)}
            )
            ENGINE = MergeTree()
            ORDER BY tuple()
            """

            self.clickhouse_client.command(ddl)
            self.logger.info(
                f"Successfully created table {target_table}"
                + (
                    f" (with derived column '{derived_column}')"
                    if derived_column
                    else ""
                )
            )

        except Exception as e:
            self.logger.error(f"Failed to create table from file {file_key}: {str(e)}")
            raise

    def _generate_create_table_ddl(
        self, table_name: str, schema: Dict[str, str]
    ) -> str:
        columns_ddl = []
        for column_name, column_type in schema.items():
            columns_ddl.append(f"    `{column_name}` {column_type}")

        columns_str = ",\n".join(columns_ddl)

        ddl = (
            f"CREATE TABLE {self.clickhouse_database}.{table_name} (\n"
            f"{columns_str}\n"
            ") ENGINE = MergeTree()\n"
            "ORDER BY tuple()"
        )

        self.logger.info(f"Generated DDL: {ddl}")
        return ddl
