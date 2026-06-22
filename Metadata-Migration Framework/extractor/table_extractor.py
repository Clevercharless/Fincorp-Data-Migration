# ==========================================
# Extractors/table_extractor.py
# ==========================================

from pyspark.sql import Row
from datetime import datetime

%run ../Common/config
%run ../Common/constants
%run ../Common/logger
%run ../Common/storage_manager
%run ../Common/retry_manager
%run ../Common/checkpoint_manager
%run ../Common/status_manager
%run ../Common.batch_manager
%run ../Common.framework_metrics
%run ../Common.audit_manager


class TableExtractor:

    def __init__(self):

        self.logger = Logger()

        self.storage = StorageManager()

        self.checkpoint = CheckpointManager(
            self.storage
        )

        self.status = StatusManager(
            self.storage
        )

        self.metrics = FrameworkMetrics()

        self.audit = AuditManager(
            self.storage
        )

    def extract(self):

        schemas = self.storage.load_delta(
            SCHEMA_PATH
        )

        completed = (
            self.checkpoint.load_completed(
                TABLE_STATUS_PATH
            )
        )

        failed_rows = []

        batch_number = 1

        total_batches = (

            BatchManager.get_batch_count(

                schemas.count(),

                BATCH_SIZE

            )

        )

        for batch in BatchManager.get_batches(

            schemas,

            BATCH_SIZE

        ):

            table_rows = []

            BatchManager.print_progress(

                batch_number,

                total_batches

            )

            for schema in batch:

                catalog_name = schema.catalog

                schema_name = schema.schema

                try:

                    tables = RetryManager.execute(

                        lambda:

                        spark.sql(
                            f"""
                            SHOW TABLES IN
                            {catalog_name}.
                            {schema_name}
                            """
                        ).collect()

                    )

                    for table in tables:

                        full_name = (

                            f"{catalog_name}."
                            f"{schema_name}."
                            f"{table.tableName}"

                        )

                        if full_name in completed:

                            self.metrics.increment_skipped()

                            continue

                        self.metrics.increment_processed()

                        detail = RetryManager.execute(

                            lambda:

                            spark.sql(
                                f"""
                                DESCRIBE DETAIL
                                {full_name}
                                """
                            ).first()

                        )

                        table_rows.append(

                            Row(

                                catalog=
                                    catalog_name,

                                schema=
                                    schema_name,

                                table=
                                    table.tableName,

                                table_type=
                                    detail["type"],

                                format=
                                    detail["format"],

                                location=
                                    detail["location"],

                                owner=
                                    detail["owner"],

                                comment=
                                    detail["description"],

                                extracted_time=
                                    datetime.now()

                            )

                        )

                        self.status.write_status(

                            full_name,

                            "TABLE_EXTRACTION",

                            SUCCESS,

                            TABLE_STATUS_PATH

                        )

                        self.checkpoint.mark_complete(

                            full_name,

                            TABLE_STATUS_PATH

                        )

                        self.audit.write_audit(

                            METADATA,

                            "TABLE_EXTRACTION",

                            full_name,

                            SUCCESS

                        )

                        self.metrics.increment_success()

                except Exception as e:

                    failed_rows.append(

                        Row(

                            object_name=
                                full_name,

                            error=
                                str(e)

                        )

                    )

                    self.metrics.increment_failed()

            if table_rows:

                self.storage.save_delta(

                    spark.createDataFrame(
                        table_rows
                    ),

                    TABLE_PATH,

                    mode="append"

                )

            batch_number += 1

        if failed_rows:

            self.storage.save_delta(

                spark.createDataFrame(
                    failed_rows
                ),

                FAILED_TABLE_PATH

            )

        self.metrics.save_metrics(

            "TABLE_EXTRACTION",

            self.storage,

            METADATA_METRICS_PATH

        )
