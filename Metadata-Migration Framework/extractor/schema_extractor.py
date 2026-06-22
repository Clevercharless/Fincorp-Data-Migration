# ==========================================
# Extractors/schema_extractor.py
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
%run ../Common/framework_metrics
%run ../Common/audit_manager


class SchemaExtractor:

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

        catalogs = self.storage.load_delta(
            CATALOG_PATH
        )

        completed = (
            self.checkpoint.load_completed(
                SCHEMA_STATUS_PATH
            )
        )

        schema_rows = []

        failed_rows = []

        for catalog in catalogs.collect():

            catalog_name = catalog.catalog

            try:

                schemas = RetryManager.execute(

                    lambda:

                    spark.sql(
                        f"""
                        SHOW SCHEMAS IN
                        {catalog_name}
                        """
                    ).collect()

                )

                for schema in schemas:

                    schema_name = (
                        schema.namespace
                    )

                    key = (
                        f"{catalog_name}."
                        f"{schema_name}"
                    )

                    if key in completed:

                        self.metrics.increment_skipped()

                        continue

                    self.metrics.increment_processed()

                    schema_rows.append(

                        Row(

                            catalog=
                                catalog_name,

                            schema=
                                schema_name,

                            extracted_time=
                                datetime.now()

                        )

                    )

                    self.status.write_status(

                        key,

                        "SCHEMA_EXTRACTION",

                        SUCCESS,

                        SCHEMA_STATUS_PATH

                    )

                    self.checkpoint.mark_complete(

                        key,

                        SCHEMA_STATUS_PATH

                    )

                    self.audit.write_audit(

                        METADATA,

                        "SCHEMA_EXTRACTION",

                        key,

                        SUCCESS

                    )

                    self.metrics.increment_success()

            except Exception as e:

                failed_rows.append(

                    Row(

                        object_name=
                            catalog_name,

                        error=
                            str(e)

                    )

                )

                self.metrics.increment_failed()

        self.storage.save_delta(

            spark.createDataFrame(
                schema_rows
            ),

            SCHEMA_PATH

        )

        self.metrics.save_metrics(

            "SCHEMA_EXTRACTION",

            self.storage,

            METADATA_METRICS_PATH

        )
