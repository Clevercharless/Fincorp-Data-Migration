# ==========================================
# UC_Creation/schema_creator.py
# ==========================================

from pyspark.sql import Row

%run ../Common/config
%run ../Common/constants
%run ../Common/logger
%run ../Common/storage_manager
%run ../Common/retry_manager
%run ../Common/checkpoint_manager
%run ../Common/status_manager
%run ../Common/framework_metrics
%run ../Common.audit_manager


class SchemaCreator:

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

    def create(self):

        self.logger.info(
            "Schema Creation Started"
        )

        schemas_df = self.storage.load_delta(
            SCHEMA_PATH
        )

        completed = (
            self.checkpoint.load_completed(
                SCHEMA_CREATION_CHECKPOINT_PATH
            )
        )

        failed_rows = []

        try:

            for row in schemas_df.toLocalIterator():

                catalog_name = row.catalog

                schema_name = row.schema

                object_key = (
                    f"{catalog_name}."
                    f"{schema_name}"
                )

                if object_key in completed:

                    self.metrics.increment_skipped()

                    continue

                try:

                    self.metrics.increment_processed()

                    RetryManager.execute(

                        lambda:

                        spark.sql(

                            f"""
                            CREATE SCHEMA IF NOT EXISTS
                            {catalog_name}.{schema_name}
                            """

                        )

                    )

                    self.status.write_status(

                        object_key,

                        "SCHEMA_CREATION",

                        SUCCESS,

                        SCHEMA_CREATION_STATUS_PATH

                    )

                    self.checkpoint.mark_complete(

                        object_key,

                        SCHEMA_CREATION_CHECKPOINT_PATH

                    )

                    self.audit.write_audit(

                        UC_CREATION,

                        "SCHEMA_CREATION",

                        object_key,

                        SUCCESS

                    )

                    self.metrics.increment_success()

                except Exception as e:

                    failed_rows.append(

                        Row(

                            object_name=
                                object_key,

                            error=
                                str(e)

                        )

                    )

                    self.audit.write_audit(

                        UC_CREATION,

                        "SCHEMA_CREATION",

                        object_key,

                        FAILED,

                        str(e)

                    )

                    self.metrics.increment_failed()

            if failed_rows:

                self.storage.save_delta(

                    spark.createDataFrame(
                        failed_rows
                    ),

                    FAILED_SCHEMA_CREATION_PATH,

                    mode="append"

                )

        finally:

            self.metrics.save_metrics(

                "SCHEMA_CREATION",

                self.storage,

                UC_METRICS_PATH

            )

        self.logger.info(
            "Schema Creation Completed"
        )
