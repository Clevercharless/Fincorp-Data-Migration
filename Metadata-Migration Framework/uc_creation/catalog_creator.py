# ==========================================
# UC_Creation/catalog_creator.py
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


class CatalogCreator:

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
            "Catalog Creation Started"
        )

        catalogs_df = self.storage.load_delta(
            CATALOG_PATH
        )

        completed = (
            self.checkpoint.load_completed(
                CATALOG_CREATION_CHECKPOINT_PATH
            )
        )

        failed_rows = []

        try:

            for row in catalogs_df.toLocalIterator():

                catalog_name = row.catalog

                if catalog_name in completed:

                    self.metrics.increment_skipped()

                    continue

                try:

                    self.metrics.increment_processed()

                    RetryManager.execute(

                        lambda:

                        spark.sql(

                            f"""
                            CREATE CATALOG IF NOT EXISTS
                            {catalog_name}
                            """

                        )

                    )

                    self.status.write_status(

                        catalog_name,

                        "CATALOG_CREATION",

                        SUCCESS,

                        CATALOG_CREATION_STATUS_PATH

                    )

                    self.checkpoint.mark_complete(

                        catalog_name,

                        CATALOG_CREATION_CHECKPOINT_PATH

                    )

                    self.audit.write_audit(

                        UC_CREATION,

                        "CATALOG_CREATION",

                        catalog_name,

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

                    self.audit.write_audit(

                        UC_CREATION,

                        "CATALOG_CREATION",

                        catalog_name,

                        FAILED,

                        str(e)

                    )

                    self.metrics.increment_failed()

            if failed_rows:

                self.storage.save_delta(

                    spark.createDataFrame(
                        failed_rows
                    ),

                    FAILED_CATALOG_CREATION_PATH,

                    mode="append"

                )

        finally:

            self.metrics.save_metrics(

                "CATALOG_CREATION",

                self.storage,

                UC_METRICS_PATH

            )

        self.logger.info(
            "Catalog Creation Completed"
        )
