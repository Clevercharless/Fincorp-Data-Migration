# ==========================================
# Extractors/catalog_extractor.py
# ==========================================

from pyspark.sql import Row
from datetime import datetime

# Common Imports

%run ../Common/config
%run ../Common/constants
%run ../Common/logger
%run ../Common/storage_manager
%run ../Common/retry_manager
%run ../Common/checkpoint_manager
%run ../Common/status_manager
%run ../Common/framework_metrics
%run ../Common/audit_manager


class CatalogExtractor:

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

        self.logger.info(
            "Catalog Extraction Started"
        )

        completed = (
            self.checkpoint.load_completed(
                CATALOG_STATUS_PATH
            )
        )

        catalog_rows = []

        failed_rows = []

        try:

            catalogs = RetryManager.execute(

                lambda:

                spark.sql(
                    "SHOW CATALOGS"
                ).collect()

            )

            for catalog in catalogs:

                catalog_name = catalog.catalog

                if catalog_name in completed:

                    self.metrics.increment_skipped()

                    continue

                try:

                    self.metrics.increment_processed()

                    catalog_rows.append(

                        Row(

                            catalog=
                                catalog_name,

                            extracted_time=
                                datetime.now()

                        )

                    )

                    self.status.write_status(

                        catalog_name,

                        "CATALOG_EXTRACTION",

                        SUCCESS,

                        CATALOG_STATUS_PATH

                    )

                    self.checkpoint.mark_complete(

                        catalog_name,

                        CATALOG_STATUS_PATH

                    )

                    self.audit.write_audit(

                        METADATA,

                        "CATALOG_EXTRACTION",

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

                        METADATA,

                        "CATALOG_EXTRACTION",

                        catalog_name,

                        FAILED,

                        str(e)

                    )

                    self.metrics.increment_failed()

            if catalog_rows:

                self.storage.save_delta(

                    spark.createDataFrame(
                        catalog_rows
                    ),

                    CATALOG_PATH

                )

            if failed_rows:

                self.storage.save_delta(

                    spark.createDataFrame(
                        failed_rows
                    ),

                    FAILED_ROOT +
                    "/catalog_failures"

                )

        finally:

            self.metrics.save_metrics(

                "CATALOG_EXTRACTION",

                self.storage,

                METADATA_METRICS_PATH

            )

        self.logger.info(
            "Catalog Extraction Completed"
        )
