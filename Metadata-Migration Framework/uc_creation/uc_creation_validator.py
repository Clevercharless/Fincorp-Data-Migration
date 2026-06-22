# ==========================================
# UC_Creation/uc_creation_validator.py
# ==========================================

from pyspark.sql import Row
from datetime import datetime

%run ../Common/config
%run ../Common/constants
%run ../Common/logger
%run ../Common/storage_manager
%run ../Common/retry_manager
%run ../Common/status_manager
%run ../Common/framework_metrics
%run ../Common/audit_manager


class UCCreationValidator:

    def __init__(self):

        self.logger = Logger()

        self.storage = StorageManager()

        self.status = StatusManager(
            self.storage
        )

        self.metrics = FrameworkMetrics()

        self.audit = AuditManager(
            self.storage
        )

    def validate(self):

        self.logger.info(
            "UC Validation Started"
        )

        manifest = self.storage.load_delta(
            MIGRATION_BATCH_PATH
        )

        validation_rows = []

        failed_rows = []

        for table in manifest.toLocalIterator():

            manifest_id = table.manifest_id

            try:

                full_name = (

                    f"{table.catalog}."

                    f"{table.schema}."

                    f"{table.table}"

                )

                exists = spark.catalog.tableExists(
                    full_name
                )

                validation_rows.append(

                    Row(

                        manifest_id=
                            manifest_id,

                        object_type=
                            "TABLE",

                        object_name=
                            full_name,

                        validation_status=
                            "SUCCESS"
                            if exists
                            else "FAILED",

                        validation_time=
                            datetime.now()

                    )

                )

                if exists:

                    self.metrics.increment_success()

                else:

                    self.metrics.increment_failed()

            except Exception as e:

                failed_rows.append(

                    Row(

                        manifest_id=
                            manifest_id,

                        error=
                            str(e)

                    )

                )

                self.metrics.increment_failed()

        self.storage.save_delta(

            spark.createDataFrame(
                validation_rows
            ),

            UC_VALIDATION_PATH

        )

        if failed_rows:

            self.storage.save_delta(

                spark.createDataFrame(
                    failed_rows
                ),

                FAILED_UC_VALIDATION_PATH,

                mode="append"

            )

        self.audit.write_audit(

            UC_CREATION,

            "UC_VALIDATION",

            "ALL_OBJECTS",

            SUCCESS

        )

        self.metrics.save_metrics(

            "UC_VALIDATION",

            self.storage,

            UC_METRICS_PATH

        )

        self.logger.info(
            "UC Validation Completed"
        )
