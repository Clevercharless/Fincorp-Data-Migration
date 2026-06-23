# ==========================================
# migration_reconciliation_builder.py
# ==========================================

from pyspark.sql import Row

%run ../Common/config
%run ../Common/logger
%run ../Common/storage_manager


class MigrationReconciliationBuilder:

    def __init__(self):

        self.logger = Logger()

        self.storage = StorageManager()

    def build(self):

        self.logger.info(
            "Building Migration Reconciliation"
        )

        manifest = self.storage.load_delta(
            MIGRATION_BATCH_PATH
        )

        executed = self.storage.load_delta(
            DATASYNC_EXECUTION_PATH
        )

        failures = self.storage.load_delta(
            FAILED_EXECUTION_PATH
        )

        source_count = (
            manifest.count()
        )

        migrated_count = (

            executed

            .agg(
                sum(
                    "table_count"
                )
            )

            .first()[0]

        ) or 0

        failed_count = (

            failures

            .agg(
                sum(
                    "table_count"
                )
            )

            .first()[0]

        ) or 0

        missing_count = (

            source_count

            - migrated_count

            - failed_count

        )

        reconciliation = spark.createDataFrame(

            [

                Row(

                    source_count=
                        source_count,

                    migrated_count=
                        migrated_count,

                    failed_count=
                        failed_count,

                    missing_count=
                        missing_count,

                    status=
                        "PASS"
                        if missing_count == 0
                        else "FAIL"

                )

            ]

        )

        self.storage.save_delta(

            reconciliation,

            MIGRATION_RECONCILIATION_PATH

        )

        self.logger.info(
            "Migration Reconciliation Built"
        )
