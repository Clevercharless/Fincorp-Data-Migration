# ==========================================
# migration_report_builder.py
# ==========================================

from pyspark.sql.functions import *

%run ../Common/config
%run ../Common/logger
%run ../Common/storage_manager


class MigrationReportBuilder:

    def __init__(self):

        self.logger = Logger()

        self.storage = StorageManager()

    def build(self):

        self.logger.info(
            "Building Migration Report"
        )

        batches = self.storage.load_delta(
            HISTORICAL_BATCH_PATH
        )

        executions = self.storage.load_delta(
            DATASYNC_EXECUTION_PATH
        )

        failures = self.storage.load_delta(
            FAILED_EXECUTION_PATH
        )

        layer_summary = (

            executions

            .groupBy(
                "layer"
            )

            .agg(

                countDistinct(
                    "historical_batch"
                ).alias(
                    "executed_batches"
                ),

                sum(
                    "table_count"
                ).alias(
                    "migrated_tables"
                ),

                sum(
                    "batch_size_gb"
                ).alias(
                    "migrated_size_gb"
                )

            )

        )

        failed_summary = (

            failures

            .groupBy(
                "layer"
            )

            .agg(

                countDistinct(
                    "batch_id"
                ).alias(
                    "failed_batches"
                )

            )

        )

        report = (

            layer_summary

            .join(

                failed_summary,

                ["layer"],

                "left"

            )

            .fillna(0)

        )

        self.storage.save_delta(

            report,

            MIGRATION_REPORT_PATH

        )

        self.logger.info(
            "Migration Report Created"
        )
