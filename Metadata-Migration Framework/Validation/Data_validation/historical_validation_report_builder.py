# ==========================================
# historical_validation_report_builder.py
# ==========================================

from pyspark.sql.functions import *

%run ../../Common/config
%run ../../Common/logger
%run ../../Common/storage_manager


class HistoricalValidationReportBuilder:

    def __init__(self):

        self.logger = Logger()

        self.storage = StorageManager()

    def build(self):

        results = self.storage.load_delta(
            HISTORICAL_VALIDATION_RESULTS_PATH
        )

        report = (

            results

            .groupBy(
                "validation_type"
            )

            .agg(

                count("*")
                .alias(
                    "total_checks"
                ),

                sum(

                    when(
                        col("status")
                        == SUCCESS,
                        1
                    )

                    .otherwise(0)

                )

                .alias(
                    "passed_checks"
                ),

                sum(

                    when(
                        col("status")
                        == FAILED,
                        1
                    )

                    .otherwise(0)

                )

                .alias(
                    "failed_checks"
                )

            )

        )

        self.storage.save_delta(

            report,

            HISTORICAL_VALIDATION_REPORT_PATH

        )
