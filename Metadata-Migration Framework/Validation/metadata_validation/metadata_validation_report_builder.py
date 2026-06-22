# ==========================================
# metadata_validation_report_builder.py
# ==========================================

from pyspark.sql.functions import *

%run ../../Common/config
%run ../../Common/logger
%run ../../Common/storage_manager


class MetadataValidationReportBuilder:

    def __init__(self):

        self.logger = Logger()

        self.storage = StorageManager()

    def build(self):

        self.logger.info(
            "Building Metadata Validation Report"
        )

        validation_df = (

            self.storage.load_delta(
                VALIDATION_RESULTS_PATH
            )

        )

        report_df = (

            validation_df

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

            .withColumn(

                "pass_percentage",

                round(

                    (
                        col(
                            "passed_checks"
                        )

                        /

                        col(
                            "total_checks"
                        )

                    ) * 100,

                    2

                )

            )

        )

        self.storage.save_delta(

            report_df,

            METADATA_VALIDATION_REPORT_PATH

        )

        self.logger.info(
            "Metadata Validation Report Created"
        )
