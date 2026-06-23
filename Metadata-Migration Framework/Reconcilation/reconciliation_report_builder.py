# ==========================================
# reconciliation_report_builder.py
# ==========================================

from pyspark.sql.functions import *

%run ../../Common/config
%run ../../Common/storage_manager


class ReconciliationReportBuilder:

    def __init__(self):

        self.storage = StorageManager()

    def build(self):

        reconciliation = (

            self.storage.load_delta(
                DATA_RECONCILIATION_PATH
            )

        )

        report = (

            reconciliation

            .groupBy(
                "reconciliation_type"
            )

            .agg(

                count("*")
                .alias(
                    "total_items"
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
                    "passed_items"
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
                    "failed_items"
                )

            )

        )

        self.storage.save_delta(

            report,

            DATA_RECONCILIATION_REPORT_PATH

        )
