# ==========================================
# historical_reconciliation_builder.py
# ==========================================

from pyspark.sql import Row

%run ../../Common/config
%run ../../Common/logger
%run ../../Common/storage_manager


class HistoricalReconciliationBuilder:

    def __init__(self):

        self.logger = Logger()

        self.storage = StorageManager()

    def build(self):

        report = self.storage.load_delta(
            HISTORICAL_VALIDATION_REPORT_PATH
        )

        total = (

            report

            .agg(
                sum(
                    "total_checks"
                )
            )

            .first()[0]

        )

        passed = (

            report

            .agg(
                sum(
                    "passed_checks"
                )
            )

            .first()[0]

        )

        failed = (

            report

            .agg(
                sum(
                    "failed_checks"
                )
            )

            .first()[0]

        )

        reconciliation = spark.createDataFrame(

            [

                Row(

                    total_checks=total,

                    passed_checks=passed,

                    failed_checks=failed,

                    pass_percentage=
                        round(
                            (
                                passed
                                /
                                total
                            ) * 100,
                            2
                        ),

                    status=
                        "PASS"
                        if failed == 0
                        else "FAIL"

                )

            ]

        )

        self.storage.save_delta(

            reconciliation,

            HISTORICAL_RECONCILIATION_PATH

        )
