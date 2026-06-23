# ==========================================
# historical_data_reconciliation.py
# ==========================================

from pyspark.sql.functions import *

%run ./base_reconciliation


class HistoricalDataReconciliation(
    BaseReconciliation
):

    def __init__(self):

        super().__init__(
            "HISTORICAL_RECONCILIATION"
        )

    def reconcile(self):

        report = (

            self.storage.load_delta(

                HISTORICAL_VALIDATION_REPORT_PATH

            )

        )

        total_checks = (

            report

            .agg(
                sum(
                    "total_checks"
                )
            )

            .first()[0]

        )

        passed_checks = (

            report

            .agg(
                sum(
                    "passed_checks"
                )
            )

            .first()[0]

        )

        failed_checks = (

            report

            .agg(
                sum(
                    "failed_checks"
                )
            )

            .first()[0]

        )

        self.add_result(

            "HISTORICAL_DATA",

            total_checks,

            passed_checks,

            SUCCESS
            if failed_checks == 0
            else FAILED,

            f"Failed Checks={failed_checks}"

        )

        self.save()
