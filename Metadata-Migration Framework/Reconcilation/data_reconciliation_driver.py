# ==========================================
# data_reconciliation_driver.py
# ==========================================

%run ./historical_data_reconciliation

%run ./reconciliation_report_builder

%run ../../Common/logger
%run ../../Common/storage_manager


logger = Logger()

storage = StorageManager()

logger.info(
    "Data Reconciliation Started"
)

try:

    HistoricalDataReconciliation().reconcile()

    ReconciliationReportBuilder().build()

    report = (

        storage

        .load_delta(
            DATA_RECONCILIATION_REPORT_PATH
        )

    )

    failed = (

        report

        .agg(
            sum(
                "failed_items"
            )
        )

        .first()[0]

    )

    if failed > 0:

        raise Exception(

            "Data Reconciliation Failed"

        )

    logger.info(
        "Data Reconciliation Passed"
    )

except Exception as e:

    logger.error(
        str(e)
    )

    raise
