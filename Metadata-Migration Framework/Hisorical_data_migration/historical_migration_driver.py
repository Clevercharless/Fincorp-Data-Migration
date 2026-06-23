# ==========================================
# historical_migration_driver.py
# ==========================================

%run ./migration_batch_builder

%run ./migration_precheck

%run ./datasync_task_builder

%run ./datasync_executor

%run ./datasync_monitor

%run ./migration_failure_handler

%run ./migration_recovery

%run ./migration_report_builder

%run ./migration_reconciliation_builder

%run ../Common/logger


logger = Logger()

logger.info(
    "Historical Migration Started"
)

try:

    # --------------------------------------
    # Build Batches
    # --------------------------------------

    MigrationBatchBuilder().build()

    # --------------------------------------
    # Precheck
    # --------------------------------------

    MigrationPrecheck().validate()

    # --------------------------------------
    # Generate Tasks
    # --------------------------------------

    DataSyncTaskBuilder().build()

    # --------------------------------------
    # Execute Tasks
    # --------------------------------------

    DataSyncExecutor().execute()

    # --------------------------------------
    # Monitor Execution
    # --------------------------------------

    DataSyncMonitor().monitor()

    # --------------------------------------
    # Recovery
    # --------------------------------------

    MigrationRecovery().recover()

    # --------------------------------------
    # Migration Report
    # --------------------------------------

    MigrationReportBuilder().build()

    # --------------------------------------
    # Migration Reconciliation
    # --------------------------------------

    MigrationReconciliationBuilder().build()

    reconciliation = (

        StorageManager()

        .load_delta(
            MIGRATION_RECONCILIATION_PATH
        )

        .first()

    )

    if reconciliation.status == "FAIL":

        raise Exception(

            "Migration Reconciliation Failed"

        )

    logger.info(

        "Historical Migration Completed"

    )

except Exception as e:

    logger.error(
        str(e)
    )

    raise
