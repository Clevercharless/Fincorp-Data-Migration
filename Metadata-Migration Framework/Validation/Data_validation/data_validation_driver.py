# ==========================================
# data_validation_driver.py
# ==========================================

%run ./file_count_validator

%run ./file_size_validator

%run ./row_count_validator

%run ./checksum_validator

%run ./historical_validation_report_builder

%run ./historical_reconciliation_builder

%run ../../Common/logger
%run ../../Common/storage_manager


logger = Logger()

storage = StorageManager()

logger.info(
    "Historical Data Validation Started"
)

try:

    FileCountValidator().validate()

    FileSizeValidator().validate()

    RowCountValidator().validate()

    ChecksumValidator().validate()

    HistoricalValidationReportBuilder().build()

    HistoricalReconciliationBuilder().build()

    reconciliation = (

        storage

        .load_delta(
            HISTORICAL_RECONCILIATION_PATH
        )

        .first()

    )

    if reconciliation.status == "FAIL":

        raise Exception(

            "Historical Validation Failed"

        )

    logger.info(
        "Historical Validation Passed"
    )

except Exception as e:

    logger.error(
        str(e)
    )

    raise
