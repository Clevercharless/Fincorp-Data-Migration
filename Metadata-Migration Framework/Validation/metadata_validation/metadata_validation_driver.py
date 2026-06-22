# ==========================================
# metadata_validation_driver.py
# ==========================================

%run ./catalog_validator
%run ./schema_validator
%run ./table_validator
%run ./column_validator
%run ./partition_validator
%run ./permission_validator
%run ./storage_location_validator
%run ./manifest_validator

%run ./metadata_validation_report_builder
%run ./metadata_reconciliation_builder

%run ../../Common/logger
%run ../../Common/storage_manager

logger = Logger()

storage = StorageManager()

logger.info(
    "Metadata Validation Started"
)

try:

    CatalogValidator().validate()

    SchemaValidator().validate()

    TableValidator().validate()

    ColumnValidator().validate()

    PartitionValidator().validate()

    PermissionValidator().validate()

    StorageLocationValidator().validate()

    ManifestValidator().validate()

    MetadataValidationReportBuilder().build()

    MetadataReconciliationBuilder().build()

    report = storage.load_delta(
        METADATA_VALIDATION_REPORT_PATH
    )

    failed_count = (

        report

        .agg(
            sum(
                "failed_checks"
            )
        )

        .first()[0]

    )

    if failed_count > 0:

        raise Exception(

            f"Metadata Validation Failed. "
            f"Failed Checks = {failed_count}"

        )

    logger.info(

        "Metadata Validation Passed"

    )

except Exception as e:

    logger.error(
        str(e)
    )

    raise
