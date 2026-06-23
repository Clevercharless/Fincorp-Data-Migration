# ==========================================
# migration_precheck.py
# ==========================================

%run ../Common/config
%run ../Common/logger
%run ../Common/storage_manager


class MigrationPrecheck:

    def __init__(self):

        self.logger = Logger()

        self.storage = StorageManager()

    def validate(self):

        self.logger.info(
            "Migration Precheck Started"
        )

        manifest_count = (

            self.storage

            .load_delta(
                MIGRATION_BATCH_PATH
            )

            .count()

        )

        if manifest_count == 0:

            raise Exception(
                "Migration Manifest Empty"
            )

        batch_count = (

            self.storage

            .load_delta(
                HISTORICAL_BATCH_PATH
            )

            .count()

        )

        if batch_count == 0:

            raise Exception(
                "Historical Batches Missing"
            )

        validation_report = (

            self.storage

            .load_delta(
                METADATA_VALIDATION_REPORT_PATH
            )

        )

        failed_checks = (

            validation_report

            .agg(
                sum(
                    "failed_checks"
                )
            )

            .first()[0]

        )

        if failed_checks > 0:

            raise Exception(

                "Metadata Validation Failed"

            )

        self.logger.info(
            "Migration Precheck Passed"
        )
