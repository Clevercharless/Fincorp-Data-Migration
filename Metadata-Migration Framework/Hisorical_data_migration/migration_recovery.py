# ==========================================
# migration_recovery.py
# ==========================================

from pyspark.sql.functions import *

%run ../Common/config
%run ../Common/logger
%run ../Common/storage_manager
%run ./datasync_executor


class MigrationRecovery:

    def __init__(self):

        self.logger = Logger()

        self.storage = StorageManager()

    def recover(self):

        self.logger.info(
            "Migration Recovery Started"
        )

        failed_batches = (

            self.storage.load_delta(
                FAILED_EXECUTION_PATH
            )

        )

        recovery_batches = (

            failed_batches

            .groupBy(

                "layer",

                "batch_id",

                "task_name"

            )

            .agg(

                max(
                    "retry_count"
                )

                .alias(
                    "retry_count"
                )

            )

            .filter(

                col(
                    "retry_count"
                )

                < MAX_RETRY_COUNT

            )

        )

        total_batches = (
            recovery_batches.count()
        )

        self.logger.info(

            f"Recovery Batches : "
            f"{total_batches}"

        )

        for batch in (
            recovery_batches.toLocalIterator()
        ):

            try:

                self.logger.info(

                    f"Retrying Batch : "
                    f"{batch.batch_id}"

                )

                DataSyncExecutor().execute_batch(

                    batch.layer,

                    batch.batch_id

                )

            except Exception as e:

                self.logger.error(

                    f"Recovery Failed : "
                    f"{str(e)}"

                )

        self.logger.info(
            "Migration Recovery Completed"
        )
