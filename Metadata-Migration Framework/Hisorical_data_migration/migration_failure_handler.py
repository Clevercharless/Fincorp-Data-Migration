# ==========================================
# migration_failure_handler.py
# ==========================================

from pyspark.sql import Row
from datetime import datetime

%run ../Common/config
%run ../Common/logger
%run ../Common/storage_manager


class MigrationFailureHandler:

    def __init__(self):

        self.logger = Logger()

        self.storage = StorageManager()

    def record_failure(

        self,

        layer,

        batch_id,

        task_name,

        error,

        retry_count,
        manifest_ids,
        table_count,
        batch_size_gb

    ):

        try:

            failure_df = spark.createDataFrame(

                [

                    Row(
                        layer=layer,
                        batch_id=batch_id,
                        task_name=task_name,
                        manifest_ids=",".join(manifest_ids),
                        table_count=table_count,
                        batch_size_gb=batch_size_gb,
                        error_message=str(error),
                        retry_count=retry_count,
                        failure_time=datetime.now()
                    )

                ]

            )

            self.storage.save_delta(

                failure_df,

                FAILED_EXECUTION_PATH,

                mode="append"

            )

            self.logger.error(

                f"Failure Recorded : "
                f"{task_name}"

            )

        except Exception as e:

            self.logger.error(

                f"Failure Handler Error : "
                f"{str(e)}"

            )

            raise

    def classify_failure(

        self,

        error_message

    ):

        error_message = (
            str(error_message).lower()
        )

        if "access denied" in error_message:

            return "PERMISSION"

        elif "network" in error_message:

            return "NETWORK"

        elif "timeout" in error_message:

            return "TIMEOUT"

        elif "not found" in error_message:

            return "SOURCE_PATH"

        else:

            return "UNKNOWN"
