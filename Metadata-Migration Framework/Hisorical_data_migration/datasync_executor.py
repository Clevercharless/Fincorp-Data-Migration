# ==========================================
# datasync_executor.py
# ==========================================

from pyspark.sql import Row
from datetime import datetime

%run ../Common/config
%run ../Common/logger
%run ../Common/storage_manager
%run ./migration_checkpoint_manager


class DataSyncExecutor:

    def __init__(self):

        self.logger = Logger()

        self.storage = StorageManager()

        self.checkpoint = (
            MigrationCheckpointManager()
        )

    def execute(self):

        self.logger.info(
            "DataSync Execution Started"
        )

        tasks = self.storage.load_delta(
            DATASYNC_TASK_PATH
        )

        completed = (
            self.checkpoint.load_completed()
        )

        execution_rows = []

        for task in (
            tasks.toLocalIterator()
        ):

            already_done = (

                completed

                .filter(

                    col(
                        "batch_id"
                    )

                    ==

                    task.historical_batch

                )

                .filter(

                    col(
                        "layer"
                    )

                    ==

                    task.layer

                )

                .count()

            )

            if already_done > 0:

                continue

            execution_rows.append(

                Row(

                    layer=
                        task.layer,

                    historical_batch=
                        task.historical_batch,

                    task_name=
                        task.task_name,

                    execution_status=
                        "RUNNING",

                    start_time=
                        datetime.now()

                )

            )

        execution_df = spark.createDataFrame(
            execution_rows
        )

        self.storage.save_delta(

            execution_df,

            DATASYNC_EXECUTION_PATH,

            mode="append"

        )

        self.logger.info(
            "DataSync Execution Submitted"
        )
