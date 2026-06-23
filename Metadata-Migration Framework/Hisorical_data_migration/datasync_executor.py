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


# ==========================================
# Execute Single Batch
# ==========================================

def execute_batch(

    self,

    layer,

    batch_id

):

    self.logger.info(

        f"Executing Batch : "
        f"{layer}-{batch_id}"

    )

    try:

        batch_tables = (

            self.storage.load_delta(
                HISTORICAL_BATCH_PATH
            )

            .filter(
                col("layer")
                == layer
            )

            .filter(
                col("historical_batch")
                == batch_id
            )

        )

        task_info = (

            self.storage.load_delta(
                DATASYNC_TASK_PATH
            )

            .filter(
                col("layer")
                == layer
            )

            .filter(
                col("historical_batch")
                == batch_id
            )

            .first()

        )

        if task_info is None:

            raise Exception(

                f"Task Not Found : "
                f"{layer}-{batch_id}"

            )

        table_count = (
            batch_tables.count()
        )

        batch_size_gb = (

            batch_tables

            .agg(
                sum(
                    "estimated_size_gb"
                )
            )

            .first()[0]

        )

        manifest_ids = [

            row.manifest_id

            for row in

            batch_tables.select(
                "manifest_id"
            ).collect()

        ]

        # ----------------------------------
        # Actual boto3 call comes here
        # ----------------------------------

        execution_status = "RUNNING"

        execution_df = spark.createDataFrame(

            [

                Row(

                    layer=layer,

                    historical_batch=
                        batch_id,

                    task_name=
                        task_info.task_name,

                    execution_status=
                        execution_status,

                    table_count=
                        table_count,

                    batch_size_gb=
                        batch_size_gb,

                    start_time=
                        datetime.now()

                )

            ]

        )

        self.storage.save_delta(

            execution_df,

            DATASYNC_EXECUTION_PATH,

            mode="append"

        )

        self.logger.info(

            f"Batch Submitted : "

            f"{layer}-{batch_id}"

        )

    except Exception as e:

        raise
