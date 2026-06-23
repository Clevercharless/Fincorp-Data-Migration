# ==========================================
# datasync_task_builder.py
# ==========================================

from pyspark.sql import Row
from datetime import datetime

%run ../Common/config
%run ../Common/logger
%run ../Common/storage_manager


class DataSyncTaskBuilder:

    def __init__(self):

        self.logger = Logger()

        self.storage = StorageManager()

    def build(self):

        self.logger.info(
            "Building DataSync Tasks"
        )

        batches = self.storage.load_delta(
            HISTORICAL_BATCH_PATH
        )

        task_rows = []

        distinct_batches = (

            batches

            .select(
                "layer",
                "historical_batch"
            )

            .distinct()

        )

        for batch in (
            distinct_batches.toLocalIterator()
        ):

            task_name = (

                f"{batch.layer}_"
                f"{batch.historical_batch}"
            )

            task_rows.append(

                Row(

                    layer=batch.layer,

                    historical_batch=
                        batch.historical_batch,

                    task_name=
                        task_name,

                    task_arn=None,

                    task_status=
                        "PENDING",

                    created_time=
                        datetime.now()

                )

            )

        task_df = spark.createDataFrame(
            task_rows
        )

        self.storage.save_delta(

            task_df,

            DATASYNC_TASK_PATH

        )

        self.logger.info(
            "DataSync Tasks Generated"
        )
