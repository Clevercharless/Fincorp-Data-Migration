# ==========================================
# datasync_monitor.py
# ==========================================

from pyspark.sql import Row
from datetime import datetime

%run ../Common/config
%run ../Common/logger
%run ../Common/storage_manager
%run ./migration_checkpoint_manager


class DataSyncMonitor:

    def __init__(self):

        self.logger = Logger()

        self.storage = StorageManager()

        self.checkpoint = (
            MigrationCheckpointManager()
        )

    def monitor(self):

        self.logger.info(
            "Monitoring DataSync Tasks"
        )

        executions = (

            self.storage.load_delta(
                DATASYNC_EXECUTION_PATH
            )

            .filter(
                col(
                    "execution_status"
                )
                == "RUNNING"
            )

        )

        for execution in (
            executions.toLocalIterator()
        ):

            try:

                # Replace with actual
                # boto3 DataSync API

                execution_status = (
                    "SUCCESS"
                )

                if execution_status == "SUCCESS":

                    self.checkpoint.mark_complete(

                        execution.historical_batch,

                        execution.layer

                    )

            except Exception as e:

                failed = spark.createDataFrame(

                    [

                        Row(

                            layer=
                                execution.layer,

                            batch=
                                execution.historical_batch,

                            error=
                                str(e),

                            failed_time=
                                datetime.now()

                        )

                    ]

                )

                self.storage.save_delta(

                    failed,

                    FAILED_DATASYNC_PATH,

                    mode="append"

                )

        self.logger.info(
            "Monitoring Completed"
        )
