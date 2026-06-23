# ==========================================
# migration_checkpoint_manager.py
# ==========================================

from pyspark.sql import Row
from datetime import datetime

%run ../Common/config
%run ../Common/storage_manager


class MigrationCheckpointManager:

    def __init__(self):

        self.storage = StorageManager()

    def mark_complete(

        self,

        batch_id,

        layer

    ):

        checkpoint = spark.createDataFrame(

            [

                Row(

                    batch_id=batch_id,

                    layer=layer,

                    status="SUCCESS",

                    update_time=
                        datetime.now()

                )

            ]

        )

        self.storage.save_delta(

            checkpoint,

            HISTORICAL_CHECKPOINT_PATH,

            mode="append"

        )

    def load_completed(self):

        try:

            return (

                self.storage

                .load_delta(
                    HISTORICAL_CHECKPOINT_PATH
                )

                .select(
                    "batch_id",
                    "layer"
                )

            )

        except:

            return spark.createDataFrame(

                [],

                "batch_id INT, layer STRING"

            )
