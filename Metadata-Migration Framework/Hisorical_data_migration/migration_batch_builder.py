# ==========================================
# migration_batch_builder.py
# ==========================================

from pyspark.sql.functions import *
from pyspark.sql.window import Window

%run ../Common/config
%run ../Common/logger
%run ../Common/storage_manager


class MigrationBatchBuilder:

    def __init__(self):

        self.logger = Logger()

        self.storage = StorageManager()

    def build(self):

        self.logger.info(
            "Building Historical Migration Batches"
        )

        manifest = self.storage.load_delta(
            MIGRATION_BATCH_PATH
        )

        manifest = manifest.fillna(
            {
                "estimated_size_gb": 1
            }
        )

        final_batches = []

        for layer in [

            "BRONZE",
            "SILVER",
            "GOLD"

        ]:

            layer_df = (

                manifest

                .filter(
                    upper(col("layer"))
                    == layer
                )

                .orderBy(
                    "estimated_size_gb"
                )

            )

            current_batch = 1

            current_size = 0

            for row in (
                layer_df.toLocalIterator()
            ):

                if (

                    current_size
                    +
                    row.estimated_size_gb

                ) > HISTORICAL_BATCH_SIZE_GB:

                    current_batch += 1

                    current_size = 0

                final_batches.append(

                    (

                        row.manifest_id,

                        layer,

                        current_batch,

                        row.estimated_size_gb

                    )

                )

                current_size += (
                    row.estimated_size_gb
                )

        batch_df = spark.createDataFrame(

            final_batches,

            [

                "manifest_id",

                "layer",

                "historical_batch",

                "estimated_size_gb"

            ]

        )

        self.storage.save_delta(

            batch_df,

            HISTORICAL_BATCH_PATH

        )

        self.logger.info(
            "Historical Batches Created"
        )
