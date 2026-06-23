# ==========================================
# cdc_batch_builder.py
# ==========================================

from pyspark.sql.functions import *

%run ../Common/config
%run ../Common/storage_manager


class CDCBatchBuilder:

    def __init__(self):

        self.storage = StorageManager()

    def build(self):

        cdc_manifest = (

            self.storage.load_delta(
                CDC_MANIFEST_PATH
            )

        )

        cdc_batches = (

            cdc_manifest

            .withColumn(

                "cdc_batch",

                monotonically_increasing_id()

            )

        )

        self.storage.save_delta(

            cdc_batches,

            CDC_BATCH_PATH

        )
