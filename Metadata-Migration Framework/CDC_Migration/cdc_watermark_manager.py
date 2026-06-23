# ==========================================
# cdc_watermark_manager.py
# ==========================================

from pyspark.sql import Row
from datetime import datetime

%run ../Common/config
%run ../Common/storage_manager


class CDCWatermarkManager:

    def __init__(self):

        self.storage = StorageManager()

    def get_watermark(

        self,

        manifest_id

    ):

        try:

            df = self.storage.load_delta(
                CDC_WATERMARK_PATH
            )

            record = (

                df

                .filter(
                    col("manifest_id")
                    == manifest_id
                )

                .orderBy(
                    desc(
                        "watermark"
                    )
                )

                .first()

            )

            if record:

                return record.watermark

            return None

        except:

            return None

    def update_watermark(

        self,

        manifest_id,

        watermark

    ):

        watermark_df = spark.createDataFrame(

            [

                Row(

                    manifest_id=
                        manifest_id,

                    watermark=
                        watermark,

                    update_time=
                        datetime.now()

                )

            ]

        )

        self.storage.save_delta(

            watermark_df,

            CDC_WATERMARK_PATH,

            mode="append"

        )
