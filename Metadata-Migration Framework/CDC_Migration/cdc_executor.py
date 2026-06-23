# ==========================================
# cdc_executor.py
# ==========================================

from datetime import datetime

%run ../Common/logger
%run ../Common/storage_manager
%run ./cdc_watermark_manager


class CDCExecutor:

    def __init__(self):

        self.logger = Logger()

        self.storage = StorageManager()

        self.watermark_manager = (
            CDCWatermarkManager()
        )

    def execute(self):

        cdc_batches = (

            self.storage.load_delta(
                CDC_BATCH_PATH
            )

        )

        for table in (
            cdc_batches.toLocalIterator()
        ):

            try:

                watermark = (
                    table.watermark
                )

                source_df = spark.read.format(
                    table.format
                ).load(
                    table.source_path
                )

                if watermark:

                    source_df = (

                        source_df

                        .filter(

                            col(
                                "last_updated_ts"
                            )

                            > watermark

                        )

                    )

                change_count = (
                    source_df.count()
                )

                source_df.write.mode(
                    "append"
                ).format(
                    table.format
                ).save(
                    table.target_path
                )

                new_watermark = (
                    datetime.now()
                )

                self.watermark_manager.update_watermark(

                    table.manifest_id,

                    new_watermark

                )

                self.logger.info(

                    f"{table.manifest_id}"

                    f" CDC Records : "

                    f"{change_count}"

                )

            except Exception as e:

                raise
