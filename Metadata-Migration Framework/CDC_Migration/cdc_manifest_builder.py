# ==========================================
# cdc_manifest_builder.py
# ==========================================

from pyspark.sql import Row

%run ../Common/config
%run ../Common/storage_manager
%run ./cdc_watermark_manager


class CDCManifestBuilder:

    def __init__(self):

        self.storage = StorageManager()

        self.watermark_manager = (
            CDCWatermarkManager()
        )

    def build(self):

        manifest = self.storage.load_delta(
            MIGRATION_BATCH_PATH
        )

        cdc_rows = []

        for table in (
            manifest.toLocalIterator()
        ):

            watermark = (

                self.watermark_manager
                .get_watermark(
                    table.manifest_id
                )

            )

            cdc_rows.append(

                Row(

                    manifest_id=
                        table.manifest_id,

                    source_path=
                        table.source_path,

                    target_path=
                        table.target_path,

                    watermark=
                        watermark,

                    layer=
                        table.layer

                )

            )

        cdc_df = spark.createDataFrame(
            cdc_rows
        )

        self.storage.save_delta(

            cdc_df,

            CDC_MANIFEST_PATH

        )
