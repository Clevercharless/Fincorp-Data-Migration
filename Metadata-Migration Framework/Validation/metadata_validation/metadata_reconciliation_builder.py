# ==========================================
# metadata_reconciliation_builder.py
# ==========================================

from pyspark.sql import Row

%run ../../Common/config
%run ../../Common/logger
%run ../../Common/storage_manager


class MetadataReconciliationBuilder:

    def __init__(self):

        self.logger = Logger()

        self.storage = StorageManager()

    def build(self):

        self.logger.info(
            "Building Metadata Reconciliation"
        )

        reconciliation_rows = []

        reconciliation_rows.append(

            Row(

                object_type="CATALOG",

                source_count=
                    self.storage
                    .load_delta(
                        CATALOG_PATH
                    )
                    .count(),

                target_count=
                    spark.sql(
                        "SHOW CATALOGS"
                    )
                    .count()

            )

        )

        reconciliation_rows.append(

            Row(

                object_type="SCHEMA",

                source_count=
                    self.storage
                    .load_delta(
                        SCHEMA_PATH
                    )
                    .count(),

                target_count=
                    self.storage
                    .load_delta(
                        VALIDATION_RESULTS_PATH
                    )

                    .filter(
                        col(
                            "validation_type"
                        )
                        ==
                        "SCHEMA_VALIDATION"
                    )

                    .filter(
                        col(
                            "status"
                        )
                        ==
                        SUCCESS
                    )

                    .count()

            )

        )

        reconciliation_rows.append(

            Row(

                object_type="TABLE",

                source_count=
                    self.storage
                    .load_delta(
                        TABLE_PATH
                    )
                    .count(),

                target_count=
                    self.storage
                    .load_delta(
                        VALIDATION_RESULTS_PATH
                    )

                    .filter(
                        col(
                            "validation_type"
                        )
                        ==
                        "TABLE_VALIDATION"
                    )

                    .filter(
                        col(
                            "status"
                        )
                        ==
                        SUCCESS
                    )

                    .count()

            )

        )

        reconciliation_df = spark.createDataFrame(
            reconciliation_rows
        )

        reconciliation_df = (

            reconciliation_df

            .withColumn(

                "status",

                when(

                    col(
                        "source_count"
                    )

                    ==

                    col(
                        "target_count"
                    ),

                    SUCCESS

                )

                .otherwise(
                    FAILED
                )

            )

        )

        self.storage.save_delta(

            reconciliation_df,

            METADATA_RECONCILIATION_PATH

        )

        self.logger.info(
            "Metadata Reconciliation Completed"
        )
