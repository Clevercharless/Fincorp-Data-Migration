# =====================================================
# MASTER METADATA BUILDER
# =====================================================

%run ../Common/config
%run ../Common/storage_manager
%run ../Common/retry_manager
%run ../Common/checkpoint_manager
%run ../Common/status_manager
%run ../Common/logger

from pyspark.sql import Row
from pyspark.sql.functions import *
from datetime import datetime

storage = StorageManager()

checkpoint = CheckpointManager(
    storage
)

status = StatusManager(
    storage
)

logger = Logger()


METADATA_OBJECTS = {

    "catalogs": {

        "source": CATALOG_PATH,

        "target": f"{MASTER_PATH}/catalogs"

    },

    "schemas": {

        "source": SCHEMA_PATH,

        "target": f"{MASTER_PATH}/schemas"

    },

    "tables": {

        "source": TABLE_PATH,

        "target": f"{MASTER_PATH}/tables"

    },

    "columns": {

        "source": COLUMN_PATH,

        "target": f"{MASTER_PATH}/columns"

    },

    "permissions": {

        "source": PERMISSION_PATH,

        "target": f"{MASTER_PATH}/permissions"

    },

    "lineage": {

        "source": LINEAGE_PATH,

        "target": f"{MASTER_PATH}/lineage"

    }

}

def build_master_metadata():

    logger.info(
        "Master Metadata Build Started"
    )

    completed = checkpoint.load_completed(
        MASTER_STATUS_PATH
    )

    failed_rows = []

    for metadata_name, paths in (
        METADATA_OBJECTS.items()
    ):

        if metadata_name in completed:

            logger.info(
                f"Skipping {metadata_name}"
            )

            continue

        try:

            source_df = RetryManager.execute(

                lambda:

                storage.load_delta(
                    paths["source"]
                )

            )

            record_count = source_df.count()

            storage.save_delta(

                source_df,

                paths["target"]

            )

            checkpoint.mark_complete(

                metadata_name,

                MASTER_STATUS_PATH

            )

            status.write_status(

                metadata_name,

                "MASTER_METADATA",

                "SUCCESS",

                MASTER_STATUS_PATH

            )

            logger.info(

                f"{metadata_name} completed "
                f"with {record_count} records"

            )

        except Exception as e:

            failed_rows.append(

                Row(

                    object_name=
                        metadata_name,

                    error=
                        str(e),

                    failed_time=
                        datetime.now()

                )

            )

            status.write_status(

                metadata_name,

                "MASTER_METADATA",

                "FAILED",

                MASTER_STATUS_PATH,

                str(e)

            )

    if failed_rows:

        storage.save_delta(

            spark.createDataFrame(
                failed_rows
            ),

            FAILED_MASTER_PATH,

            mode="append"

        )

    logger.info(
        "Master Metadata Build Completed"
    )

def build_metadata_summary():

    logger.info(
        "Metadata Summary Started"
    )

    summary_rows = []

    for metadata_name, paths in (
        METADATA_OBJECTS.items()
    ):

        try:

            df = storage.load_delta(
                paths["target"]
            )

            summary_rows.append(

                Row(

                    metadata_type=
                        metadata_name,

                    record_count=
                        df.count(),

                    generated_time=
                        datetime.now()

                )

            )

        except Exception:

            pass

    summary_df = spark.createDataFrame(
        summary_rows
    )

    storage.save_delta(

        summary_df,

        f"{MASTER_PATH}/summary"

    )

    logger.info(
        "Metadata Summary Completed"
    )


def build_migration_manifest():

    logger.info(
        "Migration Manifest Started"
    )

    tables_df = storage.load_delta(
        TABLE_PATH
    )

    manifest_df = (

        tables_df

        .withColumn(

            "source_path",

            concat(

                lit("abfss://"),

                col("catalog"),

                lit("/"),

                col("schema"),

                lit("/"),

                col("table")

            )

        )

        .withColumn(

            "target_path",

            concat(

                lit("s3://"),

                col("catalog"),

                lit("/"),

                col("schema"),

                lit("/"),

                col("table")

            )

        )

        .withColumn(

            "migration_status",

            lit("PENDING")

        )

        .withColumn(

            "validation_status",

            lit("PENDING")

        )

        .withColumn(

            "reconciliation_status",

            lit("PENDING")

        )

        .withColumn(

            "wave",

            when(
                col("schema")
                .contains("bronze"),
                lit(1)
            )

            .when(
                col("schema")
                .contains("silver"),
                lit(2)
            )

            .when(
                col("schema")
                .contains("gold"),
                lit(3)
            )

            .otherwise(
                lit(99)
            )

        )

    )

    storage.save_delta(

        manifest_df,

        f"{MASTER_PATH}/migration_manifest"

    )

    logger.info(
        "Migration Manifest Completed"
    )
