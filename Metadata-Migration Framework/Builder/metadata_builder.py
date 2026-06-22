# ==========================================
# Builders/metadata_builder.py
# ==========================================

from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ==========================================
# Common Imports
# ==========================================

%run ../Common/config
%run ../Common/constants
%run ../Common/logger
%run ../Common/storage_manager
%run ../Common/retry_manager
%run ../Common/status_manager
%run ../Common/framework_metrics
%run ../Common/audit_manager


class MetadataBuilder:

    def __init__(self):

        self.logger = Logger()

        self.storage = StorageManager()

        self.status = StatusManager(
            self.storage
        )

        self.metrics = FrameworkMetrics()

        self.audit = AuditManager(
            self.storage
        )

    # ======================================
    # Build Master Metadata
    # ======================================

    def build_master_metadata(self):

    self.logger.info(
        "Building Master Metadata"
    )

    manifest_df = self.storage.load_delta(
        MIGRATION_BATCH_PATH
    )

    master_df = (

        manifest_df

        .select(

            "manifest_id",

            "catalog",

            "schema",

            "table",

            "table_type",

            "format",

            "source_path",

            "target_path",

            "creation_batch",

            "uc_creation_status",

            "migration_status",

            "datasync_status",

            "validation_status",

            "reconciliation_status"

        )

    )

    self.storage.save_delta(

        master_df,

        MASTER_METADATA_PATH

    )

    self.status.write_status(

        "MASTER_METADATA",

        "MASTER_METADATA_BUILD",

        SUCCESS,

        MASTER_STATUS_PATH

    )

    self.audit.write_audit(

        METADATA,

        "MASTER_METADATA_BUILD",

        "MASTER_METADATA",

        SUCCESS

    )

    self.logger.info(
        "Master Metadata Built"
    )

    # ======================================
    # Metadata Summary
    # ======================================

    def build_metadata_summary(self):

        self.logger.info(
            "Building Metadata Summary"
        )

        summary_rows = [

            Row(
                metric="catalog_count",
                value=self.storage
                .load_delta(CATALOG_PATH)
                .count()
            ),

            Row(
                metric="schema_count",
                value=self.storage
                .load_delta(SCHEMA_PATH)
                .count()
            ),

            Row(
                metric="table_count",
                value=self.storage
                .load_delta(TABLE_PATH)
                .count()
            ),

            Row(
                metric="column_count",
                value=self.storage
                .load_delta(COLUMN_PATH)
                .count()
            ),

            Row(
                metric="permission_count",
                value=self.storage
                .load_delta(PERMISSION_PATH)
                .count()
            ),

            Row(
                metric="partition_count",
                value=self.storage
                .load_delta(PARTITION_PATH)
                .count()
            )

        ]

        summary_df = spark.createDataFrame(
            summary_rows
        )

        self.storage.save_delta(

            summary_df,

            METADATA_SUMMARY_PATH

        )

        self.logger.info(
            "Metadata Summary Created"
        )

    # ======================================
    # Migration Manifest
    # ======================================

    def build_migration_manifest(self):

        self.logger.info(
            "Building Migration Manifest"
        )

        tables_df = self.storage.load_delta(
            TABLE_PATH
        )

        locations_df = self.storage.load_delta(
            STORAGE_LOCATION_PATH
        )

        partitions_df = self.storage.load_delta(
            PARTITION_PATH
        )

        partition_df = (

            partitions_df

            .orderBy(
                "partition_order"
            )

            .groupBy(
                "catalog",
                "schema",
                "table"
            )

            .agg(

                collect_list(
                    "partition_column"
                ).alias(
                    "partition_columns"
                )

            )

        )

        manifest_df = (

            tables_df

            .join(

                locations_df,

                ["catalog",
                 "schema",
                 "table"],

                "left"

            )

            .join(

                partition_df,

                ["catalog",
                 "schema",
                 "table"],

                "left"

            )

            .withColumn(

                "manifest_id",

                concat_ws(

                    ".",

                    col("catalog"),

                    col("schema"),

                    col("table")

                )

            )

            .withColumn(

                "uc_creation_status",

                lit("PENDING")

            )

            .withColumn(

                "migration_status",

                lit("PENDING")

            )

            .withColumn(

                "datasync_status",

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

                "created_time",

                current_timestamp()

            )

        )

        self.storage.save_delta(

            manifest_df,

            MIGRATION_MANIFEST_PATH

        )

        self.logger.info(
            "Migration Manifest Built"
        )

    # ======================================
    # Migration Batches
    # ======================================

    def build_migration_batches(self):

        self.logger.info(
            "Building Migration Batches"
        )

        manifest_df = self.storage.load_delta(
            MIGRATION_MANIFEST_PATH
        )

        window_spec = Window.orderBy(
            "catalog",
            "schema",
            "table"
        )

        batch_df = (

            manifest_df

            .withColumn(

                "row_num",

                row_number().over(
                    window_spec
                )

            )

            .withColumn(

                "creation_batch",

                ceil(

                    col("row_num")

                    /

                    lit(
                        MIGRATION_BATCH_SIZE
                    )

                )

            )

            .drop("row_num")

        )

        self.storage.save_delta(

            batch_df,

            MIGRATION_BATCH_PATH

        )

        self.logger.info(
            "Migration Batches Built"
        )

    # ======================================
    # Manifest Validation
    # ======================================

    def validate_manifest(self):

        self.logger.info(
            "Validating Manifest"
        )

        manifest = self.storage.load_delta(
            MIGRATION_BATCH_PATH
        )

        duplicate_count = (

            manifest

            .groupBy(
                "catalog",
                "schema",
                "table"
            )

            .count()

            .filter(
                col("count") > 1
            )

            .count()

        )

        if duplicate_count > 0:

            raise Exception(

                f"{duplicate_count} "
                f"Duplicate Tables Found"

            )

        missing_target_paths = (

            manifest

            .filter(

                col(
                    "target_path"
                ).isNull()

            )

            .count()

        )

        if missing_target_paths > 0:

            raise Exception(

                f"{missing_target_paths} "

                f"Tables Missing Target Path"

            )

        missing_format = (

            manifest

            .filter(

                col(
                    "format"
                ).isNull()

            )

            .count()

        )

        if missing_format > 0:

            raise Exception(

                f"{missing_format} "

                f"Tables Missing Format"

            )

        self.logger.info(
            "Manifest Validation Passed"
        )

    # ======================================
    # Run
    # ======================================

    def run(self):

        self.build_master_metadata()

        self.build_metadata_summary()

        self.build_migration_manifest()

        self.build_migration_batches()

        self.validate_manifest()

        self.metrics.save_metrics(

            "METADATA_BUILDER",

            self.storage,

            METADATA_METRICS_PATH

        )

        self.audit.write_audit(

            METADATA,

            "METADATA_BUILDER",

            "MIGRATION_MANIFEST",

            SUCCESS

        )

        self.logger.info(
            "Metadata Builder Completed"
        )
