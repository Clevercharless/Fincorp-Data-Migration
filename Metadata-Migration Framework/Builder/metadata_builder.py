# ==========================================
# Builders/metadata_builder.py
# ==========================================

from pyspark.sql import Row
from pyspark.sql.functions import *
from datetime import datetime

# ==========================================
# Common Imports
# ==========================================

%run ../Common/config
%run ../Common/constants
%run ../Common/logger
%run ../Common/storage_manager
%run ../Common/retry_manager
%run ../Common/checkpoint_manager
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

    # =====================================
    # Build Master Metadata
    # =====================================

    def build_master_metadata(self):

        self.logger.info(
            "Building Master Metadata"
        )

        tables_df = self.storage.load_delta(
            TABLE_PATH
        )

        columns_df = self.storage.load_delta(
            COLUMN_PATH
        )

        permissions_df = self.storage.load_delta(
            PERMISSION_PATH
        )

        partitions_df = self.storage.load_delta(
            PARTITION_PATH
        )

        storage_df = self.storage.load_delta(
            STORAGE_LOCATION_PATH
        )

        lineage_df = self.storage.load_delta(
            LINEAGE_PATH
        )

        master_df = (

            tables_df

            .join(

                storage_df,

                ["catalog",
                 "schema",
                 "table"],

                "left"

            )

        )

        self.storage.save_delta(

            master_df,

            f"{MASTER_PATH}/master_metadata"

        )

        self.status.write_status(

            "MASTER_METADATA",

            "BUILD_MASTER_METADATA",

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

    # =====================================
    # Metadata Summary
    # =====================================

    def build_metadata_summary(self):

        self.logger.info(
            "Building Metadata Summary"
        )

        summary_rows = [

            Row(
                metric="catalog_count",
                value=
                self.storage
                .load_delta(CATALOG_PATH)
                .count()
            ),

            Row(
                metric="schema_count",
                value=
                self.storage
                .load_delta(SCHEMA_PATH)
                .count()
            ),

            Row(
                metric="table_count",
                value=
                self.storage
                .load_delta(TABLE_PATH)
                .count()
            ),

            Row(
                metric="column_count",
                value=
                self.storage
                .load_delta(COLUMN_PATH)
                .count()
            ),

            Row(
                metric="permission_count",
                value=
                self.storage
                .load_delta(PERMISSION_PATH)
                .count()
            ),

            Row(
                metric="partition_count",
                value=
                self.storage
                .load_delta(PARTITION_PATH)
                .count()
            )

        ]

        summary_df = spark.createDataFrame(
            summary_rows
        )

        self.storage.save_delta(

            summary_df,

            f"{MASTER_PATH}/metadata_summary"

        )

        self.logger.info(
            "Metadata Summary Created"
        )

    # =====================================
    # Migration Manifest
    # =====================================

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

                "created_time",

                current_timestamp()

            )

        )

        self.storage.save_delta(

            manifest_df,

            f"{MASTER_PATH}/migration_manifest"

        )

        self.logger.info(
            "Migration Manifest Created"
        )

    # =====================================
    # Migration Batches
    # =====================================

    def build_migration_batches(self):

        self.logger.info(
            "Building Migration Batches"
        )

        manifest_df = self.storage.load_delta(

            f"{MASTER_PATH}/migration_manifest"

        )

        manifest_df = (

            manifest_df

            .withColumn(

                "row_num",

                row_number().over(

                    Window.orderBy(
                        "catalog",
                        "schema",
                        "table"
                    )

                )

            )

            .withColumn(

                "migration_batch",

                ceil(

                    col("row_num")

                    /

                    lit(
                        MIGRATION_BATCH_SIZE
                    )

                )

            )

        )

        self.storage.save_delta(

            manifest_df,

            f"{MASTER_PATH}/migration_batches"

        )

        self.logger.info(
            "Migration Batches Created"
        )

    # =====================================
    # Metadata Validation
    # =====================================

    def validate_master_metadata(self):

        self.logger.info(
            "Validating Metadata"
        )

        tables = self.storage.load_delta(
            TABLE_PATH
        ).count()

        columns = self.storage.load_delta(
            COLUMN_PATH
        ).count()

        locations = self.storage.load_delta(
            STORAGE_LOCATION_PATH
        ).count()

        if tables == 0:

            raise Exception(
                "No Tables Extracted"
            )

        if columns == 0:

            raise Exception(
                "No Columns Extracted"
            )

        if locations == 0:

            raise Exception(
                "No Storage Locations"
            )

        self.logger.info(
            "Metadata Validation Passed"
        )

    # =====================================
    # Execute
    # =====================================

    def run(self):

        self.build_master_metadata()

        self.build_metadata_summary()

        self.build_migration_manifest()

        self.build_migration_batches()

        self.validate_master_metadata()

        self.metrics.save_metrics(

            "METADATA_BUILDER",

            self.storage,

            METADATA_METRICS_PATH

        )

        self.audit.write_audit(

            METADATA,

            "METADATA_BUILDER",

            "MASTER_METADATA",

            SUCCESS

        )
