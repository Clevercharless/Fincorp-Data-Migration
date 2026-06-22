# ==========================================
# Extractors/lineage_extractor.py
# ==========================================

from pyspark.sql import Row
from datetime import datetime

# Common Imports

%run ../Common/config
%run ../Common/constants
%run ../Common/logger
%run ../Common/storage_manager
%run ../Common/retry_manager
%run ../Common/checkpoint_manager
%run ../Common/status_manager
%run ../Common/batch_manager
%run ../Common/framework_metrics
%run ../Common/audit_manager


class LineageExtractor:

    def __init__(self):

        self.logger = Logger()

        self.storage = StorageManager()

        self.checkpoint = CheckpointManager(
            self.storage
        )

        self.status = StatusManager(
            self.storage
        )

        self.metrics = FrameworkMetrics()

        self.audit = AuditManager(
            self.storage
        )

    def extract(self):

        self.logger.info(
            "Lineage Extraction Started"
        )

        tables = self.storage.load_delta(
            TABLE_PATH
        )

        completed = (
            self.checkpoint.load_completed(
                LINEAGE_STATUS_PATH
            )
        )

        failed_rows = []

        total_batches = (

            BatchManager.get_batch_count(

                tables.count(),

                BATCH_SIZE

            )

        )

        batch_no = 1

        for batch in BatchManager.get_batches(

            tables,

            BATCH_SIZE

        ):

            BatchManager.print_progress(

                batch_no,

                total_batches

            )

            lineage_rows = []

            for table in batch:

                full_name = (

                    f"{table.catalog}."
                    f"{table.schema}."
                    f"{table.table}"

                )

                if full_name in completed:

                    self.metrics.increment_skipped()

                    continue

                try:

                    self.metrics.increment_processed()

                    # ----------------------------------
                    # Placeholder
                    # Replace with actual UC lineage API
                    # ----------------------------------

                    lineage_records = []

                    for lineage in lineage_records:

                        lineage_rows.append(

                            Row(

                                source_catalog=
                                    lineage["source_catalog"],

                                source_schema=
                                    lineage["source_schema"],

                                source_table=
                                    lineage["source_table"],

                                target_catalog=
                                    lineage["target_catalog"],

                                target_schema=
                                    lineage["target_schema"],

                                target_table=
                                    lineage["target_table"],

                                lineage_type=
                                    lineage["lineage_type"],

                                extracted_time=
                                    datetime.now()

                            )

                        )

                    self.status.write_status(

                        full_name,

                        "LINEAGE_EXTRACTION",

                        SUCCESS,

                        LINEAGE_STATUS_PATH

                    )

                    self.checkpoint.mark_complete(

                        full_name,

                        LINEAGE_STATUS_PATH

                    )

                    self.audit.write_audit(

                        METADATA,

                        "LINEAGE_EXTRACTION",

                        full_name,

                        SUCCESS

                    )

                    self.metrics.increment_success()

                except Exception as e:

                    failed_rows.append(

                        Row(

                            object_name=
                                full_name,

                            error=
                                str(e)

                        )

                    )

                    self.audit.write_audit(

                        METADATA,

                        "LINEAGE_EXTRACTION",

                        full_name,

                        FAILED,

                        str(e)

                    )

                    self.metrics.increment_failed()

            if lineage_rows:

                self.storage.save_delta(

                    spark.createDataFrame(
                        lineage_rows
                    ),

                    LINEAGE_PATH,

                    mode="append"

                )

            batch_no += 1

        if failed_rows:

            self.storage.save_delta(

                spark.createDataFrame(
                    failed_rows
                ),

                FAILED_ROOT +
                "/lineage_failures"

            )

        self.metrics.save_metrics(

            "LINEAGE_EXTRACTION",

            self.storage,

            METADATA_METRICS_PATH

        )

        self.logger.info(
            "Lineage Extraction Completed"
        )
