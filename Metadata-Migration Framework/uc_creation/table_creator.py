# ==========================================
# UC_Creation/table_creator.py
# ==========================================

from pyspark.sql import Row

%run ../Common/config
%run ../Common/constants
%run ../Common/logger
%run ../Common/storage_manager
%run ../Common/retry_manager
%run ../Common/checkpoint_manager
%run ../Common/status_manager
%run ../Common/framework_metrics
%run ../Common/audit_manager
%run ../Common/batch_manager


class TableCreator:

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

    def create(self):

        manifest = self.storage.load_delta(
            MIGRATION_BATCH_PATH
        )

        completed = (
            self.checkpoint.load_completed(
                TABLE_CREATION_CHECKPOINT_PATH
            )
        )

        failed_rows = []

        total_batches = (

            manifest

            .select("creation_batch")

            .distinct()

            .count()

        )

        batch_no = 1

        for batch_df in (

            BatchManager.get_dataframe_batches(

                manifest,

                MIGRATION_BATCH_SIZE

            )

        ):

            BatchManager.print_progress(

                batch_no,

                total_batches

            )

            for table in batch_df.toLocalIterator():

                manifest_id = (
                    table.manifest_id
                )

                if manifest_id in completed:

                    self.metrics.increment_skipped()

                    continue

                try:

                    self.metrics.increment_processed()

                    table_location = (
                        table.target_path
                    )

                    table_format = (
                        table.format
                    )

                    sql = f"""
                    CREATE TABLE IF NOT EXISTS
                    {table.catalog}.
                    {table.schema}.
                    {table.table}

                    USING {table_format}

                    LOCATION
                    '{table_location}'
                    """

                    RetryManager.execute(

                        lambda:

                        spark.sql(sql)

                    )

                    self.status.write_status(

                        manifest_id,

                        "TABLE_CREATION",

                        SUCCESS,

                        TABLE_CREATION_STATUS_PATH

                    )

                    self.checkpoint.mark_complete(

                        manifest_id,

                        TABLE_CREATION_CHECKPOINT_PATH

                    )

                    self.audit.write_audit(

                        UC_CREATION,

                        "TABLE_CREATION",

                        manifest_id,

                        SUCCESS

                    )

                    self.metrics.increment_success()

                except Exception as e:

                    failed_rows.append(

                        Row(

                            manifest_id=
                                manifest_id,

                            error=
                                str(e)

                        )

                    )

                    self.audit.write_audit(

                        UC_CREATION,

                        "TABLE_CREATION",

                        manifest_id,

                        FAILED,

                        str(e)

                    )

                    self.metrics.increment_failed()

            batch_no += 1

        if failed_rows:

            self.storage.save_delta(

                spark.createDataFrame(
                    failed_rows
                ),

                FAILED_TABLE_CREATION_PATH,

                mode="append"

            )

        self.metrics.save_metrics(

            "TABLE_CREATION",

            self.storage,

            UC_METRICS_PATH

        )
