# ==========================================
# UC_Creation/permission_creator.py
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


class PermissionCreator:

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

        self.logger.info(
            "Permission Creation Started"
        )

        permissions_df = self.storage.load_delta(
            PERMISSION_PATH
        )

        completed = (
            self.checkpoint.load_completed(
                PERMISSION_CREATION_CHECKPOINT_PATH
            )
        )

        failed_rows = []

        total_batches = BatchManager.get_batch_count(
            permissions_df.count(),
            MIGRATION_BATCH_SIZE
        )

        batch_no = 1

        for batch_df in (
            BatchManager.get_dataframe_batches(
                permissions_df,
                MIGRATION_BATCH_SIZE
            )
        ):

            BatchManager.print_progress(
                batch_no,
                total_batches
            )

            for permission in (
                batch_df.toLocalIterator()
            ):

                permission_key = (

                    f"{permission.catalog}|"

                    f"{permission.schema}|"

                    f"{permission.table}|"

                    f"{permission.principal}|"

                    f"{permission.privilege}"

                )

                if permission_key in completed:

                    self.metrics.increment_skipped()

                    continue

                try:

                    self.metrics.increment_processed()

                    full_name = (

                        f"{permission.catalog}."

                        f"{permission.schema}."

                        f"{permission.table}"

                    )

                    grant_sql = f"""
                    GRANT
                    {permission.privilege}

                    ON TABLE
                    {full_name}

                    TO
                    `{permission.principal}`
                    """

                    RetryManager.execute(

                        lambda:

                        spark.sql(
                            grant_sql
                        )

                    )

                    self.status.write_status(

                        permission_key,

                        "PERMISSION_CREATION",

                        SUCCESS,

                        PERMISSION_CREATION_STATUS_PATH

                    )

                    self.checkpoint.mark_complete(

                        permission_key,

                        PERMISSION_CREATION_CHECKPOINT_PATH

                    )

                    self.audit.write_audit(

                        UC_CREATION,

                        "PERMISSION_CREATION",

                        permission_key,

                        SUCCESS

                    )

                    self.metrics.increment_success()

                except Exception as e:

                    failed_rows.append(

                        Row(

                            permission_key=
                                permission_key,

                            catalog=
                                permission.catalog,

                            schema=
                                permission.schema,

                            table=
                                permission.table,

                            principal=
                                permission.principal,

                            privilege=
                                permission.privilege,

                            error=
                                str(e)

                        )

                    )

                    self.audit.write_audit(

                        UC_CREATION,

                        "PERMISSION_CREATION",

                        permission_key,

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

                FAILED_PERMISSION_CREATION_PATH,

                mode="append"

            )

        self.metrics.save_metrics(

            "PERMISSION_CREATION",

            self.storage,

            UC_METRICS_PATH

        )

        self.logger.info(
            "Permission Creation Completed"
        )
