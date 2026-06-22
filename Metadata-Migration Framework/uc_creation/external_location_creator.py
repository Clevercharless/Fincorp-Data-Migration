# ==========================================
# UC_Creation/external_location_creator.py
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


class ExternalLocationCreator:

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

        completed = (
            self.checkpoint.load_completed(
                EXTERNAL_LOCATION_CHECKPOINT_PATH
            )
        )

        if (
            AWS_EXTERNAL_LOCATION_NAME
            in completed
        ):

            self.logger.info(
                "External Location Exists"
            )

            return

        try:

            RetryManager.execute(

                lambda:

                spark.sql(

                    f"""
                    CREATE EXTERNAL LOCATION
                    {AWS_EXTERNAL_LOCATION_NAME}

                    URL '{AWS_S3_ROOT}'

                    WITH (
                        STORAGE CREDENTIAL
                        {AWS_STORAGE_CREDENTIAL_NAME}
                    )
                    """

                )

            )

            self.status.write_status(

                AWS_EXTERNAL_LOCATION_NAME,

                "EXTERNAL_LOCATION_CREATION",

                SUCCESS,

                EXTERNAL_LOCATION_STATUS_PATH

            )

            self.checkpoint.mark_complete(

                AWS_EXTERNAL_LOCATION_NAME,

                EXTERNAL_LOCATION_CHECKPOINT_PATH

            )

            self.audit.write_audit(

                UC_CREATION,

                "EXTERNAL_LOCATION_CREATION",

                AWS_EXTERNAL_LOCATION_NAME,

                SUCCESS

            )

            self.metrics.increment_success()

        except Exception as e:

            self.metrics.increment_failed()

            self.audit.write_audit(

                UC_CREATION,

                "EXTERNAL_LOCATION_CREATION",

                AWS_EXTERNAL_LOCATION_NAME,

                FAILED,

                str(e)

            )

            raise

        finally:

            self.metrics.save_metrics(

                "EXTERNAL_LOCATION_CREATION",

                self.storage,

                UC_METRICS_PATH

            )
