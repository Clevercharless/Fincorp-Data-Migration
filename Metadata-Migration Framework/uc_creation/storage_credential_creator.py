# ==========================================
# UC_Creation/storage_credential_creator.py
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


class StorageCredentialCreator:

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
                STORAGE_CREDENTIAL_CHECKPOINT_PATH
            )
        )

        if (
            AWS_STORAGE_CREDENTIAL_NAME
            in completed
        ):

            self.logger.info(
                "Storage Credential Already Exists"
            )

            return

        try:

            RetryManager.execute(

                lambda:

                spark.sql(

                    f"""
                    CREATE STORAGE CREDENTIAL
                    {AWS_STORAGE_CREDENTIAL_NAME}

                    WITH IAM ROLE
                    '{AWS_IAM_ROLE_ARN}'
                    """

                )

            )

            self.status.write_status(

                AWS_STORAGE_CREDENTIAL_NAME,

                "STORAGE_CREDENTIAL_CREATION",

                SUCCESS,

                STORAGE_CREDENTIAL_STATUS_PATH

            )

            self.checkpoint.mark_complete(

                AWS_STORAGE_CREDENTIAL_NAME,

                STORAGE_CREDENTIAL_CHECKPOINT_PATH

            )

            self.audit.write_audit(

                UC_CREATION,

                "STORAGE_CREDENTIAL_CREATION",

                AWS_STORAGE_CREDENTIAL_NAME,

                SUCCESS

            )

            self.metrics.increment_success()

        except Exception as e:

            self.metrics.increment_failed()

            self.audit.write_audit(

                UC_CREATION,

                "STORAGE_CREDENTIAL_CREATION",

                AWS_STORAGE_CREDENTIAL_NAME,

                FAILED,

                str(e)

            )

            raise

        finally:

            self.metrics.save_metrics(

                "STORAGE_CREDENTIAL_CREATION",

                self.storage,

                UC_METRICS_PATH

            )
