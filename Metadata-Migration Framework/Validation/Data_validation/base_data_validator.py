# ==========================================
# base_data_validator.py
# ==========================================

from pyspark.sql import Row
from datetime import datetime

%run ../../Common/config
%run ../../Common/constants
%run ../../Common/logger
%run ../../Common/storage_manager
%run ../../Common/framework_metrics
%run ../../Common/audit_manager


class BaseDataValidator:

    def __init__(

        self,

        validation_type

    ):

        self.validation_type = (
            validation_type
        )

        self.logger = Logger()

        self.storage = StorageManager()

        self.metrics = FrameworkMetrics()

        self.audit = AuditManager(
            self.storage
        )

        self.validation_results = []

        self.failed_results = []

    # ======================================
    # Validation Result
    # ======================================

    def add_result(

        self,

        manifest_id,

        table_name,

        source_value,

        target_value,

        status,

        remarks=""

    ):

        self.validation_results.append(

            Row(

                validation_type=
                    self.validation_type,

                manifest_id=
                    manifest_id,

                table_name=
                    table_name,

                source_value=
                    str(source_value),

                target_value=
                    str(target_value),

                status=
                    status,

                remarks=
                    remarks,

                validation_time=
                    datetime.now()

            )

        )

        if status == SUCCESS:

            self.metrics.increment_success()

        else:

            self.metrics.increment_failed()

    # ======================================
    # Failure
    # ======================================

    def add_failure(

        self,

        manifest_id,

        table_name,

        error

    ):

        self.failed_results.append(

            Row(

                validation_type=
                    self.validation_type,

                manifest_id=
                    manifest_id,

                table_name=
                    table_name,

                error=
                    str(error),

                validation_time=
                    datetime.now()

            )

        )

    # ======================================
    # Save Results
    # ======================================

    def save_results(self):

        if self.validation_results:

            self.storage.save_delta(

                spark.createDataFrame(
                    self.validation_results
                ),

                HISTORICAL_VALIDATION_RESULTS_PATH,

                mode="append"

            )

    # ======================================
    # Save Failures
    # ======================================

    def save_failures(self):

        if self.failed_results:

            self.storage.save_delta(

                spark.createDataFrame(
                    self.failed_results
                ),

                HISTORICAL_VALIDATION_FAILURE_PATH,

                mode="append"

            )

    # ======================================
    # Finalize
    # ======================================

    def finalize(self):

        self.save_results()

        self.save_failures()

        self.metrics.save_metrics(

            self.validation_type,

            self.storage,

            HISTORICAL_VALIDATION_METRICS_PATH

        )

        self.audit.write_audit(

            VALIDATION,

            self.validation_type,

            self.validation_type,

            SUCCESS

        )
