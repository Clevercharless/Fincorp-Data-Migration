# ==========================================
# Validation/Metadata_Validation/base_validator.py
# ==========================================

from pyspark.sql import Row
from pyspark.sql.functions import *
from datetime import datetime

# ==========================================
# Common Imports
# ==========================================

%run ../../Common/config
%run ../../Common/constants
%run ../../Common/logger
%run ../../Common/storage_manager
%run ../../Common/status_manager
%run ../../Common/framework_metrics
%run ../../Common/audit_manager


class BaseValidator:

    def __init__(
        self,
        validation_type
    ):

        self.validation_type = (
            validation_type
        )

        self.logger = Logger()

        self.storage = StorageManager()

        self.status = StatusManager(
            self.storage
        )

        self.metrics = FrameworkMetrics()

        self.audit = AuditManager(
            self.storage
        )

        self.validation_results = []

        self.failed_results = []

    # ======================================
    # Add Validation Result
    # ======================================

    def add_result(

        self,

        object_name,

        source_value,

        target_value,

        status,

        remarks=""

    ):

        self.validation_results.append(

            Row(

                validation_type=
                    self.validation_type,

                object_name=
                    object_name,

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
    # Add Failure
    # ======================================

    def add_failure(

        self,

        object_name,

        error

    ):

        self.failed_results.append(

            Row(

                validation_type=
                    self.validation_type,

                object_name=
                    object_name,

                error=
                    str(error),

                validation_time=
                    datetime.now()

            )

        )

    # ======================================
    # Save Validation Results
    # ======================================

    def save_results(self):

        if self.validation_results:

            results_df = (

                spark.createDataFrame(
                    self.validation_results
                )

            )

            self.storage.save_delta(

                results_df,

                VALIDATION_RESULTS_PATH,

                mode="append"

            )

    # ======================================
    # Save Failures
    # ======================================

    def save_failures(self):

        if self.failed_results:

            failed_df = (

                spark.createDataFrame(
                    self.failed_results
                )

            )

            self.storage.save_delta(

                failed_df,

                FAILED_VALIDATION_PATH,

                mode="append"

            )

    # ======================================
    # Finalize Validation
    # ======================================

    def finalize(self):

        self.save_results()

        self.save_failures()

        self.metrics.save_metrics(

            self.validation_type,

            self.storage,

            VALIDATION_METRICS_PATH

        )

        self.audit.write_audit(

            VALIDATION,

            self.validation_type,

            self.validation_type,

            SUCCESS

        )

        self.logger.info(

            f"{self.validation_type} "
            f"Validation Completed"

        )

    # ======================================
    # Common Compare Utility
    # ======================================

    def compare_values(

        self,

        object_name,

        source_value,

        target_value

    ):

        if source_value == target_value:

            self.add_result(

                object_name,

                source_value,

                target_value,

                SUCCESS,

                "Values Match"

            )

        else:

            self.add_result(

                object_name,

                source_value,

                target_value,

                FAILED,

                "Values Mismatch"

            )

    # ======================================
    # Common Null Check
    # ======================================

    def validate_not_null(

        self,

        object_name,

        value

    ):

        if value is None:

            self.add_result(

                object_name,

                "NOT NULL",

                "NULL",

                FAILED,

                "Null Value Found"

            )

        else:

            self.add_result(

                object_name,

                "NOT NULL",

                "NOT NULL",

                SUCCESS,

                "Validation Passed"

            )
