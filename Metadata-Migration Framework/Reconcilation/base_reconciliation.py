# ==========================================
# base_reconciliation.py
# ==========================================

from pyspark.sql import Row
from datetime import datetime

%run ../../Common/config
%run ../../Common/logger
%run ../../Common/storage_manager
%run ../../Common/framework_metrics
%run ../../Common/audit_manager


class BaseReconciliation:

    def __init__(

        self,

        reconciliation_type

    ):

        self.reconciliation_type = (
            reconciliation_type
        )

        self.logger = Logger()

        self.storage = StorageManager()

        self.metrics = FrameworkMetrics()

        self.audit = AuditManager(
            self.storage
        )

        self.results = []

    def add_result(

        self,

        metric_name,

        source_value,

        target_value,

        status,

        remarks=""

    ):

        self.results.append(

            Row(

                reconciliation_type=
                    self.reconciliation_type,

                metric_name=
                    metric_name,

                source_value=
                    source_value,

                target_value=
                    target_value,

                status=
                    status,

                remarks=
                    remarks,

                reconciliation_time=
                    datetime.now()

            )

        )

    def save(self):

        if self.results:

            self.storage.save_delta(

                spark.createDataFrame(
                    self.results
                ),

                DATA_RECONCILIATION_PATH,

                mode="append"

            )
