# ==========================================
# Common/audit_manager.py
# ==========================================

from pyspark.sql import Row
from datetime import datetime

from Common.config import AUDIT_PATH


class AuditManager:

    def __init__(
        self,
        storage
    ):

        self.storage = storage

    def write_audit(

        self,

        migration_phase,

        process_name,

        object_name,

        status,

        error_message=None,

        additional_info=None

    ):

        audit_df = spark.createDataFrame(

            [

                Row(

                    migration_phase=
                        migration_phase,

                    process_name=
                        process_name,

                    object_name=
                        object_name,

                    status=
                        status,

                    error_message=
                        error_message,

                    additional_info=
                        additional_info,

                    audit_time=
                        datetime.now()

                )

            ]

        )

        self.storage.save_delta(

            audit_df,

            AUDIT_PATH,

            mode="append"

        )

    def get_object_history(

        self,

        object_name

    ):

        return (

            self.storage

            .load_delta(
                AUDIT_PATH
            )

            .filter(
                f"object_name = '{object_name}'"
            )

            .orderBy(
                "audit_time"
            )

        )

    def get_phase_history(

        self,

        migration_phase

    ):

        return (

            self.storage

            .load_delta(
                AUDIT_PATH
            )

            .filter(
                f"""
                migration_phase =
                '{migration_phase}'
                """
            )

        )

    def get_failures():

        return (

            spark.read

            .format("delta")

            .load(AUDIT_PATH)

            .filter(
                "status='FAILED'"
            )

        )
