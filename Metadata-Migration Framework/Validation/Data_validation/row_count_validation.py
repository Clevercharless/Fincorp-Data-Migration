# ==========================================
# row_count_validator.py
# ==========================================

%run ./base_data_validator

class RowCountValidator(
    BaseDataValidator
):

    def __init__(self):

        super().__init__(
            "ROW_COUNT_VALIDATION"
        )

    def validate(self):

        manifest = self.storage.load_delta(
            MIGRATION_BATCH_PATH
        )

        try:

            for table in manifest.toLocalIterator():

                try:

                    source_table = (
                        f"{table.catalog}."
                        f"{table.schema}."
                        f"{table.table}"
                    )

                    target_table = source_table

                    source_count = spark.sql(
                        f"""
                        SELECT COUNT(*)
                        FROM {source_table}
                        """
                    ).first()[0]

                    target_count = spark.sql(
                        f"""
                        SELECT COUNT(*)
                        FROM {target_table}
                        """
                    ).first()[0]

                    self.add_result(

                        table.manifest_id,

                        table.table,

                        source_count,

                        target_count,

                        SUCCESS
                        if source_count
                        ==
                        target_count
                        else FAILED,

                        "Row Count Validation"

                    )

                except Exception as e:

                    self.add_failure(

                        table.manifest_id,

                        table.table,

                        e

                    )

        finally:

            self.finalize()
