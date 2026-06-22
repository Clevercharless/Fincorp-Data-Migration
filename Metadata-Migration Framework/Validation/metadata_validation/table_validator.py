# ==========================================
# table_validator.py
# ==========================================

%run ./base_validator

class TableValidator(BaseValidator):

    def __init__(self):

        super().__init__(
            "TABLE_VALIDATION"
        )

    def validate(self):

        try:

            tables_df = (

                self.storage.load_delta(
                    TABLE_PATH
                )

            )

            for table in (
                tables_df.toLocalIterator()
            ):

                try:

                    full_name = (

                        f"{table.catalog}."

                        f"{table.schema}."

                        f"{table.table}"

                    )

                    exists = (

                        spark.catalog.tableExists(
                            full_name
                        )

                    )

                    self.add_result(

                        full_name,

                        "EXISTS",

                        "EXISTS"
                        if exists
                        else
                        "MISSING",

                        SUCCESS
                        if exists
                        else
                        FAILED,

                        "Table Validation"

                    )

                except Exception as e:

                    self.add_failure(

                        full_name,

                        e

                    )

            self.logger.info(
                "Table Validation Completed"
            )

        finally:

            self.finalize()
