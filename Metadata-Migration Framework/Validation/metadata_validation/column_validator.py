# ==========================================
# column_validator.py
# ==========================================

%run ./base_validator

class ColumnValidator(BaseValidator):

    def __init__(self):

        super().__init__(
            "COLUMN_VALIDATION"
        )

    def validate(self):

        try:

            columns_df = self.storage.load_delta(
                COLUMN_PATH
            )

            for column in columns_df.toLocalIterator():

                try:

                    full_name = (
                        f"{column.catalog}."
                        f"{column.schema}."
                        f"{column.table}"
                    )

                    target_columns = spark.sql(
                        f"DESCRIBE TABLE {full_name}"
                    )

                    match_count = (

                        target_columns

                        .filter(
                            col("col_name")
                            ==
                            column.column_name
                        )

                        .count()

                    )

                    self.add_result(

                        f"{full_name}."
                        f"{column.column_name}",

                        column.data_type,

                        "FOUND"
                        if match_count > 0
                        else
                        "MISSING",

                        SUCCESS
                        if match_count > 0
                        else
                        FAILED,

                        "Column Validation"

                    )

                except Exception as e:

                    self.add_failure(
                        full_name,
                        e
                    )

        finally:

            self.finalize()
