# ==========================================
# storage_location_validator.py
# ==========================================

%run ./base_validator

class StorageLocationValidator(
    BaseValidator
):

    def __init__(self):

        super().__init__(
            "STORAGE_LOCATION_VALIDATION"
        )

    def validate(self):

        try:

            locations_df = self.storage.load_delta(
                STORAGE_LOCATION_PATH
            )

            for row in (
                locations_df.toLocalIterator()
            ):

                full_name = (

                    f"{row.catalog}."
                    f"{row.schema}."
                    f"{row.table}"

                )

                try:

                    detail = spark.sql(

                        f"""
                        DESCRIBE DETAIL
                        {full_name}
                        """

                    ).first()

                    self.compare_values(

                        full_name,

                        row.target_path,

                        detail.location

                    )

                except Exception as e:

                    self.add_failure(
                        full_name,
                        e
                    )

        finally:

            self.finalize()
