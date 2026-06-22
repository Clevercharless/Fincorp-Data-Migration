# ==========================================
# manifest_validator.py
# ==========================================

%run ./base_validator

class ManifestValidator(
    BaseValidator
):

    def __init__(self):

        super().__init__(
            "MANIFEST_VALIDATION"
        )

    def validate(self):

        try:

            manifest = self.storage.load_delta(
                MIGRATION_BATCH_PATH
            )

            duplicate_count = (

                manifest

                .groupBy(
                    "catalog",
                    "schema",
                    "table"
                )

                .count()

                .filter(
                    col("count") > 1
                )

                .count()

            )

            self.compare_values(

                "DUPLICATES",

                0,

                duplicate_count

            )

            missing_paths = (

                manifest

                .filter(
                    col(
                        "target_path"
                    ).isNull()
                )

                .count()

            )

            self.compare_values(

                "TARGET_PATHS",

                0,

                missing_paths

            )

            missing_formats = (

                manifest

                .filter(
                    col(
                        "format"
                    ).isNull()
                )

                .count()

            )

            self.compare_values(

                "FORMATS",

                0,

                missing_formats

            )

        finally:

            self.finalize()
