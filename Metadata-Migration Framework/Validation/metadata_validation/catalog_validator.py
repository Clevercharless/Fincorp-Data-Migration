# ==========================================
# catalog_validator.py
# ==========================================

%run ./base_validator

class CatalogValidator(BaseValidator):

    def __init__(self):

        super().__init__(
            "CATALOG_VALIDATION"
        )

    def validate(self):

        try:

            source_count = (

                self.storage

                .load_delta(
                    CATALOG_PATH
                )

                .count()

            )

            target_count = (

                spark.sql(
                    "SHOW CATALOGS"
                )

                .count()

            )

            self.compare_values(

                "CATALOG_COUNT",

                source_count,

                target_count

            )

        except Exception as e:

            self.add_failure(

                "CATALOG_VALIDATION",

                e

            )

            raise

        finally:

            self.finalize()
