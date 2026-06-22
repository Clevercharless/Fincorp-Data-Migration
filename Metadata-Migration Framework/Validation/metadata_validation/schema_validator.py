# ==========================================
# schema_validator.py
# ==========================================

%run ./base_validator

class SchemaValidator(BaseValidator):

    def __init__(self):

        super().__init__(
            "SCHEMA_VALIDATION"
        )

    def validate(self):

        try:

            source_df = (

                self.storage.load_delta(
                    SCHEMA_PATH
                )

                .select(
                    "catalog",
                    "schema"
                )

                .distinct()

            )

            source_count = source_df.count()

            target_count = 0

            for row in (
                source_df.toLocalIterator()
            ):

                try:

                    schema_exists = (

                        spark.sql(

                            f"""
                            SHOW SCHEMAS
                            IN {row.catalog}
                            """

                        )

                        .filter(

                            col(
                                "databaseName"
                            )

                            ==
                            row.schema

                        )

                        .count()

                    )

                    if schema_exists > 0:

                        target_count += 1

                except:

                    pass

            self.compare_values(

                "SCHEMA_COUNT",

                source_count,

                target_count

            )

        except Exception as e:

            self.add_failure(

                "SCHEMA_VALIDATION",

                e

            )

            raise

        finally:

            self.finalize()
