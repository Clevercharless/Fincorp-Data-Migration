# ==========================================
# permission_validator.py
# ==========================================

%run ./base_validator

class PermissionValidator(BaseValidator):

    def __init__(self):

        super().__init__(
            "PERMISSION_VALIDATION"
        )

    def validate(self):

        try:

            permissions_df = self.storage.load_delta(
                PERMISSION_PATH
            )

            for permission in (
                permissions_df.toLocalIterator()
            ):

                full_name = (

                    f"{permission.catalog}."
                    f"{permission.schema}."
                    f"{permission.table}"

                )

                try:

                    grants = spark.sql(

                        f"""
                        SHOW GRANTS
                        ON TABLE {full_name}
                        """

                    )

                    exists = (

                        grants

                        .filter(
                            col("principal")
                            ==
                            permission.principal
                        )

                        .filter(
                            col("actionType")
                            ==
                            permission.privilege
                        )

                        .count()

                    )

                    self.add_result(

                        full_name,

                        permission.privilege,

                        permission.privilege
                        if exists > 0
                        else
                        "MISSING",

                        SUCCESS
                        if exists > 0
                        else
                        FAILED,

                        "Permission Validation"

                    )

                except Exception as e:

                    self.add_failure(
                        full_name,
                        e
                    )

        finally:

            self.finalize()
