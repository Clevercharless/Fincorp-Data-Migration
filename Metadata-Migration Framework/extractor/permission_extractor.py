class PermissionExtractor:

    def extract(self):

        tables = self.storage.load_delta(
            TABLE_PATH
        )

        completed = self.checkpoint.load_completed(
            PERMISSION_STATUS_PATH
        )

        permission_rows = []

        failed_rows = []

        for batch in BatchManager.get_batches(
            tables,
            BATCH_SIZE
        ):

            for table in batch:

                full_name = (
                    f"{table.catalog}."
                    f"{table.schema}."
                    f"{table.table}"
                )

                if full_name in completed:
                    continue

                try:

                    grants = RetryManager.execute(

                        lambda:

                        spark.sql(
                            f"""
                            SHOW GRANTS
                            ON TABLE
                            {full_name}
                            """
                        ).collect()

                    )

                    for grant in grants:

                        permission_rows.append(

                            Row(

                                object_type="TABLE",

                                catalog=table.catalog,

                                schema=table.schema,

                                table=table.table,

                                principal=
                                    grant.principal,

                                privilege=
                                    grant.actionType

                            )

                        )

                    self.checkpoint.mark_complete(

                        full_name,

                        PERMISSION_STATUS_PATH

                    )

                except Exception as e:

                    failed_rows.append(

                        Row(

                            object_name=
                                full_name,

                            error=
                                str(e)

                        )

                    )

            if permission_rows:

                self.storage.save_delta(

                    spark.createDataFrame(
                        permission_rows
                    ),

                    PERMISSION_PATH,

                    mode="append"

                )

                permission_rows = []
