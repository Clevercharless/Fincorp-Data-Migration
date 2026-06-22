class PartitionExtractor:

    def extract(self):

        tables = self.storage.load_delta(
            TABLE_PATH
        )

        partition_rows = []

        for batch in BatchManager.get_batches(
            tables,
            BATCH_SIZE
        ):

            for table in batch:

                try:

                    full_name = (
                        f"{table.catalog}."
                        f"{table.schema}."
                        f"{table.table}"
                    )

                    detail = RetryManager.execute(

                        lambda:

                        spark.sql(
                            f"""
                            DESCRIBE DETAIL
                            {full_name}
                            """
                        ).first()

                    )

                    partitions = (

                        detail.partitionColumns

                        if detail.partitionColumns

                        else []

                    )

                    order = 1

                    for part in partitions:

                        partition_rows.append(

                            Row(

                                catalog=
                                    table.catalog,

                                schema=
                                    table.schema,

                                table=
                                    table.table,

                                partition_column=
                                    part,

                                partition_order=
                                    order

                            )

                        )

                        order += 1

                except:
                    pass

            if partition_rows:

                self.storage.save_delta(

                    spark.createDataFrame(
                        partition_rows
                    ),

                    PARTITION_PATH,

                    mode="append"

                )

                partition_rows = []
