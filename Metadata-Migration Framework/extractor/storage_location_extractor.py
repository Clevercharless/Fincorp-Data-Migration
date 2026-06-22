class StorageLocationExtractor:

    def extract(self):

        tables = self.storage.load_delta(
            TABLE_PATH
        )

        rows = []

        for batch in BatchManager.get_batches(
            tables,
            BATCH_SIZE
        ):

            for table in batch:

                try:

                    source_path = (
                        table.location
                    )

                    target_path = (

                        source_path.replace(

                            SOURCE_STORAGE_PREFIX,

                            TARGET_STORAGE_PREFIX

                        )

                    )

                    rows.append(

                        Row(

                            catalog=
                                table.catalog,

                            schema=
                                table.schema,

                            table=
                                table.table,

                            source_path=
                                source_path,

                            target_path=
                                target_path

                        )

                    )

                except:
                    pass

            if rows:

                self.storage.save_delta(

                    spark.createDataFrame(
                        rows
                    ),

                    STORAGE_LOCATION_PATH,

                    mode="append"

                )

                rows = []
