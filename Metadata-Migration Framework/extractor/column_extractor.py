class ColumnExtractor:

    def __init__(self):

        self.logger = Logger()
        self.storage = StorageManager()
        self.checkpoint = CheckpointManager(self.storage)
        self.status = StatusManager(self.storage)
        self.metrics = FrameworkMetrics()
        self.audit = AuditManager(self.storage)

    def extract(self):

        tables = self.storage.load_delta(TABLE_PATH)

        completed = self.checkpoint.load_completed(
            COLUMN_STATUS_PATH
        )

        failed_rows = []

        total_batches = BatchManager.get_batch_count(
            tables.count(),
            BATCH_SIZE
        )

        batch_no = 1

        for batch in BatchManager.get_batches(
            tables,
            BATCH_SIZE
        ):

            BatchManager.print_progress(
                batch_no,
                total_batches
            )

            column_rows = []

            for table in batch:

                full_name = (
                    f"{table.catalog}."
                    f"{table.schema}."
                    f"{table.table}"
                )

                if full_name in completed:
                    self.metrics.increment_skipped()
                    continue

                try:

                    columns = RetryManager.execute(

                        lambda:

                        spark.sql(
                            f"DESCRIBE TABLE {full_name}"
                        ).collect()

                    )

                    position = 1

                    for col in columns:

                        if (
                            col.col_name is None
                            or
                            col.col_name.startswith("#")
                        ):
                            continue

                        column_rows.append(

                            Row(

                                catalog=table.catalog,
                                schema=table.schema,
                                table=table.table,

                                column_name=col.col_name,

                                data_type=col.data_type,

                                nullable=True,

                                ordinal_position=position,

                                comment=col.comment

                            )

                        )

                        position += 1

                    self.checkpoint.mark_complete(
                        full_name,
                        COLUMN_STATUS_PATH
                    )

                    self.metrics.increment_success()

                except Exception as e:

                    failed_rows.append(
                        Row(
                            object_name=full_name,
                            error=str(e)
                        )
                    )

                    self.metrics.increment_failed()

            if column_rows:

                self.storage.save_delta(

                    spark.createDataFrame(
                        column_rows
                    ),

                    COLUMN_PATH,

                    mode="append"

                )

            batch_no += 1

        if failed_rows:

            self.storage.save_delta(

                spark.createDataFrame(
                    failed_rows
                ),

                FAILED_COLUMN_PATH

            )

        self.metrics.save_metrics(

            "COLUMN_EXTRACTION",

            self.storage,

            METADATA_METRICS_PATH

        )
