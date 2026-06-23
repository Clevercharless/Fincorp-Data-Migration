# ==========================================
# row_count_validator.py
# ==========================================

%run ./base_data_validator


class RowCountValidator(
    BaseDataValidator
):

    def __init__(self):

        super().__init__(
            "ROW_COUNT_VALIDATION"
        )

    def validate(self):

        manifest = self.storage.load_delta(
            MIGRATION_BATCH_PATH
        )

        try:

            for table in manifest.toLocalIterator():

                try:

                    source_df = self.read_source_data(

                        table.source_path,

                        table.format

                    )

                    target_df = self.read_target_data(

                        table.target_path,

                        table.format

                    )

                    source_count = (
                        source_df.count()
                    )

                    target_count = (
                        target_df.count()
                    )

                    self.add_result(

                        table.manifest_id,

                        table.table,

                        source_count,

                        target_count,

                        SUCCESS
                        if source_count
                        ==
                        target_count
                        else FAILED,

                        "Row Count Validation"

                    )

                except Exception as e:

                    self.add_failure(

                        table.manifest_id,

                        table.table,

                        e

                    )

        finally:

            self.finalize()
