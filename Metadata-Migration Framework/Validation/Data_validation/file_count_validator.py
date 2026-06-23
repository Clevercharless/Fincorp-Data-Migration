# ==========================================
# file_count_validator.py
# ==========================================

%run ./base_data_validator


class FileCountValidator(
    BaseDataValidator
):

    def __init__(self):

        super().__init__(
            "FILE_COUNT_VALIDATION"
        )

    def validate(self):

        manifest = self.storage.load_delta(
            MIGRATION_BATCH_PATH
        )

        try:

            for table in (
                manifest.toLocalIterator()
            ):

                source_count = (
                    self.get_source_file_count(
                        table.source_path
                    )
                )

                target_count = (
                    self.get_target_file_count(
                        table.target_path
                    )
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

                    "File Count Validation"

                )

        finally:

            self.finalize()
