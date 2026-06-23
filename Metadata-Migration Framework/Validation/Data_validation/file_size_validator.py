# ==========================================
# file_size_validator.py
# ==========================================

%run ./base_data_validator


class FileSizeValidator(
    BaseDataValidator
):

    def __init__(self):

        super().__init__(
            "FILE_SIZE_VALIDATION"
        )

    def validate(self):

        manifest = self.storage.load_delta(
            MIGRATION_BATCH_PATH
        )

        try:

            for table in (
                manifest.toLocalIterator()
            ):

                source_size = (
                    self.get_source_size(
                        table.source_path
                    )
                )

                target_size = (
                    self.get_target_size(
                        table.target_path
                    )
                )

                self.add_result(

                    table.manifest_id,

                    table.table,

                    source_size,

                    target_size,

                    SUCCESS
                    if source_size
                    ==
                    target_size
                    else FAILED,

                    "File Size Validation"

                )

        finally:

            self.finalize()
