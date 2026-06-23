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

                source_df = self.read_source_data(

    table.source_path,

    table.format

)

target_df = self.read_target_data(

    table.target_path,

    table.format

)

source_count = len(

    source_df.inputFiles()

)

target_count = len(

    target_df.inputFiles()

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
