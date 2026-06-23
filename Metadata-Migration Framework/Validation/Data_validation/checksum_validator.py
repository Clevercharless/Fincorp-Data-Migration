# ==========================================
# checksum_validator.py
# ==========================================

from pyspark.sql.functions import *

%run ./base_data_validator


class ChecksumValidator(
    BaseDataValidator
):

    def __init__(self):

        super().__init__(
            "CHECKSUM_VALIDATION"
        )

    def generate_checksum(

        self,

        table_name

    ):

        df = spark.table(
            table_name
        )

        checksum = (

            df

            .select(

                md5(

                    concat_ws(

                        "||",

                        *df.columns

                    )

                )

                .alias(
                    "hash"
                )

            )

            .agg(

                sum(

                    hash(
                        col("hash")
                    )

                )

            )

            .first()[0]

        )

        return checksum

    def validate(self):

        manifest = self.storage.load_delta(
            MIGRATION_BATCH_PATH
        )

        try:

            for table in manifest.toLocalIterator():

                try:

                    source_table = (

                        f"{table.catalog}."

                        f"{table.schema}."

                        f"{table.table}"

                    )

                    target_table = source_table

                    source_hash = (

                        self.generate_checksum(
                            source_table
                        )

                    )

                    target_hash = (

                        self.generate_checksum(
                            target_table
                        )

                    )

                    self.add_result(

                        table.manifest_id,

                        table.table,

                        source_hash,

                        target_hash,

                        SUCCESS
                        if source_hash
                        ==
                        target_hash
                        else FAILED,

                        "Checksum Validation"

                    )

                except Exception as e:

                    self.add_failure(

                        table.manifest_id,

                        table.table,

                        e

                    )

        finally:

            self.finalize()
