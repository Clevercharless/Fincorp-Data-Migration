# ==========================================
# partition_validator.py
# ==========================================

%run ./base_validator

class PartitionValidator(BaseValidator):

    def __init__(self):

        super().__init__(
            "PARTITION_VALIDATION"
        )

    def validate(self):

        try:

            partitions_df = self.storage.load_delta(
                PARTITION_PATH
            )

            partition_groups = (

                partitions_df

                .groupBy(
                    "catalog",
                    "schema",
                    "table"
                )

                .agg(

                    collect_list(
                        "partition_column"
                    ).alias(
                        "source_partitions"
                    )

                )

            )

            for row in (
                partition_groups.toLocalIterator()
            ):

                full_name = (

                    f"{row.catalog}."
                    f"{row.schema}."
                    f"{row.table}"

                )

                try:

                    target = spark.sql(
                        f"DESCRIBE DETAIL {full_name}"
                    ).first()

                    target_partitions = (
                        target.partitionColumns
                    )

                    self.compare_values(

                        full_name,

                        sorted(
                            row.source_partitions
                        ),

                        sorted(
                            target_partitions
                        )

                    )

                except Exception as e:

                    self.add_failure(
                        full_name,
                        e
                    )

        finally:

            self.finalize()
