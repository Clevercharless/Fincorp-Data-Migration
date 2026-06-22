# ==========================================
# Common/batch_manager.py
# ==========================================

from pyspark.sql.functions import (
    row_number,
    monotonically_increasing_id,
    col
)

from pyspark.sql.window import Window

import math


class BatchManager:

    @staticmethod
    def get_dataframe_batches(
        df,
        batch_size
    ):
        """
        Memory-safe DataFrame batching.

        Returns DataFrame batches instead of
        collecting all rows to driver.
        """

        window_spec = Window.orderBy(
            monotonically_increasing_id()
        )

        numbered_df = (

            df.withColumn(
                "__row_num",
                row_number().over(
                    window_spec
                )
            )

        )

        total_rows = numbered_df.count()

        batch_count = math.ceil(
            total_rows / batch_size
        )

        for batch_no in range(batch_count):

            start_row = (
                batch_no * batch_size
            ) + 1

            end_row = (
                start_row + batch_size - 1
            )

            yield (

                numbered_df

                .filter(

                    (col("__row_num") >= start_row)
                    &
                    (col("__row_num") <= end_row)

                )

                .drop("__row_num")

            )

    @staticmethod
    def get_batch_count(
        total_records,
        batch_size
    ):

        return math.ceil(
            total_records / batch_size
        )

    @staticmethod
    def print_progress(
        batch_number,
        total_batches
    ):

        print(
            f"Processing Batch "
            f"{batch_number}"
            f"/{total_batches}"
        )
