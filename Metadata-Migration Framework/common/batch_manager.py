import math

class BatchManager:

    @staticmethod
    def get_batches(
        df,
        batch_size=500
    ):

        rows = df.collect()

        for i in range(

            0,

            len(rows),

            batch_size

        ):

            yield rows[
                i:i+batch_size
            ]

    @staticmethod
    def get_list_batches(
        input_list,
        batch_size=500
    ):

        for i in range(

            0,

            len(input_list),

            batch_size

        ):

            yield input_list[
                i:i+batch_size
            ]

    @staticmethod
    def get_batch_count(

        total_records,

        batch_size

    ):

        return math.ceil(

            total_records /

            batch_size

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
