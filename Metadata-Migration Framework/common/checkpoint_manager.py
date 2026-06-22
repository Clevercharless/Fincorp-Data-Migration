from pyspark.sql import Row

class CheckpointManager:

    def __init__(

        self,

        storage

    ):

        self.storage = storage

    def load_completed(

        self,

        path

    ):

        if not self.storage.path_exists(
            path
        ):

            return set()

        df = self.storage.load_delta(
            path
        )

        return set(

            row.object_name

            for row in df.collect()

        )

    def mark_complete(

        self,

        object_name,

        path

    ):

        df = spark.createDataFrame(

            [

                Row(
                    object_name=
                        object_name
                )

            ]

        )

        self.storage.save_delta(

            df,

            path,

            mode="append"

        )
