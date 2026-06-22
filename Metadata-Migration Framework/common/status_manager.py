from pyspark.sql import Row

from datetime import datetime

class StatusManager:

    def __init__(
        self,
        storage
    ):

        self.storage = storage

    def write_status(

        self,

        object_name,

        process_name,

        status,

        path,

        error_message=None

    ):

        df = spark.createDataFrame(

            [

                Row(

                    object_name=
                        object_name,

                    process_name=
                        process_name,

                    status=
                        status,

                    error_message=
                        error_message,

                    created_time=
                        datetime.now()

                )

            ]

        )

        self.storage.save_delta(

            df,

            path,

            mode="append"

        )
