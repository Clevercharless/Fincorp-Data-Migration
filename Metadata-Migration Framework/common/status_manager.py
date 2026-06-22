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

        object_type,

        status,

        status_path,

        error_message=None

    ):

        df = spark.createDataFrame(

            [

                Row(

                    object_name=
                        object_name,

                    object_type=
                        object_type,

                    status=
                        status,

                    error_message=
                        error_message,

                    processed_time=
                        datetime.now()

                )

            ]

        )

        self.storage.save_delta(

            df,

            status_path,

            mode="append"

        )
