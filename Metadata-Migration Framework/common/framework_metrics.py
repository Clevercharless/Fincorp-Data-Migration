from pyspark.sql import Row
from datetime import datetime


class FrameworkMetrics:

    def __init__(self):

        self.start_time = datetime.now()

        self.processed = 0

        self.success = 0

        self.failed = 0

        self.skipped = 0

    def increment_processed(
        self,
        count=1
    ):

        self.processed += count

    def increment_success(
        self,
        count=1
    ):

        self.success += count

    def increment_failed(
        self,
        count=1
    ):

        self.failed += count

    def increment_skipped(
        self,
        count=1
    ):

        self.skipped += count

    def build_metrics_df(

        self,

        process_name

    ):

        end_time = datetime.now()

        duration = (

            end_time -
            self.start_time

        ).total_seconds()

        return spark.createDataFrame(

            [

                Row(

                    process_name=
                        process_name,

                    processed=
                        self.processed,

                    success=
                        self.success,

                    failed=
                        self.failed,

                    skipped=
                        self.skipped,

                    start_time=
                        self.start_time,

                    end_time=
                        end_time,

                    duration_seconds=
                        duration

                )

            ]

        )
