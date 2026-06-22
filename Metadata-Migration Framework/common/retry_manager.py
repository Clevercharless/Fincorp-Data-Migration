import time

class RetryManager:

    @staticmethod
    def execute(

        func,

        retries=3,

        wait_seconds=5

    ):

        attempt = 1

        while attempt <= retries:

            try:

                return func()

            except Exception as e:

                print(
                    f"Retry {attempt}"
                )

                if attempt == retries:

                    raise e

                time.sleep(
                    wait_seconds
                )

                attempt += 1
