import time

from Common.config import (
    MAX_RETRIES,
    RETRY_WAIT_SECONDS
)

class RetryManager:

    @staticmethod
    def execute(func):

        attempt = 1

        while attempt <= MAX_RETRIES:

            try:

                return func()

            except Exception as e:

                if attempt == MAX_RETRIES:

                    raise e

                time.sleep(
                    RETRY_WAIT_SECONDS
                )

                attempt += 1
