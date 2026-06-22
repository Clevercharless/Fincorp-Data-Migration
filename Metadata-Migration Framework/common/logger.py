from datetime import datetime

class Logger:

    @staticmethod
    def info(message):

        print(
            f"[INFO]"
            f"[{datetime.now()}]"
            f" {message}"
        )

    @staticmethod
    def error(message):

        print(
            f"[ERROR]"
            f"[{datetime.now()}]"
            f" {message}"
        )
