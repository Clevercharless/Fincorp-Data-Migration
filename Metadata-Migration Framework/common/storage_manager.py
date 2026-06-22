class StorageManager:

    def save_delta(
        self,
        df,
        path,
        mode="overwrite"
    ):

        (
            df.write
            .format("delta")
            .mode(mode)
            .save(path)
        )

    def load_delta(
        self,
        path
    ):

        return (
            spark.read
            .format("delta")
            .load(path)
        )

    def path_exists(
        self,
        path
    ):

        try:

            dbutils.fs.ls(path)

            return True

        except:

            return False
