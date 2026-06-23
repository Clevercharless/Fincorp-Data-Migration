class CDCReportBuilder:

    def build(self):

        watermarks = storage.load_delta(
            CDC_WATERMARK_PATH
        )

        report = (

            watermarks

            .groupBy()

            .count()

        )

        storage.save_delta(

            report,

            CDC_REPORT_PATH

        )
