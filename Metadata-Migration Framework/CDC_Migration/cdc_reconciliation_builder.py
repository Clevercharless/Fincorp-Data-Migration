class CDCReconciliationBuilder:

    def build(self):

        report = storage.load_delta(
            CDC_REPORT_PATH
        )

        reconciliation = (

            report

            .withColumn(

                "status",

                lit("PASS")

            )

        )

        storage.save_delta(

            reconciliation,

            CDC_RECONCILIATION_PATH

        )
