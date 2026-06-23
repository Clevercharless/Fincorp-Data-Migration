# ==========================================
# cdc_driver.py
# ==========================================

%run ./cdc_manifest_builder
%run ./cdc_batch_builder
%run ./cdc_executor
%run ./cdc_report_builder
%run ./cdc_reconciliation_builder

CDCManifestBuilder().build()

CDCBatchBuilder().build()

CDCExecutor().execute()

CDCReportBuilder().build()

CDCReconciliationBuilder().build()
