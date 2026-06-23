# ==========================================
# PATHS
# ==========================================

ROOT_PATH = "/mnt/migration_framework"

METADATA_ROOT = f"{ROOT_PATH}/metadata"

MASTER_PATH = f"{ROOT_PATH}/master_metadata"

STATUS_ROOT = f"{ROOT_PATH}/status"

FAILED_ROOT = f"{ROOT_PATH}/failed"

REPORT_ROOT = f"{ROOT_PATH}/reports"

CHECKPOINT_ROOT = f"{ROOT_PATH}/checkpoints"

METRICS_ROOT = (
    f"{ROOT_PATH}/metrics"
)

# ==========================================
# METADATA PATHS
# ==========================================

CATALOG_PATH = f"{METADATA_ROOT}/catalogs"

SCHEMA_PATH = f"{METADATA_ROOT}/schemas"

TABLE_PATH = f"{METADATA_ROOT}/tables"

COLUMN_PATH = f"{METADATA_ROOT}/columns"

PERMISSION_PATH = f"{METADATA_ROOT}/permissions"

PARTITION_PATH = f"{METADATA_ROOT}/partitions"

STORAGE_LOCATION_PATH = f"{METADATA_ROOT}/storage_locations"

LINEAGE_PATH = f"{METADATA_ROOT}/lineage"

# ==========================================
# FAILED PATHS
# ==========================================

FAILED_TABLE_PATH = f"{FAILED_ROOT}/failed_tables"

FAILED_COLUMN_PATH = f"{FAILED_ROOT}/failed_columns"

FAILED_PERMISSION_PATH = f"{FAILED_ROOT}/failed_permissions"

FAILED_PARTITION_PATH = f"{FAILED_ROOT}/failed_partitions"

FAILED_STORAGE_PATH = f"{FAILED_ROOT}/failed_storage"

# ==========================================
# STATUS PATHS
# ==========================================

CATALOG_STATUS_PATH = f"{STATUS_ROOT}/catalog_status"

SCHEMA_STATUS_PATH = f"{STATUS_ROOT}/schema_status"

TABLE_STATUS_PATH = f"{STATUS_ROOT}/table_status"

COLUMN_STATUS_PATH = f"{STATUS_ROOT}/column_status"

PERMISSION_STATUS_PATH = f"{STATUS_ROOT}/permission_status"

PARTITION_STATUS_PATH = f"{STATUS_ROOT}/partition_status"

STORAGE_STATUS_PATH = f"{STATUS_ROOT}/storage_status"

MASTER_STATUS_PATH = f"{STATUS_ROOT}/master_status"

METADATA_METRICS_PATH = (
    f"{METRICS_ROOT}/metadata"
)

UC_METRICS_PATH = (
    f"{METRICS_ROOT}/uc_creation"
)

VALIDATION_METRICS_PATH = (
    f"{METRICS_ROOT}/validation"
)

MIGRATION_METRICS_PATH = (
    f"{METRICS_ROOT}/migration"
)

RECONCILIATION_METRICS_PATH = (
    f"{METRICS_ROOT}/reconciliation"
)

# ==========================================
# AUDIT
# ==========================================

AUDIT_ROOT = (
    f"{ROOT_PATH}/audit"
)

AUDIT_PATH = (
    f"{AUDIT_ROOT}/migration_audit"
)

# ==========================================
# LINEAGE
# ==========================================

LINEAGE_PATH = (
    f"{METADATA_ROOT}/lineage"
)

FAILED_LINEAGE_PATH = (
    f"{FAILED_ROOT}/failed_lineage"
)

LINEAGE_STATUS_PATH = (
    f"{STATUS_ROOT}/lineage_status"
)

# ==========================================
# STORAGE MAPPING
# ==========================================

SOURCE_STORAGE_PREFIX = (
    "abfss://container"
)

TARGET_STORAGE_PREFIX = (
    "s3://migration-bucket"
)

# ==========================================
# MASTER METADATA
# ==========================================

MASTER_METADATA_PATH = (
    f"{MASTER_PATH}/master_metadata"
)

METADATA_SUMMARY_PATH = (
    f"{MASTER_PATH}/metadata_summary"
)

MIGRATION_MANIFEST_PATH = (
    f"{MASTER_PATH}/migration_manifest"
)

MIGRATION_BATCH_PATH = (
    f"{MASTER_PATH}/migration_batches"
)

# ==========================================
# MIGRATION SETTINGS
# ==========================================

MIGRATION_BATCH_SIZE = 100

# ==========================================
# FRAMEWORK SETTINGS
# ==========================================

BATCH_SIZE = 500

MAX_RETRIES = 3

RETRY_WAIT_SECONDS = 5

# ==========================================
# UC CREATION STATUS
# ==========================================

STORAGE_CREDENTIAL_STATUS_PATH = (
    f"{STATUS_ROOT}/storage_credential_status"
)

EXTERNAL_LOCATION_STATUS_PATH = (
    f"{STATUS_ROOT}/external_location_status"
)

# ==========================================
# UC CREATION CHECKPOINTS
# ==========================================

STORAGE_CREDENTIAL_CHECKPOINT_PATH = (
    f"{CHECKPOINT_ROOT}/storage_credential_checkpoint"
)

EXTERNAL_LOCATION_CHECKPOINT_PATH = (
    f"{CHECKPOINT_ROOT}/external_location_checkpoint"
)


# ==========================================
# UC CREATION STATUS
# ==========================================

CATALOG_CREATION_STATUS_PATH = (
    f"{STATUS_ROOT}/catalog_creation_status"
)

SCHEMA_CREATION_STATUS_PATH = (
    f"{STATUS_ROOT}/schema_creation_status"
)

# ==========================================
# UC CREATION CHECKPOINTS
# ==========================================

CATALOG_CREATION_CHECKPOINT_PATH = (
    f"{CHECKPOINT_ROOT}/catalog_creation_checkpoint"
)

SCHEMA_CREATION_CHECKPOINT_PATH = (
    f"{CHECKPOINT_ROOT}/schema_creation_checkpoint"
)

# ==========================================
# UC CREATION FAILURES
# ==========================================

FAILED_CATALOG_CREATION_PATH = (
    f"{FAILED_ROOT}/catalog_creation_failures"
)

FAILED_SCHEMA_CREATION_PATH = (
    f"{FAILED_ROOT}/schema_creation_failures"
)

# ==========================================
# UC TABLE CREATION
# ==========================================

TABLE_CREATION_STATUS_PATH = (
    f"{STATUS_ROOT}/table_creation_status"
)

TABLE_CREATION_CHECKPOINT_PATH = (
    f"{CHECKPOINT_ROOT}/table_creation_checkpoint"
)

FAILED_TABLE_CREATION_PATH = (
    f"{FAILED_ROOT}/table_creation_failures"
)

# ==========================================
# PERMISSION CREATION
# ==========================================

PERMISSION_CREATION_STATUS_PATH = (
    f"{STATUS_ROOT}/permission_creation_status"
)

PERMISSION_CREATION_CHECKPOINT_PATH = (
    f"{CHECKPOINT_ROOT}/permission_creation_checkpoint"
)

FAILED_PERMISSION_CREATION_PATH = (
    f"{FAILED_ROOT}/permission_creation_failures"
)


# ==========================================
# UC VALIDATION
# ==========================================

UC_VALIDATION_PATH = (
    f"{VALIDATION_ROOT}/uc_validation"
)

UC_VALIDATION_STATUS_PATH = (
    f"{STATUS_ROOT}/uc_validation_status"
)

FAILED_UC_VALIDATION_PATH = (
    f"{FAILED_ROOT}/uc_validation_failures"
)

# ==========================================
# VALIDATION OUTPUTS
# ==========================================

VALIDATION_RESULTS_PATH = (
    f"{VALIDATION_ROOT}/validation_results"
)

FAILED_VALIDATION_PATH = (
    f"{VALIDATION_ROOT}/validation_failures"
)

VALIDATION_METRICS_PATH = (
    f"{VALIDATION_ROOT}/validation_metrics"
)

METADATA_VALIDATION_REPORT_PATH = (
    f"{VALIDATION_ROOT}/metadata_validation_report"
)

METADATA_RECONCILIATION_PATH = (
    f"{VALIDATION_ROOT}/metadata_reconciliation"
)

# ==========================================
# HISTORICAL MIGRATION
# ==========================================

HISTORICAL_BATCH_PATH = (
    f"{MIGRATION_ROOT}/historical_batches"
)

HISTORICAL_BATCH_SIZE_GB = 500

HISTORICAL_MIGRATION_STATUS_PATH = (
    f"{STATUS_ROOT}/historical_migration_status"
)

HISTORICAL_CHECKPOINT_PATH = (
    f"{CHECKPOINT_ROOT}/historical_checkpoint"
)

FAILED_MIGRATION_PATH = (
    f"{FAILED_ROOT}/historical_migration_failures"
)

MIGRATION_REPORT_PATH = (
    f"{REPORT_ROOT}/historical_migration_report"
)

MIGRATION_RECONCILIATION_PATH = (
    f"{RECONCILIATION_ROOT}/migration_reconciliation"
)

# ==========================================
# DATASYNC
# ==========================================

DATASYNC_TASK_PATH = (
    f"{MIGRATION_ROOT}/datasync_tasks"
)

DATASYNC_EXECUTION_PATH = (
    f"{MIGRATION_ROOT}/datasync_executions"
)

DATASYNC_STATUS_PATH = (
    f"{STATUS_ROOT}/datasync_status"
)

FAILED_DATASYNC_PATH = (
    f"{FAILED_ROOT}/datasync_failures"
)

MAX_RETRY_COUNT = 3


FAILED_BATCH_PATH = (
    f"{FAILED_ROOT}/failed_batches"
)

FAILED_EXECUTION_PATH = (
    f"{FAILED_ROOT}/failed_executions"
)

# ==========================================
# DATA VALIDATION
# ==========================================

HISTORICAL_VALIDATION_RESULTS_PATH = (
    f"{VALIDATION_ROOT}/historical_validation_results"
)

HISTORICAL_VALIDATION_FAILURE_PATH = (
    f"{VALIDATION_ROOT}/historical_validation_failures"
)

HISTORICAL_VALIDATION_METRICS_PATH = (
    f"{VALIDATION_ROOT}/historical_validation_metrics"
)

HISTORICAL_RECONCILIATION_PATH = (
    f"{RECONCILIATION_ROOT}/historical_reconciliation"
)

DATA_RECONCILIATION_PATH = (
    f"{RECONCILIATION_ROOT}/data_reconciliation"
)

DATA_RECONCILIATION_REPORT_PATH = (
    f"{RECONCILIATION_ROOT}/data_reconciliation_report"
)
