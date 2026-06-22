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
