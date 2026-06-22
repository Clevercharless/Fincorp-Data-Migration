# Storage Paths

ROOT_PATH = "/mnt/migration-control"

CATALOG_PATH = f"{ROOT_PATH}/metadata/catalogs"
SCHEMA_PATH = f"{ROOT_PATH}/metadata/schemas"
TABLE_PATH = f"{ROOT_PATH}/metadata/tables"
COLUMN_PATH = f"{ROOT_PATH}/metadata/columns"
PERMISSION_PATH = f"{ROOT_PATH}/metadata/permissions"
LINEAGE_PATH = f"{ROOT_PATH}/metadata/lineage"

MASTER_PATH = f"{ROOT_PATH}/master_metadata"

# Status

STATUS_ROOT = f"{ROOT_PATH}/status"

CATALOG_STATUS_PATH = f"{STATUS_ROOT}/catalog_status"
SCHEMA_STATUS_PATH = f"{STATUS_ROOT}/schema_status"
TABLE_STATUS_PATH = f"{STATUS_ROOT}/table_status"
COLUMN_STATUS_PATH = f"{STATUS_ROOT}/column_status"
PERMISSION_STATUS_PATH = f"{STATUS_ROOT}/permission_status"

# Failed

FAILED_ROOT = f"{ROOT_PATH}/failed"

FAILED_CATALOG_PATH = f"{FAILED_ROOT}/failed_catalogs"
FAILED_SCHEMA_PATH = f"{FAILED_ROOT}/failed_schemas"
FAILED_TABLE_PATH = f"{FAILED_ROOT}/failed_tables"
FAILED_COLUMN_PATH = f"{FAILED_ROOT}/failed_columns"
FAILED_PERMISSION_PATH = f"{FAILED_ROOT}/failed_permissions"

# Retry

MAX_RETRIES = 3
