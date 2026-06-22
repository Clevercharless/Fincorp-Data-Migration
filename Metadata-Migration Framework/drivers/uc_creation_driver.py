# ==========================================
# Drivers/uc_creation_driver.py
# ==========================================

# ------------------------------------------
# UC Modules
# ------------------------------------------

%run ../UC_Creation/storage_credential_creator

%run ../UC_Creation/external_location_creator

%run ../UC_Creation/catalog_creator

%run ../UC_Creation/schema_creator

%run ../UC_Creation/table_creator

%run ../UC_Creation/permission_creator

%run ../UC_Creation/uc_creation_validator

# ------------------------------------------
# Common
# ------------------------------------------

%run ../Common/logger


logger = Logger()

logger.info(
    "UC Creation Driver Started"
)

try:

    # --------------------------------------
    # Storage Credential
    # --------------------------------------

    logger.info(
        "Creating Storage Credential"
    )

    StorageCredentialCreator().create()

    # --------------------------------------
    # External Location
    # --------------------------------------

    logger.info(
        "Creating External Location"
    )

    ExternalLocationCreator().create()

    # --------------------------------------
    # Catalogs
    # --------------------------------------

    logger.info(
        "Creating Catalogs"
    )

    CatalogCreator().create()

    # --------------------------------------
    # Schemas
    # --------------------------------------

    logger.info(
        "Creating Schemas"
    )

    SchemaCreator().create()

    # --------------------------------------
    # Tables
    # --------------------------------------

    logger.info(
        "Creating Tables"
    )

    TableCreator().create()

    # --------------------------------------
    # Permissions
    # --------------------------------------

    logger.info(
        "Applying Permissions"
    )

    PermissionCreator().create()

    # --------------------------------------
    # Validation
    # --------------------------------------

    logger.info(
        "Validating UC Creation"
    )

    UCCreationValidator().validate()

    logger.info(
        "UC Creation Completed Successfully"
    )

except Exception as e:

    logger.error(

        f"UC Creation Failed : {str(e)}"

    )

    raise
