# ==========================================
# Drivers/metadata_builder_driver.py
# ==========================================

# ------------------------------------------
# Builder Import
# ------------------------------------------

%run ../Builders/metadata_builder

# ------------------------------------------
# Common Imports
# ------------------------------------------

%run ../Common/logger
%run ../Common/constants


logger = Logger()

logger.info(
    "Metadata Builder Driver Started"
)

try:

    builder = MetadataBuilder()

    # --------------------------------------
    # Step 1
    # Build Master Metadata
    # --------------------------------------

    logger.info(
        "Building Master Metadata"
    )

    builder.build_master_metadata()

    # --------------------------------------
    # Step 2
    # Build Metadata Summary
    # --------------------------------------

    logger.info(
        "Building Metadata Summary"
    )

    builder.build_metadata_summary()

    # --------------------------------------
    # Step 3
    # Build Migration Manifest
    # --------------------------------------

    logger.info(
        "Building Migration Manifest"
    )

    builder.build_migration_manifest()

    # --------------------------------------
    # Step 4
    # Build Migration Batches
    # --------------------------------------

    logger.info(
        "Building Migration Batches"
    )

    builder.build_migration_batches()

    # --------------------------------------
    # Step 5
    # Validate Metadata
    # --------------------------------------

    logger.info(
        "Validating Metadata"
    )

    builder.validate_master_metadata()

    logger.info(
        "Metadata Builder Driver Completed"
    )

except Exception as e:

    logger.error(
        f"Metadata Builder Failed : {str(e)}"
    )

    raise
