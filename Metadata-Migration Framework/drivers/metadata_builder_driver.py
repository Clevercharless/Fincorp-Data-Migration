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

logger = Logger()

logger.info(
    "Metadata Builder Driver Started"
)

try:

    # --------------------------------------
    # Initialize Builder
    # --------------------------------------

    builder = MetadataBuilder()

    # --------------------------------------
    # Execute Full Builder Workflow
    # --------------------------------------

    logger.info(
        "Executing Metadata Builder Workflow"
    )

    builder.run()

    logger.info(
        "Metadata Builder Driver Completed Successfully"
    )

except Exception as e:

    logger.error(
        f"Metadata Builder Failed : {str(e)}"
    )

    raise
