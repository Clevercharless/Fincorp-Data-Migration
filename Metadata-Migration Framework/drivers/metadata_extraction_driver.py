# ==========================================
# Drivers/metadata_extraction_driver.py
# ==========================================

# ==========================================
# Extractor Imports
# ==========================================

%run ../Extractors/catalog_extractor
%run ../Extractors/schema_extractor
%run ../Extractors/table_extractor
%run ../Extractors/column_extractor
%run ../Extractors/permission_extractor
%run ../Extractors/partition_extractor
%run ../Extractors/storage_location_extractor
%run ../Extractors/lineage_extractor

# ==========================================
# Common Imports
# ==========================================

%run ../Common/logger

logger = Logger()

# ==========================================
# Driver Execution
# ==========================================

logger.info(
    "Metadata Extraction Driver Started"
)

try:

    # ======================================
    # Step 1
    # Catalog Extraction
    # ======================================

    logger.info(
        "Starting Catalog Extraction"
    )

    CatalogExtractor().extract()

    logger.info(
        "Catalog Extraction Completed"
    )

    # ======================================
    # Step 2
    # Schema Extraction
    # ======================================

    logger.info(
        "Starting Schema Extraction"
    )

    SchemaExtractor().extract()

    logger.info(
        "Schema Extraction Completed"
    )

    # ======================================
    # Step 3
    # Table Extraction
    # ======================================

    logger.info(
        "Starting Table Extraction"
    )

    TableExtractor().extract()

    logger.info(
        "Table Extraction Completed"
    )

    # ======================================
    # Step 4
    # Column Extraction
    # ======================================

    logger.info(
        "Starting Column Extraction"
    )

    ColumnExtractor().extract()

    logger.info(
        "Column Extraction Completed"
    )

    # ======================================
    # Step 5
    # Permission Extraction
    # ======================================

    logger.info(
        "Starting Permission Extraction"
    )

    PermissionExtractor().extract()

    logger.info(
        "Permission Extraction Completed"
    )

    # ======================================
    # Step 6
    # Partition Extraction
    # ======================================

    logger.info(
        "Starting Partition Extraction"
    )

    PartitionExtractor().extract()

    logger.info(
        "Partition Extraction Completed"
    )

    # ======================================
    # Step 7
    # Storage Location Extraction
    # ======================================

    logger.info(
        "Starting Storage Location Extraction"
    )

    StorageLocationExtractor().extract()

    logger.info(
        "Storage Location Extraction Completed"
    )

    # ======================================
    # Step 8
    # Lineage Extraction
    # ======================================

    logger.info(
        "Starting Lineage Extraction"
    )

    LineageExtractor().extract()

    logger.info(
        "Lineage Extraction Completed"
    )

    logger.info(
        "Metadata Extraction Driver Completed Successfully"
    )

except Exception as e:

    logger.error(
        f"Metadata Extraction Failed : {str(e)}"
    )

    raise
