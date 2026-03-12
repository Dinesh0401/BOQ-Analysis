import pandas as pd
from typing import Dict, List
from loguru import logger

from app.services.boq_table_detector import detect_header_row
from app.services.boq_extractor import extract_items, group_by_category
from app.utils.product_normalizer import consolidate_duplicates
from app.config.settings import get_config


def process_excel(file_path: str, industry: str = "construction") -> Dict:
    """Process an entire Excel workbook — loop every sheet.

    Returns:
        {
            "total_sheets": int,
            "sheets_with_data": int,
            "extracted_items": int,
            "items": [...],
            "categories": {...}
        }
    """
    config = get_config(industry)
    field_mapping = config["field_mapping"]
    threshold = config["thresholds"]["fuzzy_match_threshold"]

    try:
        xls = pd.ExcelFile(file_path)
    except Exception as e:
        logger.error(f"Failed to open Excel file: {e}")
        return {
            "total_sheets": 0,
            "sheets_with_data": 0,
            "extracted_items": 0,
            "items": [],
            "categories": {},
        }

    all_items: List[Dict] = []
    sheets_with_data = 0

    for sheet_name in xls.sheet_names:
        logger.info(f"Processing sheet: {sheet_name}")

        try:
            df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
        except Exception as e:
            logger.warning(f"Failed to read sheet '{sheet_name}': {e}")
            continue

        if df.empty or df.shape[0] < 2:
            logger.info(f"Sheet '{sheet_name}' is empty or too small — skipping")
            continue

        # Detect header row
        header_row = detect_header_row(df)

        # Extract items
        items = extract_items(df, header_row, field_mapping, threshold)

        if items:
            sheets_with_data += 1
            all_items.extend(items)
            logger.info(
                f"Sheet '{sheet_name}': {len(items)} items extracted"
            )

    # Consolidate duplicates across sheets
    all_items = consolidate_duplicates(all_items)

    # Group by category
    categories = group_by_category(all_items)

    result = {
        "total_sheets": len(xls.sheet_names),
        "sheets_with_data": sheets_with_data,
        "extracted_items": len(all_items),
        "items": all_items,
        "categories": categories,
    }

    logger.info(
        f"Excel processed: {result['total_sheets']} sheets, "
        f"{result['sheets_with_data']} with data, "
        f"{result['extracted_items']} items"
    )

    return result
