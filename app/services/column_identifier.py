from typing import Dict, List
from loguru import logger
from app.utils.fuzzy_matcher import fuzzy_match


def identify_columns(
    column_list: List[str],
    field_mapping: Dict[str, List[str]],
    threshold: int = 70,
) -> Dict[str, str]:
    """Fuzzy-match actual Excel column names to expected field names.

    Args:
        column_list: List of actual column names from the Excel sheet.
        field_mapping: Dict mapping field names to lists of possible aliases.
        threshold: Minimum fuzzy match score (0-100).

    Returns:
        Dict mapping field name → actual column name.
        e.g. {"description": "Item Description", "quantity": "Qty"}
    """
    identified = {}

    for field_name, aliases in field_mapping.items():
        best_match = None
        best_score = 0

        for col in column_list:
            col_clean = str(col).strip()
            match = fuzzy_match(col_clean, aliases, threshold=threshold)
            if match:
                identified[field_name] = col_clean
                logger.debug(f"Column '{col_clean}' → field '{field_name}'")
                best_match = col_clean
                break

        if not best_match:
            logger.debug(f"No column match found for field '{field_name}'")

    return identified
