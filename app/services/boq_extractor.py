import re
import json
import pandas as pd
from typing import List, Dict, Tuple
from collections import defaultdict
from pathlib import Path
from loguru import logger

from app.utils.text_cleaner import clean_text, is_valid_product, is_section_header
from app.utils.data_cleaner import clean_dataframe_structure
from app.services.column_identifier import identify_columns
from app.services.category_classifier import classify_category
from app.config.settings import MAX_REASONABLE_QUANTITY, EPC_CATEGORY_RULES

# ─── Load knowledge bases once for material scanning ─────────
_ONTOLOGY_PATH = Path(__file__).parent.parent / "knowledge" / "boq_ontology.json"
_GRAPH_PATH = Path(__file__).parent.parent / "knowledge" / "material_graph.json"


def _build_material_lookup() -> List[Tuple[str, str, str]]:
    """Build a lookup of (keyword, clean_name, category) from all knowledge sources.

    Sorted longest-first so longer matches take priority.
    """
    entries = []

    # From material_graph.json — richest source (canonical names + synonyms)
    try:
        with open(_GRAPH_PATH, "r", encoding="utf-8") as f:
            graph = json.load(f)
        for mat in graph.get("materials", []):
            name = mat["name"]
            cat = mat["category"]
            entries.append((name.lower(), name, cat))
            for syn in mat.get("synonyms", []):
                entries.append((syn.lower(), name, cat))
    except Exception:
        pass

    # From boq_ontology.json
    try:
        with open(_ONTOLOGY_PATH, "r", encoding="utf-8") as f:
            ontology = json.load(f)
        for cat, keywords in ontology.items():
            if cat.startswith("_"):
                continue
            for kw in keywords:
                if not any(e[0] == kw.lower() for e in entries):
                    clean_name = kw.title()
                    entries.append((kw.lower(), clean_name, cat))
    except Exception:
        pass

    # From EPC_CATEGORY_RULES — broadest coverage, only multi-word keywords
    for cat, keywords in EPC_CATEGORY_RULES.items():
        for kw in keywords:
            if len(kw) >= 5 and not any(e[0] == kw.lower() for e in entries):
                clean_name = kw.title()
                entries.append((kw.lower(), clean_name, cat))

    # Sort longest first — "fire alarm system" before "fire alarm" before "fire"
    entries.sort(key=lambda x: len(x[0]), reverse=True)

    logger.info(f"Material lookup built: {len(entries)} entries")
    return entries


_MATERIAL_LOOKUP = _build_material_lookup()


def extract_materials_from_text(text: str) -> List[Dict]:
    """Scan a long text and extract all known materials mentioned in it.

    Returns list of {description, category} for each unique material found.
    Uses word-boundary matching, longest-match-first.
    """
    if not text or len(text) < 8:
        return []

    text_lower = re.sub(r"[^a-zA-Z0-9\s]", " ", text.lower())
    text_lower = re.sub(r"\s+", " ", text_lower).strip()

    found = []
    found_names = set()  # Avoid duplicate clean names

    for keyword, clean_name, category in _MATERIAL_LOOKUP:
        if clean_name in found_names:
            continue
        # Word-boundary match
        pattern = r"\b" + re.escape(keyword) + r"\b"
        if re.search(pattern, text_lower):
            found.append({"description": clean_name, "category": category})
            found_names.add(clean_name)

    return found


def _parse_quantity(value) -> float:
    """Safely parse a quantity value to float."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return min(float(value), MAX_REASONABLE_QUANTITY)
    if isinstance(value, str):
        cleaned = re.sub(r"[^\d.\-]", "", value.strip())
        try:
            return min(float(cleaned), MAX_REASONABLE_QUANTITY) if cleaned else 0.0
        except ValueError:
            return 0.0
    return 0.0


def merge_multiline_descriptions(df: pd.DataFrame, desc_col: str) -> pd.DataFrame:
    """Merge rows where the description continues across multiple lines."""
    if desc_col not in df.columns:
        return df

    merged_rows = []
    buffer_desc = ""
    buffer_row = None

    for _, row in df.iterrows():
        desc = str(row.get(desc_col, "")).strip()
        if not desc or desc == "nan":
            continue

        has_numeric = False
        for col in df.columns:
            if col == desc_col:
                continue
            val = row.get(col)
            if pd.notna(val):
                try:
                    float(str(val).replace(",", ""))
                    has_numeric = True
                    break
                except (ValueError, TypeError):
                    pass

        if has_numeric:
            if buffer_desc and buffer_row is not None:
                buffer_row_copy = buffer_row.copy()
                buffer_row_copy[desc_col] = buffer_desc
                merged_rows.append(buffer_row_copy)
            buffer_desc = desc
            buffer_row = row
        else:
            if buffer_desc:
                buffer_desc += " " + desc
            else:
                buffer_desc = desc
                buffer_row = row

    if buffer_desc and buffer_row is not None:
        buffer_row_copy = buffer_row.copy()
        buffer_row_copy[desc_col] = buffer_desc
        merged_rows.append(buffer_row_copy)

    if merged_rows:
        return pd.DataFrame(merged_rows).reset_index(drop=True)
    return df


def extract_items(
    df: pd.DataFrame,
    header_row: int,
    field_mapping: Dict[str, List[str]],
    threshold: int = 70,
) -> List[Dict]:
    """Extract BOQ items from a DataFrame.

    For long descriptions (>80 chars): scan for known materials → extract each
    For short descriptions: validate and classify normally
    """
    items = []

    # Set header row as columns
    if header_row > 0 and header_row < len(df):
        new_header = df.iloc[header_row].astype(str).tolist()
        df = df.iloc[header_row + 1 :].reset_index(drop=True)
        df.columns = new_header
    else:
        df.columns = [str(c) for c in df.columns]

    # Clean dataframe
    df = clean_dataframe_structure(df)

    if df.empty:
        return items

    # Identify columns via fuzzy matching
    col_map = identify_columns(df.columns.tolist(), field_mapping, threshold)

    desc_col = col_map.get("description")
    brand_col = col_map.get("brand")
    qty_col = col_map.get("quantity")
    unit_col = col_map.get("unit")

    if not desc_col:
        logger.warning("No description column found — skipping sheet")
        return items

    # Merge multiline descriptions
    df = merge_multiline_descriptions(df, desc_col)

    for _, row in df.iterrows():
        raw_desc = str(row.get(desc_col, "")).strip()
        description = clean_text(raw_desc)

        if not description or description == "nan":
            continue

        # Skip section headers / building descriptions everywhere
        if is_section_header(description):
            continue

        quantity = _parse_quantity(row.get(qty_col)) if qty_col else 0.0
        brand = _get_brand(row, brand_col)
        unit = str(row.get(unit_col, "-")).strip() if unit_col else "-"
        if not unit or unit == "nan":
            unit = "-"

        # ── For long text: extract known materials from the paragraph ──
        if len(description) > 80:
            found_materials = extract_materials_from_text(description)
            if found_materials:
                for mat in found_materials:
                    items.append({
                        "description": mat["description"],
                        "brand": brand,
                        "quantity": quantity,
                        "unit": unit,
                        "category": mat["category"],
                    })
                continue  # Skip the raw paragraph
            # If no materials found in long text, fall through to validation
            # but only accept if it looks like a real product
            if not is_valid_product(description):
                continue

        # ── Short descriptions: normal validation + classify ──
        if not is_valid_product(description):
            continue

        category = classify_category(description)

        items.append({
            "description": description,
            "brand": brand,
            "quantity": quantity,
            "unit": unit,
            "category": category,
        })

    logger.info(f"Extracted {len(items)} items from sheet")
    return items


def _get_brand(row, brand_col) -> str:
    """Extract brand from row, default to 'Generic'."""
    if not brand_col:
        return "Generic"
    val = str(row.get(brand_col, "Generic")).strip()
    return val if val and val != "nan" else "Generic"


def group_by_category(items: List[Dict]) -> Dict[str, List[Dict]]:
    """Group items by their category."""
    grouped = defaultdict(list)
    for item in items:
        category = item.get("category", "Uncategorized")
        grouped[category].append(item)
    return dict(grouped)
