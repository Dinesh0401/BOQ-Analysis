import re
import json
from pathlib import Path
from loguru import logger

_ontology_cache = None
_ONTOLOGY_PATH = Path(__file__).parent.parent / "knowledge" / "boq_ontology.json"


def _load_ontology() -> dict:
    """Load and cache the BOQ ontology."""
    global _ontology_cache
    if _ontology_cache is not None:
        return _ontology_cache

    try:
        with open(_ONTOLOGY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Remove metadata keys
        _ontology_cache = {k: v for k, v in data.items() if not k.startswith("_")}
        logger.info(f"Loaded ontology: {len(_ontology_cache)} categories")
    except Exception as e:
        logger.error(f"Failed to load ontology: {e}")
        _ontology_cache = {}

    return _ontology_cache


def map_to_category(text: str) -> str:
    """Map text to a category using word-boundary regex from the ontology.

    WHY word-boundary: prevents 'conduit' matching inside 'air conditioning'.
    """
    if not text:
        return "Uncategorized"

    ontology = _load_ontology()
    normalized = re.sub(r"[^a-zA-Z0-9\s]", " ", text.lower()).strip()

    for category, keywords in ontology.items():
        for keyword in keywords:
            pattern = r"\b" + re.escape(keyword.lower()) + r"\b"
            if re.search(pattern, normalized):
                logger.debug(f"Ontology match: '{keyword}' → {category}")
                return category

    return "Uncategorized"
