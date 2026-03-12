import re
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict
from loguru import logger

_graph_cache = None
_GRAPH_PATH = Path(__file__).parent.parent / "knowledge" / "material_graph.json"


def _load_graph() -> dict:
    """Load and cache the material knowledge graph."""
    global _graph_cache
    if _graph_cache is not None:
        return _graph_cache

    try:
        with open(_GRAPH_PATH, "r", encoding="utf-8") as f:
            _graph_cache = json.load(f)
        logger.info(
            f"Loaded material graph: {len(_graph_cache.get('materials', []))} materials"
        )
    except Exception as e:
        logger.error(f"Failed to load material graph: {e}")
        _graph_cache = {"version": "1.0", "materials": []}

    return _graph_cache


def _save_graph(data: dict) -> None:
    """Save the material graph back to disk."""
    try:
        with open(_GRAPH_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info("Material graph saved to disk")
    except Exception as e:
        logger.error(f"Failed to save material graph: {e}")


def match_material(description: str) -> Tuple[Optional[str], Optional[str]]:
    """Match a description against the knowledge graph.

    Returns (category, material_name) or (None, None) if no match.
    """
    if not description:
        return None, None

    graph = _load_graph()
    normalized = re.sub(r"[^a-zA-Z0-9\s]", " ", description.lower()).strip()

    for material in graph.get("materials", []):
        # Check canonical name
        name_pattern = r"\b" + re.escape(material["name"].lower()) + r"\b"
        if re.search(name_pattern, normalized):
            return material["category"], material["name"]

        # Check all synonyms
        for synonym in material.get("synonyms", []):
            syn_pattern = r"\b" + re.escape(synonym.lower()) + r"\b"
            if re.search(syn_pattern, normalized):
                return material["category"], material["name"]

    return None, None


def learn_material(
    description: str,
    category: str,
    unit: str = "-",
    source: str = "llm",
) -> bool:
    """Add a new material to the knowledge graph (learning loop).

    Called after Gemini classifies something new.
    Returns True if added, False if duplicate.
    """
    if not description or not category or category == "Uncategorized":
        return False

    graph = _load_graph()
    desc_lower = description.lower().strip()

    # Check if already exists (name or synonym match)
    for material in graph.get("materials", []):
        if material["name"].lower() == desc_lower:
            return False
        for synonym in material.get("synonyms", []):
            if synonym.lower() == desc_lower:
                return False

    # Add new entry
    new_entry = {
        "name": description.strip(),
        "category": category,
        "synonyms": [],
        "typical_unit": unit,
        "source": source,
        "learned_at": datetime.now(timezone.utc).isoformat(),
    }

    graph["materials"].append(new_entry)
    _save_graph(graph)

    # Invalidate cache so next load picks up the new data
    global _graph_cache
    _graph_cache = None

    logger.info(f"Learned new material: '{description}' → {category}")
    return True


def graph_stats() -> Dict:
    """Return statistics about the material knowledge graph."""
    graph = _load_graph()
    materials = graph.get("materials", [])

    by_source = {}
    by_category = {}

    for mat in materials:
        src = mat.get("source", "unknown")
        cat = mat.get("category", "Unknown")

        by_source[src] = by_source.get(src, 0) + 1
        by_category[cat] = by_category.get(cat, 0) + 1

    return {
        "total_materials": len(materials),
        "by_source": by_source,
        "by_category": by_category,
    }
