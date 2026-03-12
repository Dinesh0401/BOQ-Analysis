import re
from typing import List
from loguru import logger

SOFT_SPLIT_PHRASES = [
    "including", "complete with", "along with",
    "with all accessories", "with necessary", "with all necessary",
    "together with", "as well as",
]

DISCARD_PREFIXES = [
    "as per", "refer", "all complete", "note",
    "clause", "vide", "approved by", "directed by", "etc", "and all",
]


def split_paragraph(text: str) -> List[str]:
    """Split a multi-system BOQ cell into individual material segments."""
    if not text or not isinstance(text, str):
        return []

    # 1. Normalize whitespace
    text = re.sub(r"\s+", " ", text.strip())

    # 2. Insert comma BEFORE each soft-split phrase
    for phrase in SOFT_SPLIT_PHRASES:
        pattern = re.compile(r"\s+" + re.escape(phrase) + r"\b", re.IGNORECASE)
        text = pattern.sub(", " + phrase, text)

    # 3. Split on comma, semicolon, newline, or " / "
    segments = re.split(r"[,;\n]|\s/\s", text)

    # 4. Filter: keep segments >= 8 chars, not pure numbers, not discard prefixes
    results = []
    for seg in segments:
        seg = seg.strip()
        if len(seg) < 8:
            continue
        if re.fullmatch(r"[\d\s.,\-/]+", seg):
            continue

        seg_lower = seg.lower().strip()
        discard = False
        for prefix in DISCARD_PREFIXES:
            if seg_lower.startswith(prefix):
                discard = True
                break
        if discard:
            continue

        results.append(seg.strip())

    logger.debug(f"Split paragraph into {len(results)} segments")
    return results
