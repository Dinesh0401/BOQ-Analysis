import pandas as pd
from loguru import logger
from app.config.settings import HEADER_SCAN_LIMIT, HEADER_KEYWORDS


def detect_header_row(df: pd.DataFrame) -> int:
    """Auto-detect the header row by scanning for keyword matches.

    Scans the first HEADER_SCAN_LIMIT rows and returns the row index
    with the highest count of header keyword matches.
    Defaults to 0 if nothing found.
    """
    if df.empty:
        return 0

    best_row = 0
    best_count = 0
    scan_limit = min(HEADER_SCAN_LIMIT, len(df))

    for idx in range(scan_limit):
        row = df.iloc[idx]
        count = 0
        for cell in row:
            if pd.isna(cell):
                continue
            cell_lower = str(cell).lower().strip()
            for kw in HEADER_KEYWORDS:
                if kw in cell_lower:
                    count += 1
                    break  # One keyword match per cell is enough

        if count > best_count:
            best_count = count
            best_row = idx

    logger.info(
        f"Detected header row: {best_row} (matched {best_count} keywords)"
    )
    return best_row
