import os
import tempfile
from fastapi import APIRouter, UploadFile, File, Query, HTTPException
from loguru import logger

from app.models.boq_schema import AnalyzeRequest
from app.services.excel_analyzer import process_excel
from app.services.category_classifier import classify_category
from app.services.graph_matcher import graph_stats, learn_material
from app.graphs.excel_graph import extract_with_ai
from app.analytics.boq_analyzer import analyze_boq
from app.analytics.risk_engine import detect_risks
from app.utils.product_normalizer import consolidate_duplicates

router = APIRouter()


@router.post("/extract")
async def extract_boq(
    file: UploadFile = File(...),
    industry: str = Query(default="construction"),
):
    """Rule-based extraction only (no AI, fast)."""
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Only .xlsx or .xls files accepted")

    # Check file size (max 10MB)
    contents = await file.read()
    if len(contents) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File exceeds 10MB limit")

    # Save to temp file
    suffix = os.path.splitext(file.filename)[1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(contents)
        tmp_path = tmp.name

    try:
        result = process_excel(tmp_path, industry)
        return result
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise HTTPException(status_code=500, detail=f"Extraction failed: {str(e)}")
    finally:
        try:
            os.unlink(tmp_path)
        except PermissionError:
            pass  # Windows file lock — will be cleaned up by OS


@router.post("/upload-excel")
async def upload_excel(
    file: UploadFile = File(...),
    industry: str = Query(default="construction"),
):
    """Gemini AI extraction with rule-based fallback + learning loop."""
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Only .xlsx or .xls files accepted")

    contents = await file.read()
    if len(contents) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File exceeds 10MB limit")

    suffix = os.path.splitext(file.filename)[1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(contents)
        tmp_path = tmp.name

    try:
        # Step 1: Rule-based extraction
        result = process_excel(tmp_path, industry)
        items = result.get("items", [])

        # Step 2: Find uncategorized items for AI classification
        uncategorized = [i for i in items if i.get("category") == "Uncategorized"]

        if uncategorized:
            logger.info(
                f"{len(uncategorized)} uncategorized items — sending to Gemini AI"
            )

            # Build text from uncategorized items for AI
            raw_text = "\n".join(
                [i["description"] for i in uncategorized if i.get("description")]
            )

            if raw_text.strip():
                try:
                    ai_result = extract_with_ai(raw_text, industry)
                    ai_items = ai_result.get("items", [])
                except Exception as ai_err:
                    logger.warning(f"AI extraction failed, using rule-based only: {ai_err}")
                    ai_items = []

                # Build lookup from AI results
                ai_lookup = {}
                for ai_item in ai_items:
                    desc = ai_item.get("description", "").lower().strip()
                    if desc and ai_item.get("category") != "Uncategorized":
                        ai_lookup[desc] = ai_item

                # Update uncategorized items with AI classifications
                for item in items:
                    if item.get("category") != "Uncategorized":
                        continue

                    desc_lower = item["description"].lower().strip()

                    # Direct match
                    if desc_lower in ai_lookup:
                        ai_item = ai_lookup[desc_lower]
                        item["category"] = ai_item["category"]
                        # Learning loop: save to graph
                        learn_material(
                            item["description"],
                            ai_item["category"],
                            item.get("unit", "-"),
                            source="llm",
                        )
                        continue

                    # Partial match: check if any AI description is in this item
                    for ai_desc, ai_item in ai_lookup.items():
                        if ai_desc in desc_lower or desc_lower in ai_desc:
                            item["category"] = ai_item["category"]
                            learn_material(
                                item["description"],
                                ai_item["category"],
                                item.get("unit", "-"),
                                source="llm",
                            )
                            break

        # Consolidate and regroup
        items = consolidate_duplicates(items)
        from app.services.boq_extractor import group_by_category

        categories = group_by_category(items)

        result["items"] = items
        result["categories"] = categories
        result["extracted_items"] = len(items)

        return result

    except Exception as e:
        logger.error(f"Upload processing failed: {e}")
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")
    finally:
        try:
            os.unlink(tmp_path)
        except PermissionError:
            pass  # Windows file lock — will be cleaned up by OS


@router.post("/analyze")
async def analyze_items(request: AnalyzeRequest):
    """Analyze extracted BOQ items for category summaries and insights."""
    try:
        result = analyze_boq(request.items)
        return result
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.post("/risk")
async def assess_risk(request: AnalyzeRequest):
    """Detect procurement and data-quality risks in BOQ items."""
    try:
        result = detect_risks(request.items)
        return result
    except Exception as e:
        logger.error(f"Risk assessment failed: {e}")
        raise HTTPException(status_code=500, detail=f"Risk assessment failed: {str(e)}")


@router.get("/graph-stats")
async def get_graph_stats():
    """Return material knowledge graph statistics."""
    try:
        stats = graph_stats()
        return stats
    except Exception as e:
        logger.error(f"Graph stats failed: {e}")
        raise HTTPException(status_code=500, detail=f"Graph stats failed: {str(e)}")
