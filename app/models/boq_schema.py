from pydantic import BaseModel
from typing import List, Dict


class BOQItem(BaseModel):
    description: str
    brand: str = "Generic"
    quantity: float = 0.0
    unit: str = "-"
    category: str = "Uncategorized"


class BOQList(BaseModel):
    items: List[BOQItem]


class AnalyzeRequest(BaseModel):
    items: List[Dict]
