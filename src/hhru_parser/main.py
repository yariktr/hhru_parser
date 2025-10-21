from __future__ import annotations
from .methods.http import HTTPParser
from .bd.bd_vacancy import init_db, upsert_minimal

def run_pipeline(query: str, limit: int = 5):
    init_db()
    parser = HTTPParser()
    items = parser.search(query=query, limit=limit)
    if items:
        upsert_minimal(items)
    return items
