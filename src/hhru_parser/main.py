from __future__ import annotations
from .methods.http import HTTPParser
from .bd.bd_vacancy import init_db, upsert_vacancies

def run_pipeline(query: str, limit: int = 5, cookies_file: str | None = None):
    init_db()
    parser = HTTPParser(cookies_file=cookies_file)
    items, meta = parser.search(query=query, limit=limit)
    if items:
        upsert_vacancies(items)
    return items, meta
