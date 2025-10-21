from __future__ import annotations
import os
from datetime import datetime, timezone
import psycopg
from dotenv import load_dotenv

load_dotenv()

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS vacancies (
  id TEXT PRIMARY KEY,
  url TEXT UNIQUE,
  title TEXT,
  source TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
"""

UPSERT_SQL = """
INSERT INTO vacancies (id, url, title, source, created_at, updated_at)
VALUES (%(id)s, %(url)s, %(title)s, %(source)s, %(created_at)s, %(updated_at)s)
ON CONFLICT (id) DO UPDATE SET
  url = EXCLUDED.url,
  title = EXCLUDED.title,
  source = EXCLUDED.source,
  updated_at = EXCLUDED.updated_at;
"""

def _conn():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg.connect(url, autocommit=True)

def init_db():
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)

def upsert_minimal(vacancies):
    now = datetime.now(timezone.utc)
    with _conn() as conn, conn.cursor() as cur:
        for v in vacancies:
            cur.execute(UPSERT_SQL, {
                "id": v["id"],
                "url": v["url"],
                "title": v.get("title"),
                "source": "http",
                "created_at": now,
                "updated_at": now,
            })
