from __future__ import annotations
import os, json
from datetime import datetime, timezone
import psycopg

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(usecwd=True), override=False)

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

MIGRATIONS = [
    "ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS company_name TEXT",
    "ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS company_url TEXT",
    "ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS salary_from INTEGER",
    "ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS salary_to INTEGER",
    "ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS salary_currency TEXT",
    "ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS is_gross BOOLEAN",
    "ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS experience_text TEXT",
    "ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS exp_bucket TEXT",
    "ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS schedule TEXT",
    "ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS employment_type TEXT",
    "ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS location_city TEXT",
    "ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS responses_count INTEGER",
    "ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS published_at TEXT",
    "ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS description TEXT",
    "ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS skills TEXT[]",
    "ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS salary_text TEXT",
    "ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS raw_json JSONB",
]

UPSERT_SQL = """
INSERT INTO vacancies (
  id, url, title, source,
  company_name, company_url,
  salary_from, salary_to, salary_currency, is_gross, salary_text,
  experience_text, exp_bucket,
  schedule, employment_type, location_city,
  responses_count, published_at, description, skills, raw_json,
  created_at, updated_at
) VALUES (
  %(id)s, %(url)s, %(title)s, %(source)s,
  %(company_name)s, %(company_url)s,
  %(salary_from)s, %(salary_to)s, %(salary_currency)s, %(is_gross)s, %(salary_text)s,
  %(experience_text)s, %(exp_bucket)s,
  %(schedule)s, %(employment_type)s, %(location_city)s,
  %(responses_count)s, %(published_at)s, %(description)s, %(skills)s, %(raw_json)s,
  %(created_at)s, %(updated_at)s
)
ON CONFLICT (id) DO UPDATE SET
  url = EXCLUDED.url,
  title = EXCLUDED.title,
  source = EXCLUDED.source,
  company_name = EXCLUDED.company_name,
  company_url = EXCLUDED.company_url,
  salary_from = EXCLUDED.salary_from,
  salary_to = EXCLUDED.salary_to,
  salary_currency = EXCLUDED.salary_currency,
  is_gross = EXCLUDED.is_gross,
  salary_text = EXCLUDED.salary_text,
  experience_text = EXCLUDED.experience_text,
  exp_bucket = EXCLUDED.exp_bucket,
  schedule = EXCLUDED.schedule,
  employment_type = EXCLUDED.employment_type,
  location_city = EXCLUDED.location_city,
  responses_count = EXCLUDED.responses_count,
  published_at = EXCLUDED.published_at,
  description = EXCLUDED.description,
  skills = EXCLUDED.skills,
  raw_json = EXCLUDED.raw_json,
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
        for sql in MIGRATIONS:
            cur.execute(sql)

def upsert_vacancies(vacancies: list[dict]):
    now = datetime.now(timezone.utc)
    with _conn() as conn, conn.cursor() as cur:
        for v in vacancies:
            skills = v.get("skills") or []
            if not isinstance(skills, list):
                skills = [str(skills)]
            row = {
                "id": v.get("id"),
                "url": v.get("url"),
                "title": v.get("title"),
                "source": v.get("source", "http"),
                "company_name": v.get("company_name"),
                "company_url": v.get("company_url"),
                "salary_from": v.get("salary_from"),
                "salary_to": v.get("salary_to"),
                "salary_currency": v.get("salary_currency"),
                "is_gross": v.get("is_gross"),
                "salary_text": v.get("salary_text"),
                "experience_text": v.get("experience_text"),
                "exp_bucket": v.get("exp_bucket"),
                "schedule": v.get("schedule"),
                "employment_type": v.get("employment_type"),
                "location_city": v.get("location_city"),
                "responses_count": v.get("responses_count"),
                "published_at": v.get("published_at"),
                "description": v.get("description"),
                "skills": skills if skills else None,
                "raw_json": json.dumps(v, ensure_ascii=False),
                "created_at": now,
                "updated_at": now,
            }
            cur.execute(UPSERT_SQL, row)
            


from statistics import median

def _fetch_rows_for_salary(currency: str = "RUB") -> list[tuple[str | None, int | None, int | None]]:
    """
    Возвращает список кортежей (exp_bucket, salary_from, salary_to) только для указанной валюты.
    """
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT exp_bucket, salary_from, salary_to
            FROM vacancies
            WHERE salary_currency = %s
            """,
            (currency,),
        )
        return cur.fetchall()

def _fetch_counts_by_schedule() -> list[tuple[str | None, int]]:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(schedule, 'unknown') AS sch, COUNT(*)
            FROM vacancies
            GROUP BY sch
            ORDER BY COUNT(*) DESC
            """
        )
        return cur.fetchall()

def _fetch_top_companies(limit: int = 10) -> list[tuple[str | None, int]]:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(company_name, 'unknown') AS name, COUNT(*) AS c
            FROM vacancies
            GROUP BY name
            ORDER BY c DESC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()
    

def compute_basic_stats(currency: str = "RUB") -> dict:
    """
    Возвращает словарь со сводной статистикой:
      - salary_by_experience: [{exp_bucket, count, avg, median, currency}]
      - schedule_distribution: [{schedule, count}]
      - top_companies: [{company_name, count}]
    Все агрегаты считаются только по таблице vacancies без JOIN/UNNEST,
    чтобы исключить размножение строк.
    """
    with _conn() as conn, conn.cursor() as cur:
        # --- 1) Зарплата по группам опыта ---
        # Нормализуем "числовую" зарплату как среднее из (salary_from, salary_to), если есть обе.
        # Фильтруем по целевой валюте и только по тем вакансиям, где есть хотя бы одно из полей.
        cur.execute(
            """
            WITH base AS (
                SELECT
                    COALESCE(NULLIF(exp_bucket, ''), 'unknown') AS exp_bucket,
                    CASE
                        WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL
                            THEN (salary_from + salary_to) / 2.0
                        WHEN salary_from IS NOT NULL
                            THEN salary_from::float
                        WHEN salary_to IS NOT NULL
                            THEN salary_to::float
                        ELSE NULL
                    END AS sal
                FROM vacancies
                WHERE salary_currency = %s
                  AND (salary_from IS NOT NULL OR salary_to IS NOT NULL)
            )
            SELECT
                exp_bucket,
                COUNT(*) AS count,
                ROUND(AVG(sal))::float AS avg,
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sal))::float AS median
            FROM base
            GROUP BY exp_bucket
            ORDER BY
                CASE exp_bucket
                    WHEN '0-1' THEN 1
                    WHEN '1-3' THEN 2
                    WHEN '3-6' THEN 3
                    WHEN '6+'  THEN 4
                    ELSE 5
                END, exp_bucket;
            """,
            (currency,),
        )
        salary_rows = [
            {
                "exp_bucket": r[0],
                "count": int(r[1]),
                "avg": r[2],
                "median": r[3],
                "currency": currency,
            }
            for r in cur.fetchall()
        ]

        # --- 2) Распределение по формату работы ---
        cur.execute(
            """
            SELECT
                COALESCE(NULLIF(schedule, ''), 'unknown') AS schedule,
                COUNT(*) AS count
            FROM vacancies
            GROUP BY COALESCE(NULLIF(schedule, ''), 'unknown')
            ORDER BY count DESC, schedule;
            """
        )
        schedule_rows = [{"schedule": r[0], "count": int(r[1])} for r in cur.fetchall()]

        # --- 3) Топ компаний ---
        cur.execute(
            """
            SELECT
                COALESCE(NULLIF(company_name, ''), 'unknown') AS company_name,
                COUNT(*) AS count
            FROM vacancies
            GROUP BY COALESCE(NULLIF(company_name, ''), 'unknown')
            ORDER BY count DESC, company_name
            LIMIT 15;
            """
        )
        top_companies_rows = [{"company_name": r[0], "count": int(r[1])} for r in cur.fetchall()]

    return {
        "salary_by_experience": salary_rows,
        "schedule_distribution": schedule_rows,
        "top_companies": top_companies_rows,
    }


def existing_ids(ids: list[str]) -> set[str]:
    """Вернёт множество id, которые уже есть в таблице vacancies."""
    if not ids:
        return set()
    with _conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM vacancies WHERE id = ANY(%s);", (ids,))
        return {row[0] for row in cur.fetchall()}

