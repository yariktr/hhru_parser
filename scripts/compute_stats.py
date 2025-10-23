import argparse
from hhru_parser.logging_setup import setup_logging
from hhru_parser.bd.bd_vacancy import compute_basic_stats

def main():
    setup_logging()
    ap = argparse.ArgumentParser()
    ap.add_argument("--currency", default="RUB", help="Валюта для расчёта зарплат (по умолчанию RUB)")
    args = ap.parse_args()

    stats = compute_basic_stats(currency=args.currency)

    print("\n== ЗП по группам опыта ==")
    print("bucket   | count |   avg    |  median  | currency")
    print("-----------------------------------------------")
    for row in stats["salary_by_experience"]:
        print(f"{row['exp_bucket']:<7} | {row['count']:>5} | {row['avg']:>8} | {row['median']:>8} | {row['currency']}")

    print("\n== Распределение по формату работы ==")
    for row in stats["schedule_distribution"]:
        print(f"- {row['schedule']}: {row['count']}")

    print("\n== Топ компаний ==")
    for row in stats["top_companies"]:
        print(f"- {row['company_name']}: {row['count']}")

if __name__ == "__main__":
    main()
