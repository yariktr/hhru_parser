import argparse
from pathlib import Path

from hhru_parser.logging_setup import setup_logging
from hhru_parser.main import run_pipeline


def main():
    setup_logging()

    ap = argparse.ArgumentParser()
    ap.add_argument("--test_query", required=True, help="Строка поиска на hh.ru")
    ap.add_argument("-n", "--limit", type=int, default=5, help="Сколько карточек обрабатывать")
    ap.add_argument("--cookies-file", help="Путь к JSON-файлу с куками hh.ru")
    args = ap.parse_args()

    # необязательно, но удобно: проверим путь к кукам заранее
    if args.cookies_file:
        cf = Path(args.cookies_file)
        if not cf.exists():
            print(f"[WARN] cookies-file не найден: {cf} — продолжу без кук.")
            args.cookies_file = None

    items, meta = run_pipeline(
        query=args.test_query,
        limit=args.limit,
        cookies_file=args.cookies_file,
    )

    if not items:
        print("No items. Try later or reduce -n.")
        return

    print(f"\nTotal found (search page): {meta.get('total_found')}")
    print(
        f"Processed: {meta.get('count')} | "
        f"avg={meta.get('avg_sec')}s | "
        f"median={meta.get('med_sec')}s | "
        f"total={meta.get('total_time')}s\n"
    )

    for it in items:
        sal = it.get("salary_text") or "no information"
        comp = it.get("company_name") or "no information"
        title = str(it.get("title") or "no information")
        print(f"- [{it['id']}] {title} | {comp} | {sal} | {it['url']}")


if __name__ == "__main__":
    main()
