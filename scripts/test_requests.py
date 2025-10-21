import argparse
from hhru_parser.main import run_pipeline

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--test_query", required=True)
    ap.add_argument("-n", "--limit", type=int, default=5)
    args = ap.parse_args()
    items = run_pipeline(args.test_query, args.limit)
    if not items:
        print("No items. Try later or reduce -n.")
        return
    for it in items:
        print(f"- [{it['id']}] {it.get('title')!s} | {it['url']}")

if __name__ == "__main__":
    main()
