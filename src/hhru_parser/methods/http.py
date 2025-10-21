from __future__ import annotations
import re
import time
import requests
from bs4 import BeautifulSoup

class HTTPParser:
    SEARCH_URL = "https://hh.ru/search/vacancy"

    def __init__(self):
        self.sess = requests.Session()
        self.sess.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/124.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru,en;q=0.9",
            "Connection": "keep-alive",
            "Referer": "https://hh.ru/",
        })

    def search(self, query: str, limit: int = 5):
        r = self.sess.get(self.SEARCH_URL, params={"text": query}, timeout=20)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")
        links = [a.get("href") for a in soup.select("a.serp-item__title") if a.get("href")]

        if not links:
            links = []
            for a in soup.find_all("a", href=True):
                if re.search(r"/vacancy/\d+", a["href"]):
                    links.append(a["href"].split("?")[0])

        seen, uniq = set(), []
        for u in links:
            u = u.split("?")[0]
            if u not in seen:
                seen.add(u)
                uniq.append(u)
        uniq = uniq[:limit]

        out = []
        for url in uniq:
            time.sleep(2)
            rr = self.sess.get(url, timeout=20)
            if rr.status_code in (403, 429):
                break
            rr.raise_for_status()

            s2 = BeautifulSoup(rr.text, "html.parser")
            title_tag = s2.find(attrs={"data-qa": "vacancy-title"}) or s2.find("h1")
            title = title_tag.get_text(strip=True) if title_tag else None

            m = re.search(r"/vacancy/(\d+)", url)
            vac_id = m.group(1) if m else url

            out.append({"id": vac_id, "url": url, "title": title})

        return out
