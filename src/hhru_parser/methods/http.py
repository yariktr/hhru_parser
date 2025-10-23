from __future__ import annotations
import logging, json, os
import re
import time
import random
from statistics import mean, median
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from dataclasses import asdict

from hhru_parser.models import Vacancy

class HTTPParser:
    SEARCH_URL = "https://hh.ru/search/vacancy"

    def __init__(self, cookies_file: str | None = None):
        self.log = logging.getLogger(__name__)
        
        self.base_delay = 2.0
        self.jitter = 0.6
        self.backoff_factor = 2.0
        self.max_delay = 60.0
        self.success_to_relax = 5
        self.current_delay = self.base_delay
        self._success_streak = 0

        ua_pool = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        ]
        ua = random.choice(ua_pool)

        self.sess = requests.Session()
        self.sess.headers.update({
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru,en;q=0.9",
            "Connection": "keep-alive",
            "Referer": "https://hh.ru/",
        })
        self.log.debug("Selected UA: %s", ua)

        if cookies_file:
            loaded = self._load_cookies_from_json(cookies_file)
            if loaded:
                self.log.info("Куки из файла подгружены: %s", cookies_file)
            else:
                self.log.warning("Не удалось подгрузить куки из файла: %s — работаем без кук", cookies_file)


    def _load_cookies_from_json(self, path: str) -> bool:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            jar = requests.cookies.RequestsCookieJar()

            if isinstance(data, dict) and "cookies" in data and isinstance(data["cookies"], list):
                cookies_iter = data["cookies"]
            elif isinstance(data, list):
                cookies_iter = data
            else:
                self.log.warning("Неизвестный формат cookies JSON: %s", path)
                return False

            count = 0
            for c in cookies_iter:
                name = c.get("name")
                value = c.get("value")
                domain = c.get("domain") or ""
                path_c = c.get("path") or "/"
                if not name or value is None:
                    continue
                if "hh.ru" not in domain:
                    continue
                jar.set(name, value, domain=domain, path=path_c)
                count += 1

            if count:
                self.sess.cookies.update(jar)
                return True
            return False
        except Exception as e:
            self.log.warning("Ошибка загрузки cookies (%s): %s", type(e).__name__, path)
            return False
        
    def _sleep_with_jitter(self) -> None:
        j = random.uniform(-self.jitter, self.jitter)
        pause = max(0.0, self.current_delay + j)
        if pause >= 0.01:
            time.sleep(pause)
        self.log.debug("sleep %.2fs (delay=%.2f, jitter=%.2f)", pause, self.current_delay, j)

    def _on_block(self, status: int, url: str) -> None:
        # экспоненциальный бэкофф при 403/429
        old = self.current_delay
        self.current_delay = min(self.current_delay * self.backoff_factor, self.max_delay)
        self._success_streak = 0
        self.log.warning("Block %s on %s → backoff delay: %.2fs → %.2fs",
                        status, url, old, self.current_delay)

    def _on_success(self) -> None:
        # плавное снижение задержки к базовой после серии успехов
        self._success_streak += 1
        if self._success_streak >= self.success_to_relax and self.current_delay > self.base_delay:
            old = self.current_delay
            # шаг к базовой (10% расстояния)
            self.current_delay = max(self.base_delay, self.base_delay + 0.9 * (self.current_delay - self.base_delay))
            self._success_streak = 0
            self.log.info("Relax delay: %.2fs → %.2fs", old, self.current_delay)

            

    def search(self, query: str, limit: int = 5) -> Tuple[List[Dict], Dict]:
        t0 = time.perf_counter()
        self.log.info("Поиск: %r (limit=%d)", query, limit)

        r = self.sess.get(self.SEARCH_URL, params={"text": query}, timeout=(5, 20))
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")
        total_found = self._extract_total_found(soup)
        if total_found is not None:
            self.log.info("Найдено всего по запросу: %s", total_found)

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
                seen.add(u); uniq.append(u)
        uniq = uniq[:limit]
        self.log.info("Ссылок к обработке: %d", len(uniq))

        out: List[Dict] = []
        per_item_times: List[float] = []

        for url in tqdm(uniq, desc="Вакансии", unit="шт"):
            self._sleep_with_jitter()
            t1 = time.perf_counter()

            rr = self.sess.get(url, timeout=(5, 20))

            if rr.status_code in (403, 429):
                self._on_block(rr.status_code, url)
                continue 

            rr.raise_for_status()

            s2 = BeautifulSoup(rr.text, "html.parser")
            item = self.parse_vacancy(s2, url)
            out.append(asdict(item))

            dt = time.perf_counter() - t1
            per_item_times.append(dt)

            self._on_success()  
            self.log.debug("OK %s (%.2f сек)", url, dt)


        total_time = time.perf_counter() - t0
        avg_sec = round(mean(per_item_times), 3) if per_item_times else None
        med_sec = round(median(per_item_times), 3) if per_item_times else None
        self.log.info(
            "Готово: обработано %d, общая длительность %.2f сек, среднее на карточку %s сек, медиана %s сек",
            len(out), total_time, avg_sec, med_sec
        )

        meta = {
            "total_found": total_found,
            "avg_sec": avg_sec,
            "med_sec": med_sec,
            "count": len(out),
            "total_time": round(total_time, 3),
        }
        return out, meta

    # ---------- helpers ----------
    def _extract_total_found(self, soup) -> Optional[int]:
        el = soup.select_one('[data-qa="vacancies-search-header"]') or soup.select_one('[data-qa="serp__found"]')
        if not el:
            for sel in ("h1", ".bloko-header-section-3", ".bloko-header-2"):
                el = soup.select_one(sel)
                if el and el.get_text(strip=True):
                    break
        def extract_num(text: str) -> Optional[int]:
            m = re.search(r"(\d[\d\s\u00A0]*)", text or "")
            if not m:
                return None
            num = m.group(1).replace(" ", "").replace("\u00A0", "")
            try:
                return int(num)
            except ValueError:
                return None
        if el:
            n = extract_num(el.get_text(" ", strip=True))
            if n is not None:
                return n
        for t in soup.stripped_strings:
            n = extract_num(t)
            if n and n > 10:
                return n
        return None

    # ---------- helpers: карточка вакансии ----------
    def parse_vacancy(self, soup: BeautifulSoup, url: str) -> Vacancy:
        vac_id = self._extract_id_from_url(url)
        title = self._parse_title(soup)
        company_name, company_url = self._parse_company(soup)
        salary_from, salary_to, salary_currency, is_gross, salary_text = self._parse_salary(soup)
        experience_text, exp_bucket = self._parse_experience(soup)
        schedule, employment_type = self._parse_schedule_and_employment(soup)
        location_city = self._parse_location(soup)
        published_at = self._parse_published_at(soup)
        responses_count = self._parse_responses_count(soup)
        description = self._parse_description(soup)
        skills = self._parse_skills(soup)

        raw = {
            "title": title,
            "company_name": company_name,
            "company_url": company_url,
            "salary_text": salary_text,
            "experience_text": experience_text,
            "schedule": schedule,
            "employment_type": employment_type,
            "location_city": location_city,
            "published_at": published_at,
            "responses_count": responses_count,
            "skills": skills,
        }

        return Vacancy(
            id=vac_id,
            url=url,
            title=title,
            company_name=company_name,
            company_url=company_url,
            salary_from=salary_from,
            salary_to=salary_to,
            salary_currency=salary_currency,
            is_gross=is_gross,
            salary_text=salary_text,
            experience_text=experience_text,
            exp_bucket=exp_bucket,
            schedule=schedule,
            employment_type=employment_type,
            location_city=location_city,
            responses_count=responses_count,
            published_at=published_at,
            description=description,
            skills=skills,
            raw_json=raw,
        )

    def _extract_id_from_url(self, url: str) -> str:
        m = re.search(r"/vacancy/(\d+)", url)
        return m.group(1) if m else url

    def _parse_title(self, soup: BeautifulSoup) -> Optional[str]:
        tag = soup.find(attrs={"data-qa": "vacancy-title"}) or soup.find("h1")
        return tag.get_text(strip=True) if tag else None

    def _parse_company(self, soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
        el = soup.select_one('[data-qa="vacancy-company-name"]')
        if el:
            name = el.get_text(strip=True)
            link = el.find_parent("a") or el.find("a")
            href = link.get("href") if link and link.has_attr("href") else None
            return name, href
        return None, None

    def _parse_salary(self, soup: BeautifulSoup):
        el = soup.select_one('[data-qa="vacancy-salary"]')
        if not el:
            return None, None, None, None, None
        text = el.get_text(" ", strip=True)
        nums = re.findall(r"(\d[\d\s\u00A0]*)", text)
        def to_int(s):
            try:
                return int(s.replace(" ", "").replace("\u00A0", ""))
            except Exception:
                return None
        s_from = s_to = None
        if len(nums) == 1:
            s_from = to_int(nums[0])
        elif len(nums) >= 2:
            s_from, s_to = to_int(nums[0]), to_int(nums[1])
        currency = None
        if "₽" in text or "руб" in text.lower():
            currency = "RUB"
        elif "€" in text or "eur" in text.lower():
            currency = "EUR"
        elif "$" in text or "usd" in text.lower():
            currency = "USD"
        is_gross = True if "до вычета" in text.lower() else None
        return s_from, s_to, currency, is_gross, text

    def _parse_experience(self, soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
        el = soup.select_one('[data-qa="vacancy-experience"]')
        text = el.get_text(" ", strip=True) if el else None
        if not text:
            cand = soup.find(string=re.compile(r"опыт", re.I))
            text = cand.strip() if cand else None
        bucket = None
        if text:
            m = re.search(r"(\d)[–-](\d)", text) 
            if m:
                lo, hi = int(m.group(1)), int(m.group(2))
                if hi <= 1: bucket = "0-1"
                elif hi <= 3: bucket = "1-3"
                elif hi <= 6: bucket = "3-6"
                else: bucket = "6+"
            else:
                m2 = re.search(r"(\d+)\+?", text)
                if m2:
                    v = int(m2.group(1))
                    if v <= 1: bucket = "0-1"
                    elif v <= 3: bucket = "1-3"
                    elif v <= 6: bucket = "3-6"
                    else: bucket = "6+"
        return text, bucket

    def _parse_schedule_and_employment(self, soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
        text = soup.get_text(" ", strip=True).lower()
        schedule = None
        if "удал" in text or "remote" in text:
            schedule = "remote"
        elif "гибрид" in text or "hybrid" in text:
            schedule = "hybrid"
        elif "офис" in text:
            schedule = "office"

        employment = None
        if "полная занятость" in text or "full time" in text:
            employment = "full-time"
        elif "частичная занятость" in text or "part time" in text:
            employment = "part-time"
        elif "стажировка" in text or "intern" in text:
            employment = "intern"

        return schedule, employment

    def _parse_location(self, soup: BeautifulSoup) -> Optional[str]:
        el = soup.select_one('[data-qa="vacancy-view-location"]')
        if el:
            return el.get_text(" ", strip=True)
        cand = soup.select_one(".vacancy-view-location") or soup.find(string=re.compile(r"Россия|Украина|Казахстан|Беларус", re.I))
        return cand.strip() if isinstance(cand, str) else (cand.get_text(" ", strip=True) if cand else None)

    def _parse_published_at(self, soup: BeautifulSoup) -> Optional[str]:
        el = soup.select_one('[data-qa="vacancy-view-creation-time"]') or soup.find("time")
        if el:
            return el.get_text(" ", strip=True)
        return None

    def _parse_responses_count(self, soup: BeautifulSoup) -> Optional[int]:
        el = soup.find(string=re.compile(r"тклик", re.I)) 
        if not el:
            return None
        m = re.search(r"(\d+)", el)
        return int(m.group(1)) if m else None

    def _parse_description(self, soup: BeautifulSoup) -> Optional[str]:
        el = soup.select_one('[data-qa="vacancy-description"]')
        if el:
            return el.get_text("\n", strip=True)
        return None

    def _parse_skills(self, soup: BeautifulSoup) -> List[str]:
        skills = []
        for el in soup.select('[data-qa="skills-element"], .bloko-tag__text'):
            txt = el.get_text(strip=True)
            if txt and txt not in skills:
                skills.append(txt)
        return skills
