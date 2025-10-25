#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Апдейтер ru_map_nba.json из ru_pending_nba.json через sports.ru (баскетбол).
1) /basketball/person/<slug>/  → заголовок -> последнее слово (фамилия)
2) /basketball/player/<slug>/ → то же
3) Поиск sports.ru → первый профиль → фамилия
4) Фоллбэк: словарь EXCEPT_LAST и/или латиница как временный вариант
После успешного разрешения id уходит из очереди.
"""

import os, sys, re, json, unicodedata, time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from urllib.parse import quote_plus

RU_MAP_PATH     = "ru_map_nba.json"
RU_PENDING_PATH = "ru_pending_nba.json"

SPORTS_RU_HOST   = "https://www.sports.ru"
SRU_PERSON_ROOT  = SPORTS_RU_HOST + "/basketball/person/"
SRU_PLAYER_ROOT  = SPORTS_RU_HOST + "/basketball/player/"
SRU_SEARCH       = SPORTS_RU_HOST + "/search/?q="

EXCEPT_LAST = {
    "Doncic":"Дончич","Jokic":"Йокич","Embiid":"Эмбиид","Curry":"Карри","James":"Джеймс","Davis":"Дэвис",
    "Durant":"Дюрэнт","Booker":"Букер","Irving":"Ирвинг","Tatum":"Тейтум","Brown":"Браун","Harden":"Харден",
    "George":"Джордж","Leonard":"Леонард","Antetokounmpo":"Адетокумбо","Young":"Янг","Adebayo":"Адебайо",
    "Williamson":"Уильямсон","Mitchell":"Митчелл","Brunson":"Брансон","Randle":"Рэндл","Sabonis":"Сабонис",
    "Markkanen":"Маркканен","Haliburton":"Халибертон","Wembanyama":"Вембаньяма","Edwards":"Эдвардс",
    "Siakam":"Сиакам","Anunoby":"Ануноби","Porzingis":"Порзингис","Gilgeous-Alexander":"Гилджес-Александер",
    "Fox":"Фокс","Maxey":"Макси","Holiday":"Холидэй","Lopez":"Лопес","Mobley":"Мобли","Allen":"Аллен",
    "Vucevic":"Вучевич","Ayton":"Эйтон","Demin":"Дёмин","Goldin":"Голдин",
}

def make_session():
    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.6,
              status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({
        "User-Agent":"NBA-RU-CACHE-UPDATER/1.0",
        "Accept-Language":"ru-RU,ru;q=0.9,en;q=0.6",
    })
    return s

S = make_session()

def load(path, default):
    if not os.path.exists(path): return default
    try:
        with open(path,"r",encoding="utf-8") as f: return json.load(f)
    except Exception: return default

def save(path, data):
    tmp = path + ".tmp"
    with open(tmp,"w",encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def slugify(first: str, last: str) -> str:
    base = f"{first} {last}".strip()
    base = unicodedata.normalize("NFKD", base)
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    base = base.lower().strip()
    base = re.sub(r"[^a-z0-9]+","-", base).strip("-")
    return base

def try_profile(first: str, last: str) -> str | None:
    slug = slugify(first, last)
    for root in (SRU_PERSON_ROOT, SRU_PLAYER_ROOT):
        url = root + slug + "/"
        r = S.get(url, timeout=20)
        if r.status_code == 200 and ("/basketball/person/" in r.url or "/basketball/player/" in r.url):
            return url
    return None

def extract_surname(url: str) -> str | None:
    r = S.get(url, timeout=20)
    if r.status_code != 200: return None
    soup = BeautifulSoup(r.text, "html.parser")
    h = soup.find(["h1","h2"])
    if not h: return None
    full = " ".join(h.get_text(" ", strip=True).split())
    parts = [p for p in re.split(r"\s+", full) if p]
    if len(parts) >= 2:
        return parts[-1]
    return None

def search_surname(first: str, last: str) -> str | None:
    q = quote_plus(f"{first} {last}".strip())
    r = S.get(SRU_SEARCH + q, timeout=20)
    if r.status_code != 200: return None
    soup = BeautifulSoup(r.text, "html.parser")
    link = soup.select_one('a[href*="/basketball/person/"]') or soup.select_one('a[href*="/basketball/player/"]')
    if not link or not link.get("href"): return None
    href = link["href"]
    if href.startswith("/"): href = SPORTS_RU_HOST + href
    return extract_surname(href)

def main():
    ru_map = load(RU_MAP_PATH, {})
    pending = load(RU_PENDING_PATH, [])
    if not pending:
        print("No pending.")
        return

    still = []
    resolved = 0

    for it in pending:
        pid = str(it.get("id") or "")
        first = (it.get("first") or "").strip()
        last  = (it.get("last") or "").strip()
        if not pid: continue
        if pid in ru_map: continue

        ru = None
        if last in EXCEPT_LAST:
            ru = EXCEPT_LAST[last]
        if not ru and first and last:
            url = try_profile(first, last)
            if url:
                ru = extract_surname(url)
        if not ru and first and last:
            ru = search_surname(first, last)
        if not ru:
            ru = last or first or pid

        if ru and any(ch.isalpha() for ch in ru):
            ru_map[pid] = ru
            resolved += 1
        else:
            still.append(it)

        time.sleep(0.25)

    save(RU_MAP_PATH, ru_map)
    save(RU_PENDING_PATH, still)
    print(f"Resolved: {resolved}, left: {len(still)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
