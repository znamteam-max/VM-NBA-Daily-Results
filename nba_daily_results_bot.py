#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU) — Sports.ru primary, ESPN fallback for boxscore

• Матчи/счёт/названия на русском: Sports.ru (список матчей дня + страница матча)
• Игроки: Sports.ru таблицы; если шапки нет/непонятна — fallback ESPN (points/reb/ast/stl/blk),
          имена приводим к виду «И. Фамилия» по профилю/поиску Sports.ru; если не нашли — оригинал.
• ОТ: считаем только из таблицы «по периодам»: >4 пар счёта → (ОТ), иначе — нет.
• Кастом-эмодзи: из ENV TEAM_EMOJI_JSON. Если значение — число, выводим как <tg-emoji emoji-id="...">.

ENV:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID — обязательны
  TEAM_EMOJI_JSON — JSON {"LAL":"5328...","NYK":"🙂", ...}
  DEBUG_NBA=1 — подробные логи
  NBA_DATE_MODE=today|best — брать «сегодня по ET» или день с максимальным числом матчей
"""

import os, sys, re, json
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from urllib.parse import urljoin, quote_plus

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ========= ENV / LOG =========
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()
DEBUG     = os.getenv("DEBUG_NBA", "").strip() not in {"", "0", "false", "False"}
DATE_MODE = (os.getenv("NBA_DATE_MODE", "today") or "today").lower()

def logdbg(*a):
    if DEBUG:
        print("[DBG]", *a, file=sys.stderr)

# ========= DATES / RU =========
def et_today() -> date:
    return datetime.now(ZoneInfo("America/New_York")).date()

RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

# ========= HTTP =========
def make_session():
    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.7,
              status_forcelist=[429,500,502,503,504], allowed_methods=["GET","POST"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({
        "User-Agent": "NBA-ResultsBot/6.2 (sportsru primary, espn fallback)",
        "Accept-Language": "ru,en;q=0.7",
    })
    return s
S = make_session()

# ========= CONSTANTS =========
DAY_URL_TMPL = "https://www.sports.ru/stat/basketball/center/end/{y:04d}/{m:02d}/{d:02d}.html"
MATCH_PREFIX = "https://www.sports.ru/basketball/match/"

SLUG2ABBR = {
    "atlanta-hawks":"ATL","boston-celtics":"BOS","brooklyn-nets":"BKN","charlotte-hornets":"CHA",
    "chicago-bulls":"CHI","cleveland-cavaliers":"CLE","dallas-mavericks":"DAL","denver-nuggets":"DEN",
    "detroit-pistons":"DET","golden-state-warriors":"GSW","houston-rockets":"HOU","indiana-pacers":"IND",
    "los-angeles-clippers":"LAC","los-angeles-lakers":"LAL","memphis-grizzlies":"MEM","miami-heat":"MIA",
    "milwaukee-bucks":"MIL","minnesota-timberwolves":"MIN","new-orleans-pelicans":"NOP","new-york-knicks":"NYK",
    "oklahoma-city-thunder":"OKC","orlando-magic":"ORL","philadelphia-76ers":"PHI","phoenix-suns":"PHX",
    "portland-trail-blazers":"POR","sacramento-kings":"SAC","san-antonio-spurs":"SAS","toronto-raptors":"TOR",
    "utah-jazz":"UTA","washington-wizards":"WAS",
}
TEAM_RU = {
    "ATL":"Атланта","BOS":"Бостон","BKN":"Бруклин","CHA":"Шарлотт","CHI":"Чикаго","CLE":"Кливленд",
    "DAL":"Даллас","DEN":"Денвер","DET":"Детройт","GSW":"Голден Стэйт","HOU":"Хьюстон","IND":"Индиана",
    "LAC":"Клипперс","LAL":"Лейкерс","MEM":"Мемфис","MIA":"Майами","MIL":"Милуоки","MIN":"Миннесота",
    "NOP":"Новый Орлеан","NYK":"Нью-Йорк","OKC":"Оклахома-Сити","ORL":"Орландо","PHI":"Филадельфия",
    "PHX":"Финикс","POR":"Портленд","SAC":"Сакраменто","SAS":"Сан-Антонио","TOR":"Торонто","UTA":"Юта","WAS":"Вашингтон",
}

ESPN_BASE = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba"

# ========= TEAM EMOJI (custom) =========
def load_team_emojis() -> dict[str,str]:
    raw = os.getenv("TEAM_EMOJI_JSON", "").strip()
    if not raw: return {}
    try:
        mp = json.loads(raw)
        if isinstance(mp, dict):
            return {k.upper(): str(v) for k, v in mp.items()}
    except Exception:
        pass
    return {}
TEAM_EMOJI = load_team_emojis()

def team_emoji(abbr: str) -> str:
    val = TEAM_EMOJI.get((abbr or "").upper())
    if not val:
        return "🏀"
    # если это числовой ID кастом-эмодзи Телеграма — оборачиваем
    if re.fullmatch(r"\d{6,}", val):
        return f'<tg-emoji emoji-id="{val}"></tg-emoji>'
    return val

# ========= HELPERS =========
def abbr_from_url(url: str, side: int) -> str | None:
    m = re.search(r"/basketball/match/([a-z0-9\-]+)-vs-([a-z0-9\-]+)/", url)
    if not m: return None
    slug = m.group(1 + side)
    return SLUG2ABBR.get(slug)

def _int_first(text: str) -> int:
    m = re.search(r"-?\d+", text or "")
    return int(m.group(0)) if m else 0

# ========= LINKS OF THE DAY (Sports.ru) =========
def fetch_day_links(d: date) -> list[str]:
    url = DAY_URL_TMPL.format(y=d.year, m=d.month, d=d.day)
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        logdbg("SPORTS DAY URL FAIL", url, r.status_code); return []
    logdbg("SPORTS DAY URL", url, "OK")
    soup = BeautifulSoup(r.text, "html.parser")
    out, seen = [], set()
    for a in soup.find_all("a", href=True):
        h = a["href"]
        if not (h.startswith("/basketball/match/") or h.startswith(MATCH_PREFIX)): continue
        if "/live/" in h: continue
        full = urljoin("https://www.sports.ru", h).split("#", 1)[0]
        if "vs" not in full: continue
        if not full.endswith("/"): full += "/"
        a_abbr, b_abbr = abbr_from_url(full, 0), abbr_from_url(full, 1)
        if not (a_abbr and b_abbr and a_abbr in TEAM_RU and b_abbr in TEAM_RU): continue
        if full not in seen:
            out.append(full); seen.add(full)
    logdbg("SPORTS LINKS", len(out))
    return out

def count_games_for_day(d: date) -> int:
    return len(fetch_day_links(d))

# ========= SPORTS.RU PARSERS =========
def norm_col(text: str) -> str:
    t = re.sub(r"\s+", "", (text or "").lower())
    t = (t.replace("очки","оч").replace("очков","оч")
           .replace("передачи","аст").replace("перед","аст")
           .replace("подборы","подбор").replace("перехваты","перех")
           .replace("блок-шоты","блокшоты").replace("блок","блокшоты"))
    return t

def col_metric(t: str) -> str | None:
    if t in {"оч","pts"}: return "pts"
    if t in {"подбор","reb"}: return "reb"
    if t in {"аст","ast"}: return "ast"
    if t in {"перех","stl"}: return "stl"
    if t in {"блокшоты","blk","бш"}: return "blk"
    return None

def header_to_index(tbl) -> dict[str,int]:
    thead = tbl.find("thead")
    if thead:
        rows = thead.find_all("tr")
        if rows:
            cells = rows[-1].find_all(["th","td"])
            headers = [c.get_text(" ", strip=True) for c in cells]
            idx_map = {}
            for i, h in enumerate(headers):
                m = col_metric(norm_col(h))
                if m and m not in idx_map: idx_map[m] = i
            if idx_map:
                logdbg("HEAD (thead):", headers, "→", idx_map)
                return idx_map
    # иногда Sports.ru не отдаёт thead — не гадаем, вернём пусто -> включим ESPN
    logdbg("HEAD not found")
    return {}

def parse_stats_table_near(h3) -> tuple[list[dict], int, bool]:
    """Возвращает (players, team_pts, header_ok). Если header_ok=False — не используем игроков."""
    tbl = h3.find_next("table")
    if not tbl: return [], 0, False
    idx_map = header_to_index(tbl)
    tbody = tbl.find("tbody") or tbl
    team_pts = 0
    players = []

    # найдём строку «Итого/Всего», чтобы вытащить командные очки
    for tr in tbody.find_all("tr"):
        cells = tr.find_all(["th","td"])
        if not cells: continue
        name_cell = cells[0]
        first_text = name_cell.get_text(" ", strip=True)
        if re.search(r"^(итого|всего)\s*$", first_text, flags=re.IGNORECASE):
            if "pts" in idx_map and idx_map["pts"] < len(cells):
                team_pts = _int_first(cells[idx_map["pts"]].get_text(" ", strip=True))
            else:
                # без шапки лучше не гадать
                pass
            break

    if not idx_map:
        # шапки нет — вернём только team_pts, игроков не используем (пусть ESPN добавит)
        return [], team_pts, False

    # нормальный разбор строк игроков
    for tr in tbody.find_all("tr"):
        cells = tr.find_all(["th","td"])
        if not cells: continue
        name_cell = cells[0]
        first_text = name_cell.get_text(" ", strip=True)
        if re.search(r"^(итого|всего)\s*$", first_text, flags=re.IGNORECASE):
            continue
        # имя: чаще всего в <a>
        a = name_cell.find("a")
        name_ru = (a.get_text(" ", strip=True) if a else first_text).strip()
        if not name_ru or name_ru.isdigit():
            # иногда первый столбец — номер; попробуем второй
            if len(cells) > 1:
                a2 = cells[1].find("a")
                name_ru = (a2.get_text(" ", strip=True) if a2 else cells[1].get_text(" ", strip=True)).strip()
        if not name_ru or name_ru.isdigit():
            continue

        def val(metric):
            i = idx_map.get(metric, -1)
            if i < 0 or i >= len(cells): return 0
            return _int_first(cells[i].get_text(" ", strip=True))

        players.append({
            "name_ru": name_ru,
            "pts": val("pts"), "reb": val("reb"), "ast": val("ast"),
            "stl": val("stl"), "blk": val("blk"),
        })

    return players, team_pts, True

def detect_ot(soup: BeautifulSoup) -> bool:
    """Смотрим только таблицу с периодами: 4 пары — без ОТ; >4 — ОТ; иначе — нет."""
    for h3 in soup.find_all("h3"):
        txt = (h3.get_text(" ", strip=True) or "").lower()
        if "период" in txt or "по периодам" in txt:
            tbl = h3.find_next("table")
            if not tbl: continue
            row = tbl.find("tr")
            if not row: continue
            pairs = re.findall(r"\b\d{1,3}\s*:\s*\d{1,3}\b", row.get_text(" ", strip=True))
            return len(pairs) > 4
    return False

def parse_sports_match(url: str) -> dict | None:
    a_abbr = abbr_from_url(url, 0); b_abbr = abbr_from_url(url, 1)
    if not (a_abbr and b_abbr and a_abbr in TEAM_RU and b_abbr in TEAM_RU): return None
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        logdbg("PARSE START", url, "HTTP", r.status_code); return None
    soup = BeautifulSoup(r.text, "html.parser")

    # ищем два блока «статистика игроков»
    h3s = [h for h in soup.find_all("h3") if re.search(r"статистика игроков", h.get_text(" ", strip=True), re.I)]
    teamA_name = TEAM_RU.get(a_abbr, a_abbr)
    teamB_name = TEAM_RU.get(b_abbr, b_abbr)
    playersA, playersB, scoreA, scoreB = [], [], 0, 0
    header_okA = header_okB = False

    if len(h3s) >= 2:
        playersA, scoreA, header_okA = parse_stats_table_near(h3s[0])
        playersB, scoreB, header_okB = parse_stats_table_near(h3s[1])

        # поправим названия, если Sports.ru отдал их в заголовке
        def team_from_h3(h3):
            t = h3.get_text(" ", strip=True)
            m = re.match(r"\s*(.+?)\.\s*статистика игроков\s*$", t, flags=re.IGNORECASE)
            if m: return m.group(1).strip('«»" ')
            return (t.replace("статистика игроков","").strip()).strip('«»" ')
        ta = team_from_h3(h3s[0]); tb = team_from_h3(h3s[1])
        if ta: teamA_name = ta
        if tb: teamB_name = tb

    # запасной способ счёта
    if not (scoreA and scoreB):
        txt = soup.get_text(" ", strip=True)
        m = re.search(r"\b(\d{1,3})\s*:\s*(\d{1,3})\b", txt)
        if m:
            scoreA = scoreA or int(m.group(1))
            scoreB = scoreB or int(m.group(2))

    logdbg("PARSE TEAMS", teamA_name, teamB_name, "SCORES", scoreA or 0, scoreB or 0,
           "A_rows", len(playersA), "B_rows", len(playersB))

    return {
        "url": url,
        "teams": [
            {"abbr": a_abbr, "name_ru": teamA_name, "score": scoreA or 0,
             "players": playersA if header_okA else []},
            {"abbr": b_abbr, "name_ru": teamB_name, "score": scoreB or 0,
             "players": playersB if header_okB else []},
        ],
        "ot": detect_ot(soup),
    }

# ========= ESPN FALLBACK (players) =========
def espn_get(url: str) -> dict:
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        return {}
    try: return r.json()
    except Exception: return {}

def espn_scoreboard(d: date) -> list[dict]:
    dates = d.strftime("%Y%m%d")
    j = espn_get(f"{ESPN_BASE}/scoreboard?dates={dates}")
    return j.get("events") or []

def espn_find_event_and_map(d: date, abbrA: str, abbrB: str):
    """Возвращает (event_id, map_teamid_to_abbr) или (None, {})."""
    for ev in espn_scoreboard(d):
        comp = (ev.get("competitions") or [{}])[0]
        mapping = {}
        abbrs = set()
        for c in (comp.get("competitors") or []):
            t = c.get("team") or {}
            tid = str(t.get("id") or "")
            a = (t.get("abbreviation") or "").upper()
            if a == "GS": a = "GSW"
            mapping[tid] = a
            abbrs.add(a)
        if {abbrA, abbrB} == abbrs:
            return ev.get("id"), mapping
    return None, {}

def espn_boxscore_players(event_id: str) -> dict[str, list[dict]]:
    j = espn_get(f"{ESPN_BASE}/boxscore?event={event_id}")
    out = {}
    for t in (j.get("players") or []):
        team = t.get("team") or {}
        tid = str(team.get("id") or "")
        arr = []
        for grp in (t.get("statistics") or []):
            for a in (grp.get("athletes") or []):
                ath = a.get("athlete") or {}
                pid = str(ath.get("id") or "")
                name = ath.get("displayName") or ""
                parts = [p for p in re.split(r"\s+", name.strip()) if p]
                first = parts[0] if parts else ""
                last = " ".join(parts[1:]) if len(parts) > 1 else parts[0] if parts else ""
                stats_map = {}
                for k, v in (a.get("stats") or {}).items(): stats_map[k.lower()] = v
                for k, v in (ath.get("stats") or {}).items(): stats_map.setdefault(k.lower(), v)
                def iget(*keys, default=0):
                    for k in keys:
                        if k in stats_map:
                            try: return int(stats_map[k])
                            except Exception:
                                try: return int(float(stats_map[k]))
                                except Exception: pass
                    return default
                pts = iget("points","pts")
                reb = iget("rebounds","reb","totreb","reboundstotal")
                ast = iget("assists","ast")
                stl = iget("steals","stl")
                blk = iget("blocks","blk")
                if all(v in (None, 0) for v in [pts,reb,ast,stl,blk]): continue
                arr.append({"id": pid, "first": first, "last": last,
                            "pts": int(pts or 0), "reb": int(reb or 0), "ast": int(ast or 0),
                            "stl": int(stl or 0), "blk": int(blk or 0)})
        # слить дубли
        merged = {}
        for p in arr:
            if p["id"] not in merged: merged[p["id"]] = p
            else:
                m = merged[p["id"]]
                for k in ("pts","reb","ast","stl","blk"):
                    m[k] = max(m[k], p[k])
        out[tid] = list(merged.values())
    return out

# ========= RU NAME (Sports.ru) =========
SPORTS_RU = "https://www.sports.ru"
SRU_PERSON = SPORTS_RU + "/basketball/person/"
SRU_PLAYER = SPORTS_RU + "/basketball/player/"
SRU_SEARCH = SPORTS_RU + "/search/?q="

def _slugify(first: str, last: str) -> str:
    base = f"{first} {last}".strip().lower()
    return re.sub(r"[^a-z0-9]+", "-", base).strip("-")

def _sru_profile_url(first: str, last: str) -> str | None:
    slug = _slugify(first, last)
    for root in (SRU_PERSON, SRU_PLAYER):
        url = root + slug + "/"
        r = S.get(url, timeout=12)
        if r.status_code == 200 and ("/basketball/person/" in r.url or "/basketball/player/" in r.url):
            return url
    return None

def _sru_extract_name(url: str) -> str | None:
    try:
        r = S.get(url, timeout=15)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.text, "html.parser")
        h = soup.find(["h1","h2"])
        full = h.get_text(" ", strip=True) if h else ""
        full = " ".join(full.split())
        return full or None
    except Exception:
        return None

def _sru_search_name(first: str, last: str) -> str | None:
    try:
        q = quote_plus(f"{first} {last}".strip())
        r = S.get(SRU_SEARCH + q, timeout=15)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.text, "html.parser")
        a = soup.select_one('a[href*="/basketball/person/"]') or soup.select_one('a[href*="/basketball/player/"]')
        if not a or not a.get("href"): return None
        href = a["href"]; 
        if href.startswith("/"): href = SPORTS_RU + href
        return _sru_extract_name(href)
    except Exception:
        return None

def ru_initials_surname_from_en(first: str, last: str) -> str:
    nm = None
    url = _sru_profile_url(first, last)
    if url: nm = _sru_extract_name(url)
    if not nm: nm = _sru_search_name(first, last)
    if nm:
        parts = [p for p in nm.split() if p]
        if len(parts) >= 2: return f"{parts[0][0]}. {parts[-1]}"
        return nm
    # fallback — латиница
    first_i = (first[:1] + ". ") if first else ""
    return f"{first_i}{last}".strip()

# ========= PICK / FORMAT =========
def is_double_double(p: dict) -> bool:
    cats = [p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]]
    return sum(1 for v in cats if v >= 10) >= 2

def pick_players_for_team(team_abbr: str, plist: list[dict]) -> list[dict]:
    if not plist: return []
    plist_sorted = sorted(plist, key=lambda x: (x["pts"], x["reb"], x["ast"]), reverse=True)
    picked = []

    special_last = None
    if team_abbr == "BKN": special_last = "demin"
    if team_abbr == "MIA": special_last = "goldin"

    special = None
    if special_last:
        for p in plist_sorted:
            if p.get("_source") == "espn":
                ln = (p.get("last","") or "").lower()
                if ln.endswith(special_last):
                    special = p; break
            else:
                nm = (p.get("name_ru","") or "")
                if "Дёмин" in nm or "Голдин" in nm:
                    special = p; break

    best = plist_sorted[0]; picked.append(best)
    if special and special not in picked:
        picked.append(special)
    else:
        for p in plist_sorted[1:]:
            if p in picked: continue
            if p["pts"] >= 20 or is_double_double(p) or p["stl"] >= 5 or p["blk"] >= 5:
                picked.append(p); break

    return picked[:2]

def initials_plus_surname(name_ru: str) -> str:
    parts = [w for w in name_ru.split() if w]
    if len(parts) <= 1: return name_ru
    return f"{parts[0][0]}. {parts[-1]}"

def fmt_player_line_ru(name_ru: str, pts:int, reb:int, ast:int, stl:int, blk:int, force_top3=False) -> str:
    name = initials_plus_surname(name_ru)
    base = [f"{name}: {pts} {ru_plural(pts, ('очко','очка','очков'))}"]
    if force_top3:
        extras = [("подбор","подбора","подборов",reb),
                  ("передача","передачи","передач",ast),
                  ("перехват","перехвата","перехватов",stl),
                  ("блок-шот","блок-шота","блок-шотов",blk)]
        extras = [(w1,w2,w3,val) for (w1,w2,w3,val) in extras if val and val>0]
        extras.sort(key=lambda t: t[3], reverse=True)
        extras = extras[:3]
        for (w1,w2,w3,val) in extras:
            base.append(f"{val} {ru_plural(val,(w1,w2,w3))}")
    else:
        if reb >= 5: base.append(f"{reb} {ru_plural(reb, ('подбор','подбора','подборов'))}")
        if ast >= 5: base.append(f"{ast} {ru_plural(ast, ('передача','передачи','передач'))}")
        if stl >= 4: base.append(f"{stl} {ru_plural(stl, ('перехват','перехвата','перехватов'))}")
        if blk >= 4: base.append(f"{blk} {ru_plural(blk, ('блок-шот','блок-шота','блок-шотов'))}")
    return "<span class=\"tg-spoiler\">" + ", ".join(base) + "</span>"

# ========= ENRICH WITH ESPN WHEN NEEDED =========
def enrich_with_espn(game: dict, day: date):
    need_a = not game["teams"][0]["players"]
    need_b = not game["teams"][1]["players"]
    if not (need_a or need_b): return

    abbrA, abbrB = game["teams"][0]["abbr"], game["teams"][1]["abbr"]
    event_id, id2abbr = espn_find_event_and_map(day, abbrA, abbrB)
    if not event_id or not id2abbr:
        logdbg("FALLBACK ESPN: event not found for", abbrA, abbrB); 
        return
    by_team = espn_boxscore_players(event_id)

    def to_ru_list(players_en):
        out = []
        for p in players_en:
            ru = ru_initials_surname_from_en(p["first"], p["last"])
            item = {"name_ru": ru, "pts": p["pts"], "reb": p["reb"], "ast": p["ast"], "stl": p["stl"], "blk": p["blk"],
                    "_source": "espn", "last": p["last"]}
            out.append(item)
        return out

    # распределим по командам по карте teamId → abbr
    abbr2list = {abbrA: [], abbrB: []}
    for tid, lst in by_team.items():
        ab = id2abbr.get(tid)
        if ab in abbr2list:
            abbr2list[ab].extend(to_ru_list(lst))

    for side in (0,1):
        team = game["teams"][side]
        if not team["players"]:
            chosen = pick_players_for_team(team["abbr"], abbr2list.get(team["abbr"], []))
            team["players"] = chosen

# ========= BUILD GAME BLOCK =========
def build_game_block(game: dict, idx: int, total: int) -> list[str]:
    t1, t2 = game["teams"][0], game["teams"][1]
    a_win = t1["score"] > t2["score"]; b_win = t2["score"] > t1["score"]
    name1 = TEAM_RU.get(t1["abbr"], t1["name_ru"])
    name2 = TEAM_RU.get(t2["abbr"], t2["name_ru"])
    e1, e2 = team_emoji(t1["abbr"]), team_emoji(t2["abbr"])
    scoreA = f"<b>{t1['score']}</b>" if a_win else f"{t1['score']}"
    scoreB = f"<b>{t2['score']}</b>" if b_win else f"{t2['score']}"
    ot_tag = " (ОТ)" if game["ot"] else ""

    lines = [
        f"{e1} {name1}: <span class=\"tg-spoiler\">{scoreA}</span>",
        f"{e2} {name2}: <span class=\"tg-spoiler\">{scoreB}</span>{ot_tag}",
        ""
    ]

    # команда 1
    for p in (t1.get("players") or []):
        force = False
        if "Дёмин" in p.get("name_ru","") and t1["abbr"]=="BKN": force = True
        if "Голдин" in p.get("name_ru","") and t1["abbr"]=="MIA": force = True
        lines.append(fmt_player_line_ru(p["name_ru"], p["pts"], p["reb"], p["ast"], p["stl"], p["blk"], force_top3=force))
    if t1.get("players"): lines.append("")

    # команда 2
    for p in (t2.get("players") or []):
        force = False
        if "Дёмин" in p.get("name_ru","") and t2["abbr"]=="BKN": force = True
        if "Голдин" in p.get("name_ru","") and t2["abbr"]=="MIA": force = True
        lines.append(fmt_player_line_ru(p["name_ru"], p["pts"], p["reb"], p["ast"], p["stl"], p["blk"], force_top3=force))

    if idx + 1 < total:
        lines += ["", "–––––––––––––––––––––––", ""]
    return lines

# ========= BUILD POST =========
def build_post(d: date) -> str:
    links = fetch_day_links(d)
    games = []
    for u in links:
        try:
            g = parse_sports_match(u)
            if g: games.append(g)
        except Exception as e:
            logdbg("PARSE ERROR", u, repr(e))

    # ESPN для тех, где игроков нет
    for g in games:
        if not g["teams"][0]["players"] or not g["teams"][1]["players"]:
            enrich_with_espn(g, d)

    n = len(games)
    title = (
        f"НБА • {ru_date(d)} • {n} {ru_plural(n, ('матч','матча','матчей'))}\n"
        "Результаты надёжно спрятаны 👇\n"
        "–––––––––––––––––––––––\n\n"
    )
    if n == 0: return title.rstrip()

    lines = []
    for i, g in enumerate(games):
        lines.extend(build_game_block(g, i, n))
    return (title + "\n".join(lines)).strip()

# ========= TELEGRAM =========
def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    r = S.post(url, json=payload, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text}")

# ========= MAIN =========
def pick_report_day() -> date:
    if DATE_MODE == "best":
        d_today = et_today(); d_yest = d_today - timedelta(days=1)
        n_today = count_games_for_day(d_today); n_yest = count_games_for_day(d_yest)
        logdbg(f"DAY CANDIDATES ET today={d_today}({n_today}) / yest={d_yest}({n_yest})")
        return d_today if n_today >= n_yest else d_yest
    else:
        d = et_today(); logdbg("DAY PICKED (today)", d); return d

if __name__ == "__main__":
    try:
        day = pick_report_day()
        post = build_post(day)
        tg_send(post)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
