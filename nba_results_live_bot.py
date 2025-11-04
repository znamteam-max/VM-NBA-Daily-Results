#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Results → Telegram (per game, instant)

• Пост по матчу уходит сразу после статуса "completed" в ESPN.
• Игроки тянутся со sports.ru, если уже есть таблицы; иначе — быстрый пост без игроков.
• Анти-дубликат: .posted/event_<eventId>.done. Плюс починили коммит маркеров в workflow.
• День — по PT (Pacific Time), чтобы все матчи дня были в одном числе.
• Защита от «кривых» счётов со sports.ru: если подозрительно — берем счёт из ESPN.
• Исправлена канонизация аббревиатур (в т.ч. UTAH→UTA) и замена custom emoji при 2–5 буквах.

ENV:
- TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
- TEAM_EMOJI_JSON — {"BOS":"<custom_emoji_id>", ...}
- REPORT_DATE_PT — YYYY-MM-DD (тест)
- DEBUG_NBA=1 — подробные логи

Тест-флаги:
- SEND_TEST_MESSAGE=1 — отправить ping и выйти
- LIST_EVENTS=1 — вывести финалы за REPORT_DATE_PT и выйти
- ONLY_EVENT_ID=<id> — публиковать только этот матч
- DRY_RUN=1 — не отправлять в TG, только логи
- ALT_CHAT_ID=<id> — слать в альтернативный чат
"""

import os, sys, re, json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, urlunparse
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except Exception:
    Retry = None
from bs4 import BeautifulSoup

# -------- ENV --------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TEAM_EMOJI_JSON = os.getenv("TEAM_EMOJI_JSON", "").strip()
DEBUG = bool(os.getenv("DEBUG_NBA", "").strip())

ONLY_EVENT_ID   = os.getenv("ONLY_EVENT_ID", "").strip()
DRY_RUN         = bool(os.getenv("DRY_RUN", "").strip())
SEND_TEST_MSG   = bool(os.getenv("SEND_TEST_MESSAGE", "").strip())
LIST_EVENTS     = bool(os.getenv("LIST_EVENTS", "").strip())
ALT_CHAT_ID     = os.getenv("ALT_CHAT_ID", "").strip()

# -------- HTTP --------
HTTP_TIMEOUT = 12

def _mk_adapter():
    if Retry is not None:
        try:
            r = Retry(total=3, connect=3, read=3, backoff_factor=0.4,
                      status_forcelist=[429,500,502,503,504],
                      allowed_methods=["GET","POST"])
        except TypeError:
            r = Retry(total=3, backoff_factor=0.4)
        return HTTPAdapter(max_retries=r)
    return HTTPAdapter(max_retries=2)

def make_session():
    s = requests.Session()
    ad = _mk_adapter()
    s.mount("https://", ad); s.mount("http://", ad)
    s.headers.update({
        "User-Agent": "NBA-LiveResultsBot/1.2 (per-game; pt-day; espn-finish; sportsru-players; custom-emoji; fix-scores)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
        "Connection": "close",
    })
    return s

S = make_session()
def log(*a):
    if DEBUG: print(*a, file=sys.stderr)

# -------- DATES --------
RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}

def ru_date(d: date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def pick_report_date_pacific_env() -> date:
    env = os.getenv("REPORT_DATE_PT", "").strip()
    if env:
        try: return date.fromisoformat(env)
        except Exception: pass
    now_pt = datetime.now(ZoneInfo("America/Los_Angeles"))
    # до 06:00 PT считаем вчера
    return now_pt.date() if now_pt.hour >= 6 else (now_pt.date() - timedelta(days=1))

def espn_dates_for_pt_day(d_pt: date) -> list[date]:
    tz_pt = ZoneInfo("America/Los_Angeles"); tz_et = ZoneInfo("America/New_York")
    start_pt = datetime(d_pt.year, d_pt.month, d_pt.day, 0, 0, tzinfo=tz_pt)
    end_pt   = datetime(d_pt.year, d_pt.month, d_pt.day, 23, 59, tzinfo=tz_pt)
    base = { start_pt.astimezone(tz_et).date(), end_pt.astimezone(tz_et).date() }
    ext = set()
    for d in base:
        ext.add(d); ext.add(d - timedelta(days=1)); ext.add(d + timedelta(days=1))
    return sorted(ext)

def sportsru_dates_for_pt_day(d_pt: date) -> list[date]:
    tz_pt = ZoneInfo("America/Los_Angeles"); tz_msk = ZoneInfo("Europe/Moscow")
    start_pt = datetime(d_pt.year, d_pt.month, d_pt.day, 0, 0, tzinfo=tz_pt)
    end_pt   = datetime(d_pt.year, d_pt.month, d_pt.day, 23, 59, tzinfo=tz_pt)
    return sorted({ start_pt.astimezone(tz_msk).date(), end_pt.astimezone(tz_msk).date() })

# -------- TEAMS / EMOJI --------
TEAM_RU_TO_ABBR = {
    "Атланта":"ATL","Бостон":"BOS","Бруклин":"BKN","Шарлотт":"CHA","Чикаго":"CHI",
    "Кливленд":"CLE","Даллас":"DAL","Денвер":"DEN","Детройт":"DET","Голден Стэйт":"GSW",
    "Хьюстон":"HOU","Индиана":"IND","Клипперс":"LAC","Лейкерс":"LAL","Мемфис":"MEM",
    "Майами":"MIA","Милуоки":"MIL","Миннесота":"MIN","Новый Орлеан":"NOP","Нью-Йорк":"NYK",
    "Оклахома-Сити":"OKC","Орландо":"ORL","Филадельфия":"PHI","Финикс":"PHX","Портленд":"POR",
    "Сакраменто":"SAC","Сан-Антонио":"SAS","Торонто":"TOR","Юта":"UTA","Вашингтон":"WAS",
}
ABBR_TO_RU = {v:k for k,v in TEAM_RU_TO_ABBR.items()}

TEAM_EMOJI_DEFAULT = {
    "ATL":"🦅","BOS":"☘️","BKN":"🕸️","CHA":"🐝","CHI":"🐂","CLE":"🛡️","DAL":"🐎","DEN":"⛏️","DET":"🔧",
    "GSW":"🗡️","HOU":"🚀","IND":"💫","LAC":"✂️","LAL":"⭐️","MEM":"🐻","MIA":"🔥","MIL":"🦌","MIN":"🐺",
    "NOP":"🪶","NYK":"🗽","OKC":"⚡","ORL":"✨","PHI":"🔔","PHX":"☀️","POR":"🧭","SAC":"👑","SAS":"🪙",
    "TOR":"🦖","UTA":"🎷","WAS":"🧙",
}

# Канон. аббревиатур ESPN ↔ sports.ru (расширено)
_ABBR_CANON_SWAP = {
    "GS":"GSW","PHO":"PHX","CHO":"CHA","WSH":"WAS","UTH":"UTA","SA":"SAS","NO":"NOP","NY":"NYK","BRK":"BKN",
    "UTAH":"UTA","PHILA":"PHI","PHIL":"PHI","NOLA":"NOP","LAKERS":"LAL","CLIPPERS":"LAC"
}

def canon_abbr(s: str | None) -> str:
    x = (s or "").upper().strip()
    return _ABBR_CANON_SWAP.get(x, x)

def load_custom_emoji():
    if not TEAM_EMOJI_JSON: return {}
    try:
        d = json.loads(TEAM_EMOJI_JSON)
        if isinstance(d, dict):
            return {canon_abbr(k): str(v) for k,v in d.items() if v}
    except Exception:
        pass
    return {}
CUSTOM_EMOJI = load_custom_emoji()

def emoji_token(abbr: str) -> str:
    return f"{{EMO:({canon_abbr(abbr)})}}"

# -------- SPORTS.RU --------
def day_url(d: date) -> str:
    return f"https://www.sports.ru/stat/basketball/center/end/{d:%Y/%m/%d}.html"

def _normalize_match_url(u: str) -> str:
    full = "https://www.sports.ru" + u if u.startswith("/") else u
    p = urlparse(full); return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

def _soup(url: str):
    try:
        r = S.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200: return None
        return BeautifulSoup(r.text, "html.parser")
    except Exception:
        return None

def collect_day_links(d: date) -> list[str]:
    soup = _soup(day_url(d))
    if not soup: return []
    out, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/basketball/match/" not in href: continue
        full = _normalize_match_url(href)
        if full in seen: continue
        seen.add(full); out.append(full)
    log(f"[DBG] SPORTS LINKS {len(out)} for {d}")
    return out

def _canonical_ru_team(raw: str) -> str | None:
    if not raw: return None
    t = raw.replace("«","").replace("»","").strip()
    t = re.sub(r"\(.*?\)", "", t).strip()
    for k in TEAM_RU_TO_ABBR:
        if t.startswith(k) or k in t:
            return k
    return None

def _anchor_team_players(soup: BeautifulSoup, team_ru: str):
    stamp = team_ru.lower()
    for h in soup.find_all(["h2","h3","h4"]):
        txt = h.get_text(" ", strip=True).lower()
        if "статистика игроков" in txt and stamp in txt:
            return h
    return None

def _find_table_after(anchor):
    if not anchor: return None
    t = anchor.find_next("table")
    if t: return t
    node = anchor
    for _ in range(12):
        node = node.find_next()
        if not node: break
        if getattr(node, "get_text", None):
            tx = node.get_text(" ", strip=True).lower()
            if "игрок" in tx:
                return node
    return None

def _header_map(cells: list[str]) -> dict:
    mp={}
    for i, raw in enumerate(cells):
        t = raw.strip().lower().replace(" ", "")
        if t == "о" and "pts" not in mp:   mp["pts"] = i
        if t == "пб" and "reb" not in mp: mp["reb"] = i
        if t == "ап" and "ast" not in mp: mp["ast"] = i
        if t == "пх" and "stl" not in mp: mp["stl"] = i
        if t == "бш" and "blk" not in mp: mp["blk"] = i
    return mp

def _as_int(x: str) -> int:
    x = (x or "").strip().replace("\u2009"," ")
    if not x: return 0
    if ":" in x: return 0
    if "/" in x:
        try: return int(x.split("/",1)[0])
        except Exception: return 0
    try: return int(x)
    except Exception:
        try: return int(float(x))
        except Exception: return 0

def _parse_players_table(node) -> list[dict]:
    rows_out=[]
    def cells_of_tr(tr):
        tds = tr.find_all(["td","th"])
        return [td.get_text(" ", strip=True) for td in tds]
    trs = node.find_all("tr") if hasattr(node, "find_all") else []
    if not trs: return rows_out
    header = None
    for tr in trs:
        cells = cells_of_tr(tr)
        if not cells: continue
        joined = " ".join(cells).strip().lower()
        if joined.startswith("игрок "): header = cells; break
    if not header: return rows_out
    col = _header_map(header)
    for tr in trs:
        cells = cells_of_tr(tr)
        if not cells: continue
        j = " ".join(cells).strip().lower()
        if j.startswith("игрок ") or j.startswith("итого "): continue
        name = None
        for c in cells[:3]:
            if re.search(r"[^\d/:%\s\-]", c):
                name = c.strip(); break
        if not name: continue
        def g(idx): return _as_int(cells[idx]) if idx is not None and idx < len(cells) else 0
        pts = g(col.get("pts")); reb = g(col.get("reb")); ast = g(col.get("ast"))
        stl = g(col.get("stl")); blk = g(col.get("blk"))
        if not any([pts,reb,ast,stl,blk]): continue
        rows_out.append({"name": name, "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk})
    return rows_out

def _extract_total_score_from_page(soup: BeautifulSoup) -> tuple[int,int]:
    txt = soup.get_text("\n", strip=True)
    m = re.search(r"заверш", txt, flags=re.I)
    if m:
        before = txt[:m.start()]
        pairs = [(int(a), int(b)) for a,b in re.findall(r"(\d{1,3})\s*:\s*(\d{1,3})", before)]
        if pairs:
            def is_period(p): a,b = p; return (0 <= a <= 50) and (0 <= b <= 50) and (20 <= a+b <= 85)
            i = len(pairs) - 1; skipped = 0
            while i >= 0 and skipped < 7 and is_period(pairs[i]): i -= 1; skipped += 1
            if i >= 0:
                a,b = pairs[i]; log(f"[DBG] SCORE FINAL-BEFORE-FINISHED -> {a}:{b}"); return a,b
            a,b = pairs[-1]; log(f"[DBG] SCORE FALLBACK-LASTPAIR -> {a}:{b}"); return a,b
    # если не нашли — не пытаемся угадывать (чтобы не ловить 23:59)
    return (0,0)

def parse_sports_match(url: str) -> dict | None:
    soup = _soup(url)
    if not soup: return None
    meta = soup.find("meta", attrs={"property":"og:title"})
    title = meta.get("content") if meta and meta.get("content") else (soup.title.string if soup.title else "")
    teamA = teamB = None
    if title and " - " in title:
        left, right = [x.strip() for x in title.split(" - ", 1)]
        teamA = _canonical_ru_team(left)
        right = right.split(":")[0].strip()
        teamB = _canonical_ru_team(right)
    if not (teamA and teamB): return None
    a_abbr = TEAM_RU_TO_ABBR.get(teamA,""); b_abbr = TEAM_RU_TO_ABBR.get(teamB,"")
    if not a_abbr or not b_abbr: return None
    ancA = _anchor_team_players(soup, teamA); ancB = _anchor_team_players(soup, teamB)
    if not ancA or not ancB: return None
    tabA = _find_table_after(ancA); tabB = _find_table_after(ancB)
    if not tabA or not tabB: return None
    rowsA = _parse_players_table(tabA); rowsB = _parse_players_table(tabB)
    if not (rowsA or rowsB): return None
    scoreA, scoreB = _extract_total_score_from_page(soup)
    log(f"[DBG] OK {teamA}-{teamB} SCORE {scoreA}:{scoreB} A_rows {len(rowsA)} B_rows {len(rowsB)}")
    return {
        "teamA": {"name": teamA, "abbr": a_abbr, "score": scoreA},
        "teamB": {"name": teamB, "abbr": b_abbr, "score": scoreB},
        "players": {teamA: rowsA, teamB: rowsB},
        "records": {},
        "url": url,
    }

def fetch_sports_games_for_pt_day(d_pt: date) -> list[dict]:
    all_games=[]
    for d_msk in sportsru_dates_for_pt_day(d_pt):
        for url in collect_day_links(d_msk):
            info = parse_sports_match(url)
            if info:
                all_games.append(info)
    uniq={}
    for g in all_games: uniq[g["url"]] = g
    return list(uniq.values())

# -------- ESPN --------
ESPN_SB = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={ymd}"

def _espn_record(c: dict) -> str:
    for r in (c.get("records") or []):
        if (r.get("type") in ("total", "overall")) and r.get("summary"):
            return r["summary"]
    rec = c.get("record")
    if isinstance(rec, dict) and rec.get("summary"): return rec["summary"]
    if isinstance(rec, str) and rec.strip(): return rec.strip()
    if c.get("standingSummary"): return str(c.get("standingSummary")).strip()
    t = c.get("team") or {}; trec = t.get("record")
    if isinstance(trec, dict):
        if trec.get("summary"): return trec["summary"]
        ov = trec.get("overall") or {}
        if ov.get("summary"): return ov["summary"]
    if isinstance(trec, str) and trec.strip(): return trec.strip()
    if t.get("standingSummary"): return str(t.get("standingSummary")).strip()
    return ""

def _intish(x):
    try: return int(float(x))
    except Exception: return 0

def fetch_espn_events_for_day(d: date) -> list[dict]:
    try:
        r = S.get(ESPN_SB.format(ymd=d.strftime("%Y%m%d")), timeout=HTTP_TIMEOUT)
        if r.status_code != 200: return []
        j = r.json()
    except Exception:
        return []
    out=[]
    for ev in (j.get("events") or []):
        try:
            comp = (ev.get("competitions") or [None])[0] or {}
            comps = comp.get("competitors") or []
            if len(comps) != 2: continue
            home = next(c for c in comps if c.get("homeAway")=="home")
            away = next(c for c in comps if c.get("homeAway")=="away")
            th = (home.get("team") or {}); ta = (away.get("team") or {})
            abbr_h = canon_abbr(th.get("abbreviation")); abbr_a = canon_abbr(ta.get("abbreviation"))
            status = (ev.get("status") or {}).get("type") or {}
            completed = bool(status.get("completed", False))
            comp_date = comp.get("startDate") or comp.get("date") or ev.get("date") or ""
            out.append({
                "eventId": str(ev.get("id") or ""),
                "completed": completed,
                "utcDate": str(ev.get("date") or ""),
                "utcCompDate": str(comp_date),
                "home": {"abbr": abbr_h, "teamId": str(th.get("id") or ""),
                         "score": _intish(home.get("score", 0)), "winner": bool(home.get("winner", False)),
                         "record": _espn_record(home)},
                "away": {"abbr": abbr_a, "teamId": str(ta.get("id") or ""),
                         "score": _intish(away.get("score", 0)), "winner": bool(away.get("winner", False)),
                         "record": _espn_record(away)},
                "_raw_comp": comp, "_raw_ev": ev,
            })
        except Exception:
            continue
    return out

def _espn_pt_start_dt(e: dict):
    tz_pt = ZoneInfo("America/Los_Angeles")
    iso = None
    try:
        comp = e.get("_raw_comp") or (e.get("competitions") or [None])[0] or {}
        iso = comp.get("startDate") or comp.get("date")
        if not iso:
            st = ((e.get("_raw_ev") or {}).get("status") or {}).get("type") or {}
            iso = st.get("startDate") or st.get("date")
        if not iso:
            iso = e.get("utcCompDate") or e.get("utcDate") or (e.get("_raw_ev") or {}).get("date")
        if iso:
            iso = iso.replace("Z","+00:00")
            return datetime.fromisoformat(iso).astimezone(tz_pt)
    except Exception:
        pass
    return None

def espn_completed_events_for_pt_day(d_pt: date) -> list[dict]:
    raw=[]
    for d in espn_dates_for_pt_day(d_pt):
        raw.extend(fetch_espn_events_for_day(d))
    out=[]
    for e in raw:
        if not e.get("completed"): continue
        dt = _espn_pt_start_dt(e)
        if dt and dt.date() == d_pt:
            out.append(e)
    log(f"[DBG] ESPN completed for {d_pt}: {len(out)}")
    return out

# -------- MATCHING & FIXUPS --------
def find_sportsru_match_for_event(sru_games: list[dict], e: dict) -> dict | None:
    eh, ea = canon_abbr(e["home"]["abbr"]), canon_abbr(e["away"]["abbr"])
    ev_key = frozenset([eh, ea])
    candidates=[]
    for g in sru_games:
        ga, gb = g["teamA"]["abbr"], g["teamB"]["abbr"]
        g_key = frozenset([ga, gb])
        if g_key == ev_key:
            candidates.append(g)
    if not candidates:
        return None
    # если есть точное совпадение по счёту — берём его
    for g in candidates:
        A,B = g["teamA"], g["teamB"]
        if (A["abbr"]==eh and A["score"]==e["home"]["score"] and B["score"]==e["away"]["score"]) or \
           (B["abbr"]==eh and B["score"]==e["home"]["score"] and A["score"]==e["away"]["score"]):
            return g
    # иначе — тот, где больше игроков (скорее всего финальная таблица уже есть)
    best = max(candidates, key=lambda g: len(g["players"].get(g["teamA"]["name"],[]))+len(g["players"].get(g["teamB"]["name"],[])))
    return best

def synthesize_game_from_espn(e: dict) -> dict:
    Aabbr = canon_abbr(e["home"]["abbr"])
    Babbr = canon_abbr(e["away"]["abbr"])
    Aname = ABBR_TO_RU.get(Aabbr, Aabbr)
    Bname = ABBR_TO_RU.get(Babbr, Babbr)
    return {
        "teamA": {"name": Aname, "abbr": Aabbr, "score": int(e["home"]["score"])},
        "teamB": {"name": Bname, "abbr": Babbr, "score": int(e["away"]["score"])},
        "players": {Aname: [], Bname: []},
        "records": {
            Aabbr: e["home"].get("record", ""),
            Babbr: e["away"].get("record", "")
        },
        "url": f"espn:{e.get('eventId','')}",
    }

def _score_suspicious(a: int, b: int) -> bool:
    # Явные мусоры от sports.ru (0:0, 200+, 23:59-ловушки и т.д.)
    if a is None or b is None: return True
    if a == 0 and b == 0: return True
    if a >= 200 or b >= 200: return True
    s = a + b
    if s < 60 or s > 300: return True
    return False

def fix_scores_with_espn(info: dict, e: dict):
    A,B = info["teamA"], info["teamB"]
    a,b = int(A.get("score") or 0), int(B.get("score") or 0)
    if _score_suspicious(a,b):
        # подменяем на ESPN
        if canon_abbr(e["home"]["abbr"]) == A["abbr"]:
            A["score"] = int(e["home"]["score"]); B["score"] = int(e["away"]["score"])
        else:
            A["score"] = int(e["away"]["score"]); B["score"] = int(e["home"]["score"])

# -------- FORMAT --------
def initials_ru(full: str) -> str:
    parts = [p for p in re.split(r"\s+", (full or "").strip()) if p]
    if not parts: return full or ""
    if len(parts) == 1: return parts[0]
    first = parts[0]; last = parts[-1]
    if last.lower() in {"jr.","jr","мл.","ст.","sr.","sr"} and len(parts)>=3:
        last = parts[-2] + " " + last
    return f"{first[0]}. {last}"

def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def ru_forms(label: str, v: int) -> str:
    if label=="pts": return f"{v} {ru_plural(v, ('очко','очка','очков'))}"
    if label=="reb": return f"{v} {ru_plural(v, ('подбор','подбора','подборов'))}"
    if label=="ast": return f"{v} {ru_plural(v, ('передача','передачи','передач'))}"
    if label=="stl": return f"{v} {ru_plural(v, ('перехват','перехвата','перехватов'))}"
    if label=="blk": return f"{v} {ru_plural(v, ('блок-шот','блок-шота','блок-шотов'))}"
    return f"{v}"

def hot_mark(p: dict) -> str:
    if (p["pts"]>=35) or (p["reb"]>=15) or (p["ast"]>=12) or (p["stl"]>=5) or (p["blk"]>=5):
        return " 🔥"
    return ""

def is_dd(p: dict) -> bool:
    return sum(x>=10 for x in [p["pts"],p["reb"],p["ast"],p["stl"],p["blk"]]) >= 2

def second_ok(p: dict) -> bool:
    return (p["pts"]>=20) or is_dd(p) or (p["stl"]>=6) or (p["blk"]>=6)

def score_key(p: dict):
    return (p["pts"], p["reb"]+p["ast"], p["stl"]+p["blk"])

def pick_team_players(abbr: str, rows: list[dict]) -> list[tuple[dict,bool,bool]]:
    if not rows: return []
    rows = sorted(rows, key=score_key, reverse=True)
    special_keys=[]
    if abbr=="BKN": special_keys=["дёмин","demin"]
    if abbr=="MIA": special_keys=["голдин","goldin"]
    special=None
    for p in rows:
        nm=(p["name"] or "").lower()
        if any(k in nm for k in special_keys):
            special=p; break
    out=[]
    top = rows[0]
    if special and special["name"] == top["name"]:
        out.append((special, True, True))
    elif special:
        out.append((top, False, False)); out.append((special, True, True))
    else:
        out.append((top, False, False))
    if len(out)<2:
        for p in rows[1:]:
            if p["name"] == top["name"]: continue
            if second_ok(p): out.append((p, False, False)); break
    return out[:2]

def sp(s: str) -> str: return f'<span class="tg-spoiler">{s}</span>'

def format_player_regular(p: dict, bold=False) -> str:
    name = initials_ru(p["name"])
    if bold: name = f"<b>{name}</b>"
    out = [ru_forms("pts", p["pts"])]
    if p["reb"]>=5: out.append(ru_forms("reb", p["reb"]))
    if p["ast"]>=5: out.append(ru_forms("ast", p["ast"]))
    if p["stl"]>=4: out.append(ru_forms("stl", p["stl"]))
    if p["blk"]>=4: out.append(ru_forms("blk", p["blk"]))
    return f"{name}: " + ", ".join(out) + hot_mark(p)

def format_player_special(p: dict) -> str:
    name = f"<b>{initials_ru(p['name'])}</b>"
    stats=[("pts",p["pts"]),("reb",p["reb"]),("ast",p["ast"]),("stl",p["stl"]),("blk",p["blk"]) ]
    stats=[(k,v) for k,v in stats if v>0]; stats.sort(key=lambda kv: kv[1], reverse=True)
    chosen=stats[:3]
    return f"{name}: " + ", ".join(ru_forms(k,v) for k,v in chosen) + hot_mark(p)

def format_score_line(name_ru: str, abbr: str, score: int, winner: bool, record: str) -> str:
    score_txt = f"<b>{score}</b>" if winner else f"{score}"
    if record: score_txt += f" ({record})"
    return f"{emoji_token(abbr)} {name_ru}: {sp(score_txt)}"

def build_block(info: dict) -> str:
    A,B = info["teamA"], info["teamB"]
    recA = info.get("records", {}).get(A["abbr"], "")
    recB = info.get("records", {}).get(B["abbr"], "")
    a_win = A["score"] > B["score"]; b_win = B["score"] > A["score"]
    head = (
        f"{format_score_line(ABBR_TO_RU.get(A['abbr'], A['name']), A['abbr'], A['score'], a_win, recA)}\n"
        f"{format_score_line(ABBR_TO_RU.get(B['abbr'], B['name']), B['abbr'], B['score'], b_win, recB)}\n\n"
    )
    rowsA = info["players"].get(ABBR_TO_RU.get(A["abbr"], A["name"]), []); rowsB = info["players"].get(ABBR_TO_RU.get(B["abbr"], B["name"]), [])
    al = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p,bold,det) in pick_team_players(A["abbr"], rowsA)]
    bl = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p,bold,det) in pick_team_players(B["abbr"], rowsB)]
    lines=[]
    if al: lines.extend(al)
    if al and bl: lines.append("")
    if bl: lines.extend(bl)
    return head + ("\n".join(lines) if lines else "")

# -------- Telegram (custom emoji entities) --------
def _u16len(s: str) -> int:
    return len(s.encode("utf-16-le")) // 2

def tg_send(text: str):
    if not BOT_TOKEN or not (CHAT_ID or ALT_CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID/ALT_CHAT_ID не заданы")
    chat_id = ALT_CHAT_ID or CHAT_ID

    # {EMO:(ABBR)} → custom emoji / default
    entities=[]
    out_parts=[]; last=0
    for m in re.finditer(r"\{\s*EMO:\s*\(([A-Z]{2,5})\)\s*\}", text):
        raw = m.group(1).upper()
        abbr = canon_abbr(raw)
        out_parts.append(text[last:m.start()])
        start_offset = _u16len("".join(out_parts))
        if abbr in CUSTOM_EMOJI:
            out_parts.append("⬤")
            entities.append({
                "type": "custom_emoji",
                "offset": start_offset,
                "length": 1,
                "custom_emoji_id": CUSTOM_EMOJI[abbr]
            })
        else:
            out_parts.append(TEAM_EMOJI_DEFAULT.get(abbr, "🏀"))
        last = m.end()
    out_parts.append(text[last:])
    final_text = "".join(out_parts)

    if DRY_RUN:
        log("[DRY tg_send] " + final_text[:160].replace("\n"," ") + ("..." if len(final_text)>160 else ""))
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": final_text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if entities: payload["entities"] = entities
    r = S.post(url, json=payload, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text}")

# -------- MAIN --------
def main():
    # PING
    if SEND_TEST_MSG:
        tg_send(f"✅ NBA Live bot ping {datetime.now().isoformat()}")
        print("OK ping")
        return

    d_pt = pick_report_date_pacific_env()
    Path(".posted").mkdir(parents=True, exist_ok=True)

    # LIST
    if LIST_EVENTS:
        evs = espn_completed_events_for_pt_day(d_pt)
        for e in evs:
            print(f"{e['eventId']}  {canon_abbr(e['away']['abbr'])}@{canon_abbr(e['home']['abbr'])}  "
                  f"{e['home']['score']}-{e['away']['score']}")
        print("OK list")
        return

    # ESPN финалы
    evs = espn_completed_events_for_pt_day(d_pt)
    if ONLY_EVENT_ID:
        evs = [e for e in evs if e["eventId"] == ONLY_EVENT_ID]

    # sports.ru
    sru_games = fetch_sports_games_for_pt_day(d_pt)

    posted_now = 0
    for e in evs:
        eid = e["eventId"]
        marker = Path(f".posted/event_{eid}.done")
        if marker.exists():
            log(f"[DBG] SKIP already posted {eid}")
            continue

        g = find_sportsru_match_for_event(sru_games, e)
        info = g if g else synthesize_game_from_espn(e)
        info["teamA"]["abbr"] = canon_abbr(info["teamA"]["abbr"])
        info["teamB"]["abbr"] = canon_abbr(info["teamB"]["abbr"])
        info["records"] = {
            canon_abbr(e["home"]["abbr"]): e["home"].get("record",""),
            canon_abbr(e["away"]["abbr"]): e["away"].get("record",""),
        }
        # перепроверяем счёт
        fix_scores_with_espn(info, e)

        text = build_block(info)

        if DRY_RUN:
            log(f"[DRY] would post event {eid}: {canon_abbr(e['away']['abbr'])}@{canon_abbr(e['home']['abbr'])}  "
                f"{info['teamA']['score']}-{info['teamB']['score']}")
        else:
            tg_send(text)
            marker.write_text(datetime.now().isoformat())
            posted_now += 1
            log(f"[DBG] posted event {eid} -> {info['teamA']['score']}-{info['teamB']['score']}")

    print(f"OK posted={posted_now}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
