#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU), Yahoo-based

• Источник матчей/статистики: Yahoo Sports (внутренние JSON + fallback из HTML).
• Имёны/фамилии: тянем русские фамилии со sports.ru; если не нашли — оставляем латиницу.
• Порог показа игроков:
    - очки ≥ 30, ИЛИ
    - дабл-дабл (любые два из PTS/REB/AST ≥ 10), ИЛИ
    - подборы ≥ 15, ИЛИ передачи ≥ 12, ИЛИ перехваты ≥ 4, ИЛИ блок-шоты ≥ 4.
  Спец-правило: Дёмин (BKN) и Голдин (MIA) форс-включение и выделение жирным, если играли.
• Овертайм: "(ОТ)" или "(2ОТ)" и т.п.
• Логотипы: обычные emoji из словаря или кастом-эмодзи по файлу team_emoji_ids.json (карта { "LAL": "<custom_emoji_id>" }).
  Сообщение отправляется одним постом.
"""

import os, sys, re, json, time, unicodedata
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ---------------- ENV ----------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# ---------------- YAHOO ----------------
Y_SCORE_HTML   = "https://sports.yahoo.com/nba/scoreboard/?date={date}"  # YYYY-MM-DD
Y_SCORES_API_CANDIDATES = [
    "https://sports.yahoo.com/_td/api/resource/sports.scores;leagues=nba;date={date}",
    "https://sports.yahoo.com/_td/api/resource/sports.league.scoreboard;league=nba;date={date}",
    "https://sports.yahoo.com/_td/api/resource/sports.scoreboard;leagues=nba;date={date}",
]
Y_BOX_API_CANDIDATES = [
    "https://sports.yahoo.com/_td/api/resource/sports.game.stats;gameId={gid}",
    "https://sports.yahoo.com/_td/api/resource/sports.game.stats?gameId={gid}",
    "https://sports.yahoo.com/_td/api/resource/sports.game.meta;gameId={gid}",
    "https://sports.yahoo.com/_td/api/resource/sports.game.detail;gameId={gid}",
    "https://sports.yahoo.com/_td/api/resource/sports.game;gameId={gid}",
]

# ---------------- sports.ru ----------------
SPORTS_RU   = "https://www.sports.ru"
SRU_PERSON  = SPORTS_RU + "/basketball/person/"
SRU_PLAYER  = SPORTS_RU + "/basketball/player/"
SRU_SEARCH  = SPORTS_RU + "/search/?q="

# ---------------- CACHE ----------------
RU_MAP_PATH          = "ru_map_nba.json"        # { athleteId: {"first":"Имя-ru","last":"Фамилия-ru"} | "Фамилия-legacy" }
RU_PENDING_PATH      = "ru_pending_nba.json"    # [{id, first, last}]
TEAM_CUSTOM_IDS_PATH = "team_emoji_ids.json"    # { "LAL": "custom_emoji_id", ... }

RU_MAP: dict[str, object] = {}
RU_PENDING: list[dict] = []
TEAM_CUSTOM_IDS: dict[str, str] = {}
_session_pending_ids: set[str] = set()

# ---------------- DATES ----------------
RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def ru_plural(n: int, f: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return f[2]
    if 2 <= n1 <= 4:  return f[1]
    if n1 == 1:      return f[0]
    return f[2]

# ---------------- HTTP ----------------
def make_session():
    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.7,
              status_forcelist=[429,500,502,503,504],
              allowed_methods=["GET","POST"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({
        "User-Agent": "NBA-DailyResultsBot/4.1 (Yahoo + sports.ru resolver)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
    })
    return s
S = make_session()
def log(*a): print(*a, file=sys.stderr)

def _get_json(url: str) -> dict:
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        return {}
    try:
        return r.json()
    except Exception:
        return {}

def _get_text(url: str) -> str:
    r = S.get(url, timeout=25)
    return r.text if r.status_code == 200 else ""

# ---------------- TEAMS ----------------
TEAM_RU = {
    "ATL":"Атланта","BOS":"Бостон","BKN":"Бруклин","NY":"Нью-Йорк","NYK":"Нью-Йорк","PHI":"Филадельфия",
    "TOR":"Торонто","CHI":"Чикаго","CLE":"Кливленд","DET":"Детройт","IND":"Индиана","MIL":"Милуоки",
    "DEN":"Денвер","MIN":"Миннесота","OKC":"Оклахома-Сити","POR":"Портленд","UTA":"Юта","UTAH":"Юта",
    "GS":"Голден Стэйт","GSW":"Голден Стэйт","LAC":"Клипперс","LAL":"Лейкерс","PHX":"Финикс","SAC":"Сакраменто",
    "MIA":"Майами","ORL":"Орландо","DAL":"Даллас","HOU":"Хьюстон","MEM":"Мемфис","NO":"Новый Орлеан",
    "NOP":"Новый Орлеан","SA":"Сан-Антонио","SAS":"Сан-Антонио","WSH":"Вашингтон","WAS":"Вашингтон",
}
TEAM_EMOJI = {
    "ATL":"🦅","BOS":"☘️","BKN":"🕸️","NY":"🗽","NYK":"🗽","PHI":"🔔",
    "TOR":"🦖","CHI":"🐂","CLE":"🛡️","DET":"🔧","IND":"💫","MIL":"🦌",
    "DEN":"⛏️","MIN":"🐺","OKC":"⚡","POR":"🧭","UTA":"🎷","UTAH":"🎷",
    "GS":"🗡️","GSW":"🗡️","LAC":"✂️","LAL":"⭐","PHX":"☀️","SAC":"👑",
    "MIA":"🔥","ORL":"✨","DAL":"🐎","HOU":"🚀","MEM":"🐻","NO":"🪶",
    "NOP":"🪶","SA":"🪙","SAS":"🪙","WSH":"🧙","WAS":"🧙",
}

# ---------------- CACHE I/O ----------------
def _load_json(path: str, default):
    if not os.path.exists(path): return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default
def _save_json(path: str, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

# ---------------- RU NAMES (sports.ru) ----------------
def is_cyrillic(s: str) -> bool:
    return bool(s) and any("А" <= ch <= "я" for ch in s)

def _latin_initial_to_cyr(first_en: str) -> str:
    if not first_en: return "И"
    ch = first_en.strip()[:1].upper()
    table = {"A":"А","B":"Б","C":"К","D":"Д","E":"Е","F":"Ф","G":"Г","H":"Х","I":"И","J":"Д","K":"К",
             "L":"Л","M":"М","N":"Н","O":"О","P":"П","Q":"К","R":"Р","S":"С","T":"Т","U":"У","V":"В",
             "W":"В","X":"К","Y":"Й","Z":"З"}
    return table.get(ch, "И")

def _slugify(first: str, last: str) -> str:
    base = f"{first} {last}".strip()
    base = unicodedata.normalize("NFKD", base)
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    base = base.lower()
    base = re.sub(r"[^a-z0-9]+", "-", base).strip("-")
    return base

def _sportsru_try_profile(first: str, last: str) -> str | None:
    slug = _slugify(first, last)
    for root in (SRU_PERSON, SRU_PLAYER):
        url = root + slug + "/"
        r = S.get(url, timeout=15)
        if r.status_code == 200 and ("/basketball/person/" in r.url or "/basketball/player/" in r.url):
            return url
    return None

def _rus_first_last_from_header(text: str) -> tuple[str,str] | None:
    full = " ".join(text.split())
    parts = [p for p in re.split(r"\s+", full) if p]
    if len(parts) >= 2:
        ru_first, ru_last = parts[0], parts[-1]
        if ru_last.lower() in {"мл.", "младший"} and len(parts) >= 3:
            ru_last = parts[-2] + " мл."
        return ru_first, ru_last
    return None

def _sportsru_from_profile(url: str) -> tuple[str,str] | None:
    try:
        r = S.get(url, timeout=20)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.text, "html.parser")
        h = soup.find(["h1","h2"])
        if not h: return None
        return _rus_first_last_from_header(h.get_text(" ", strip=True))
    except Exception:
        return None

def _sportsru_search(first: str, last: str) -> tuple[str,str] | None:
    try:
        q = quote_plus(f"{first} {last}".strip())
        r = S.get(SRU_SEARCH + q, timeout=20)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.text, "html.parser")
        a = soup.select_one('a[href*="/basketball/person/"]') or soup.select_one('a[href*="/basketball/player/"]')
        if not a or not a.get("href"): return None
        href = a["href"]
        if href.startswith("/"): href = SPORTS_RU + href
        return _sportsru_from_profile(href)
    except Exception:
        return None

EXCEPT_LAST = {
    "ingram":"Ингрэм","barrett":"Барретт","antetokounmpo":"Адетокумбо","anthony":"Энтони",
    "wagner":"Вагнер","bane":"Бэйн","young":"Янг","alexander-walker":"Александер-Уокер",
    "brunson":"Брансон","towns":"Таунс","brown":"Браун","hauser":"Хаузер","thomas":"Томас",
    "porter":"Портер","mitchell":"Митчелл","allen":"Аллен","durant":"Дюрэнт","sengun":"Шенгюн",
    "cunningham":"Каннингем","thompson":"Томпсон","jackson jr.":"Джексон-младший","jackson":"Джексон",
    "adebayo":"Адебайо","jovic":"Йович","williamson":"Уильямсон","murphy":"Мерфи",
    "wembanyama":"Вембаньяма","vassell":"Васселл","davis":"Дэвис","flagg":"Флэгг","george":"Джордж",
    "johnson":"Джонсон","doncic":"Дончич","dončić":"Дончич","reaves":"Ривз","edwards":"Эдвардс",
    "randle":"Рэндл","avdija":"Авдия","grant":"Грант","curry":"Карри","kuminga":"Куминга",
    "lavine":"Лавин","monk":"Монк","markkanen":"Маркканен","harden":"Харден","leonard":"Леонард",
    "brooks":"Брукс","booker":"Букер","porzingis":"Порзингис","gilgeous-alexander":"Гилджес-Александер",
    "demin":"Дёмин","goldin":"Голдин",
}
def _queue_pending(pid: str, first: str, last: str):
    if not pid or pid in _session_pending_ids: return
    if pid in RU_MAP: return
    for it in RU_PENDING:
        if it.get("id") == pid: return
    RU_PENDING.append({"id": pid, "first": first, "last": last})
    _session_pending_ids.add(pid)

def _improve_cached_if_needed(pid: str, first: str, last: str):
    cur = RU_MAP.get(pid)
    need = (not cur) or (isinstance(cur, str) and not is_cyrillic(cur)) \
           or (isinstance(cur, dict) and (not is_cyrillic(cur.get("last","")) or not cur.get("first")))
    if not need: return
    url = _sportsru_try_profile(first, last)
    got = _sportsru_from_profile(url) if url else None
    if not got: got = _sportsru_search(first, last)
    if got:
        RU_MAP[pid] = {"first": got[0], "last": got[1]}; return
    low_last = (last or "").strip().lower()
    if low_last in EXCEPT_LAST:
        RU_MAP[pid] = {"first":"", "last": EXCEPT_LAST[low_last]}; return
    RU_MAP[pid] = {"first":"", "last": (last or "").strip()}

def resolve_ru_name(first_en: str, last_en: str, athlete_id: str) -> tuple[str,str]:
    if athlete_id:
        _improve_cached_if_needed(athlete_id, first_en or "", last_en or "")
        val = RU_MAP.get(athlete_id)
        if isinstance(val, dict): return (val.get("first",""), val.get("last",""))
        if isinstance(val, str):  return ("", val)
    url = _sportsru_try_profile(first_en or "", last_en or "")
    got = _sportsru_from_profile(url) if url else None
    if not got: got = _sportsru_search(first_en or "", last_en or "")
    if got: return got
    low_last = (last_en or "").strip().lower()
    ru_last = EXCEPT_LAST.get(low_last) or (last_en or "").strip()
    if athlete_id: _queue_pending(athlete_id, first_en or "", last_en or "")
    return ("", ru_last)

# ---------------- DATE PICK ----------------
def pick_report_date() -> date:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return (now_et.date() - timedelta(days=1)) if now_et.hour < 8 else now_et.date()
def pick_candidate_days() -> list[date]:
    base = pick_report_date()
    return [base, base - timedelta(days=1), base - timedelta(days=2)]

# ---------------- YAHOO SCOREBOARD ----------------
def _extract_app_json_from_html(html: str) -> dict:
    m = re.search(r"root\.App\.main\s*=\s*({.*?})\s*;\s*\n", html, flags=re.DOTALL)
    if not m:
        m = re.search(r"window\.__APOLLO_STATE__\s*=\s*({.*?});", html, flags=re.DOTALL)
    if not m: return {}
    blob = m.group(1)
    try:
        return json.loads(blob)
    except Exception:
        cleaned = re.sub(r"/\*.*?\*/", "", blob, flags=re.DOTALL)
        cleaned = re.sub(r"//.*?$", "", cleaned, flags=re.MULTILINE)
        try:
            return json.loads(cleaned)
        except Exception:
            return {}

def fetch_scoreboard_yahoo(day: date) -> list[dict]:
    dstr = day.strftime("%Y-%m-%d")
    for tmpl in Y_SCORES_API_CANDIDATES:
        j = _get_json(tmpl.format(date=dstr))
        games = _parse_scoreboard_json(j)
        if games:
            return games
    html = _get_text(Y_SCORE_HTML.format(date=dstr))
    j = _extract_app_json_from_html(html)
    games = _parse_scoreboard_json(j)
    return games

def _parse_scoreboard_json(j: dict) -> list[dict]:
    if not j: return []
    candidates = []
    t = j
    for key in ("context","dispatcher","stores","SportsScoresStore"):
        if isinstance(t, dict) and key in t: t = t[key]
        else: t = None; break
    if isinstance(t, dict): candidates.append(t)
    if "events" in j or "games" in j: candidates.append(j)
    if "scoreboard" in j: candidates.append(j["scoreboard"])

    out = []
    for store in candidates:
        games_arrays = []
        if isinstance(store, dict):
            for k,v in store.items():
                if isinstance(v, list) and v and isinstance(v[0], dict) and ("gameId" in v[0] or "id" in v[0] or "status" in v[0]):
                    games_arrays.append(v)
            arr = store.get("games") or store.get("events") or []
            if isinstance(arr, list) and arr: games_arrays.append(arr)

        for arr in games_arrays:
            for g in arr:
                try:
                    gid = str(g.get("gameId") or g.get("id") or g.get("gid") or "")
                    status = (g.get("status") or g.get("statusDisplay") or g.get("statusType") or "").lower()
                    completed = ("final" in status) or (g.get("completed") is True)
                    if not completed: continue
                    ot = ""
                    st = (g.get("statusDisplay") or g.get("status") or "")
                    st_l = str(st).lower()
                    if "ot" in st_l:
                        m = re.search(r'(\d+)\s*ot', st_l) or re.search(r'(\d)ot', st_l)
                        ot = f" ({int(m.group(1))}ОТ)" if m else " (ОТ)"
                    teams = g.get("teams") or g.get("participants") or g.get("competitors") or []
                    comp = []
                    leaders_by_abbr = {}
                    for tm in teams:
                        team = tm.get("team") or tm.get("info") or tm
                        abbr = (team.get("abbreviation") or team.get("abbr") or team.get("code") or "").upper()
                        if abbr == "GS": abbr = "GSW"
                        score = int(float(tm.get("score") or team.get("score") or 0))
                        win = bool(tm.get("winner") or team.get("winner") or False)
                        record = ""
                        rec = tm.get("record") or team.get("record")
                        if isinstance(rec, dict):
                            w = rec.get("wins") or rec.get("w") or ""
                            l = rec.get("losses") or rec.get("l") or ""
                            record = f"{w}-{l}" if (w != "" and l != "") else ""
                        elif isinstance(rec, str):
                            record = rec
                        comp.append({"abbr": abbr, "score": score, "winner": win, "record": record, "teamId": str(team.get("id") or tm.get("id") or "")})
                        # leaders (PTS/REB/AST) — если есть
                        leaders = {}
                        for catkey in ("leaders","statLeaders"):
                            if catkey in tm and isinstance(tm[catkey], list):
                                for ld in tm[catkey]:
                                    cat = str(ld.get("name") or ld.get("category") or "").lower()
                                    ath = ld.get("athlete") or ld.get("player") or {}
                                    leaders.setdefault(cat, []).append({
                                        "id": str(ath.get("id") or ""),
                                        "first": (ath.get("firstName") or "").strip(),
                                        "last":  (ath.get("lastName")  or "").strip(),
                                        "value": float(ld.get("value") or 0),
                                        "name":  (ath.get("displayName") or ath.get("fullName") or "").strip(),
                                    })
                        leaders_by_abbr[abbr] = leaders
                    if len(comp) == 2:
                        out.append({"eventId": gid, "competitors": comp, "ot": ot, "leaders_by_abbr": leaders_by_abbr})
                except Exception:
                    continue
    return out

# ---------------- YAHOO BOXES ----------------
def fetch_box_yahoo(game_id: str) -> list[dict]:
    """
    Возвращает список блоков по командам:
    [{teamId, players:[{id,first,last,name,pts,reb,ast,stl,blk}]}]
    """
    j = {}
    for tmpl in Y_BOX_API_CANDIDATES:
        j = _get_json(tmpl.format(gid=game_id))
        if j: break
    if not j:
        return []

    teams = []

    def norm_key(k: str) -> str:
        k = (k or "").lower()
        return {"points":"pts","pts":"pts","p":"pts",
                "rebounds":"reb","reb":"reb","r":"reb",
                "assists":"ast","ast":"ast","a":"ast",
                "steals":"stl","stl":"stl","s":"stl",
                "blocks":"blk","blk":"blk","b":"blk"}.get(k, k)

    def to_int(x) -> int:
        try:
            if x is None:
                return 0
            if isinstance(x, (int, float)):
                return int(x)
            if isinstance(x, str):
                m = re.search(r"-?\d+", x)
                return int(m.group(0)) if m else 0
            if isinstance(x, dict):
                for k in ("value", "val", "stat", "amount"):
                    if k in x:
                        try:
                            return int(float(x[k]))
                        except Exception:
                            continue
            return 0
        except Exception:
            return 0

    collected: dict[str, dict[str, dict]] = {}

    def ensure_player(tid: str, pid: str, first: str, last: str, name: str):
        if tid not in collected: collected[tid] = {}
        m = collected[tid].setdefault(pid or name or "", {
            "id": pid or "", "first": first or "", "last": last or "",
            "name": name or (first+" "+last).strip() or "Player",
            "pts":0,"reb":0,"ast":0,"stl":0,"blk":0
        })
        return m

    def walk(node):
        if isinstance(node, dict):
            tid = str(node.get("teamId") or node.get("teamID") or node.get("team_id") or node.get("tid") or "")
            # форма 1: playerStats + statCategories
            if "playerStats" in node and isinstance(node["playerStats"], list):
                for it in node["playerStats"]:
                    pl = it.get("player") or it.get("athlete") or {}
                    pid = str(pl.get("id") or "")
                    first = (pl.get("firstName") or "").strip()
                    last  = (pl.get("lastName") or "").strip()
                    name  = (pl.get("fullName") or pl.get("displayName") or "").strip()
                    m = ensure_player(tid, pid, first, last, name)
                    cats = it.get("statCategories") or it.get("stats") or []
                    if isinstance(cats, dict):
                        cats = [{"name":k,"value":v} for k,v in cats.items()]
                    for c in cats:
                        k = norm_key(str(c.get("name") or c.get("label") or c.get("abbr") or ""))
                        v = to_int(c.get("value"))
                        if k in ("pts","reb","ast","stl","blk"):
                            m[k] = max(m[k], v)
            # форма 2: node["stats"] — список объектов с label/value рядом с player|athlete
            if "stats" in node and isinstance(node["stats"], list) and any(isinstance(x, dict) for x in node["stats"]) and ("player" in node or "athlete" in node):
                pl = node.get("player") or node.get("athlete") or {}
                pid = str(pl.get("id") or "")
                first = (pl.get("firstName") or "").strip()
                last  = (pl.get("lastName") or "").strip()
                name  = (pl.get("fullName") or pl.get("displayName") or "").strip()
                m = ensure_player(tid, pid, first, last, name)
                for c in node["stats"]:
                    k = norm_key(str(c.get("name") or c.get("label") or c.get("abbr") or ""))
                    v = to_int(c.get("value"))
                    if k in ("pts","reb","ast","stl","blk"):
                        m[k] = max(m[k], v)
            # рекурсивно внутрь
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(j)

    for tid, players in collected.items():
        teams.append({"teamId": tid, "players": list(players.values())})
    return teams

# ---------------- HIGHLIGHTS & RENDER ----------------
def merge_with_leaders(players: list[dict], leaders: dict) -> list[dict]:
    by_id = {p["id"]: p for p in (players or [])}
    def apply(cat, key):
        for it in leaders.get(cat, []) or []:
            pid = it.get("id","")
            if not pid: continue
            val = int(float(it.get("value") or 0))
            m = by_id.setdefault(pid, {"id":pid,"first":it.get("first",""),"last":it.get("last",""),
                                       "name":it.get("name",""),"pts":0,"reb":0,"ast":0,"stl":0,"blk":0})
            m[key] = max(m[key], val)
    apply("points","pts"); apply("rebounds","reb"); apply("assists","ast")
    return list(by_id.values())

def _flame(pts:int, reb:int, ast:int, stl:int, blk:int) -> str:
    dd = sum(v>=10 for v in (pts,reb,ast))
    td = dd >= 3
    if pts >= 35 or td or (pts>=30 and dd>=2): return " 🔥"
    return ""

def _initial_ru(first_en: str, ru_first: str, fallback_name: str) -> str:
    if ru_first: return ru_first[:1].upper()
    base = first_en or (fallback_name.split()[0] if fallback_name else "")
    return _latin_initial_to_cyr(base)

def display_name_ru(p: dict, ru_first: str, ru_last: str) -> str:
    init = _initial_ru(p.get("first",""), ru_first, p.get("name",""))
    surname = (ru_last or "").strip()
    if not surname:
        last_en = (p.get("last") or (p.get("name","").split()[-1] if p.get("name") else ""))
        surname = (last_en or "").strip()
    return f"{init}. {surname}"

def fmt_stat_line_ru(p: dict, ru_first: str, ru_last: str, bold: bool=False) -> str:
    name = display_name_ru(p, ru_first, ru_last)
    if bold: name = f"<b>{name}</b>"
    pts, reb, ast, stl, blk = p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]
    parts = [f"{name}: {pts} {ru_plural(pts, ('очко','очка','очков'))}"]
    if reb >= 5: parts.append(f"{reb} {ru_plural(reb, ('подбор','подбора','подборов'))}")
    if ast >= 5: parts.append(f"{ast} {ru_plural(ast, ('передача','передачи','передач'))}")
    if stl >= 4: parts.append(f"{stl} {ru_plural(stl, ('перехват','перехвата','перехватов'))}")
    if blk >= 4: parts.append(f"{blk} {ru_plural(blk, ('блок-шот','блок-шота','блок-шотов'))}")
    return ", ".join(parts) + _flame(pts,reb,ast,stl,blk)

def is_highlight(p: dict) -> bool:
    pts, reb, ast, stl, blk = p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]
    dd = sum(v>=10 for v in (pts,reb,ast))
    return (pts >= 30) or (dd >= 2) or (reb >= 15) or (ast >= 12) or (stl >= 4) or (blk >= 4)

def select_highlights(players: list[dict], abbr: str) -> list[tuple[dict,bool]]:
    if not players: return []
    want_special = "demin" if abbr=="BKN" else ("goldin" if abbr=="MIA" else None)
    def score_key(p): return (p.get("pts",0), p.get("reb",0)+p.get("ast",0), p.get("stl",0)+p.get("blk",0))
    sorted_all = sorted(players, key=score_key, reverse=True)
    picks = [p for p in sorted_all if is_highlight(p)][:2] or sorted_all[:1]
    spec = None
    if want_special:
        spec = next((p for p in sorted_all if (p.get("last","") or p.get("name","")).strip().lower().endswith(want_special)), None)
        if spec and all(spec["id"] != q["id"] for q in picks):
            if len(picks) == 2: picks[1] = spec
            else: picks.append(spec)
    return [(p, bool(spec and p["id"] == spec["id"])) for p in picks]

SEP = "–––––––––––––––––––––––"

def _team_line_text(abbr: str, score: int, record: str, winner: bool, ot_suffix: str, entities, offset_ref) -> str:
    name = TEAM_RU.get(abbr, abbr)
    s   = f"<b>{score}</b>" if winner else f"{score}"
    rec = f" ({record})" if record else ""
    use_custom = TEAM_CUSTOM_IDS.get(abbr)
    if use_custom:
        piece = f"■ {name}: {s}{rec}{ot_suffix}"
        # entity: один символ '■' заменяется кастом-эмодзи
        entities.append({"type":"custom_emoji","offset":offset_ref[0],"length":1,"custom_emoji_id":use_custom})
        offset_ref[0] += len(piece) + 1
        return piece
    else:
        emo = TEAM_EMOJI.get(abbr, "🏀")
        piece = f"{emo} {name}: {s}{rec}{ot_suffix}"
        offset_ref[0] += len(piece) + 1
        return piece

def build_game_block(game: dict, entities, offset_ref) -> str:
    comp = game["competitors"]
    if len(comp) != 2: return ""
    a, b = comp[0], comp[1]

    head_a = _team_line_text(a["abbr"], a["score"], a["record"], a["winner"], "", entities, offset_ref)
    head_b = _team_line_text(b["abbr"], b["score"], b["record"], b["winner"], game.get("ot",""), entities, offset_ref)
    head = head_a + "\n" + head_b + "\n"
    offset_ref[0] += 1

    # игроки: тянем из боксов Yahoo
    box_teams = fetch_box_yahoo(game["eventId"])  # [{teamId, players:[...]}]
    by_tid = {t["teamId"]: t.get("players", []) for t in box_teams}

    def lines_for_team(c):
        arr = by_tid.get(c["teamId"], [])
        arr = merge_with_leaders(arr, game.get("leaders_by_abbr", {}).get(c["abbr"], {}))
        picks = select_highlights(arr, c["abbr"])
        lines = []
        for p, bold in picks:
            ru_first, ru_last = resolve_ru_name(p.get("first",""), p.get("last",""), p.get("id",""))
            lines.append(fmt_stat_line_ru(p, ru_first, ru_last, bold))
        return lines

    lines = []
    la = lines_for_team(a)
    lb = lines_for_team(b)
    if la: lines.extend(la + [""])
    if lb: lines.extend(lb)

    text = head + ("\n".join(l for l in lines if l.strip()))
    offset_ref[0] += len(text) - len(head)
    return text.strip()

def build_post_text_and_entities() -> tuple[str, list]:
    global TEAM_CUSTOM_IDS
    TEAM_CUSTOM_IDS = _load_json(TEAM_CUSTOM_IDS_PATH, {})

    chosen_day = None
    games = []
    for d in pick_candidate_days():
        games = fetch_scoreboard_yahoo(d)
        if games:
            chosen_day = d
            break
    if not chosen_day: chosen_day = pick_report_date()

    title = f"НБА • {ru_date(chosen_day)} • {len(games)} {ru_plural(len(games), ('матч','матча','матчей'))}\n" \
            "Результаты надёжно спрятаны 👇\n" \
            f"{SEP}\n\n"

    entities: list[dict] = []
    offset_ref = [len(title)]  # позиция для entities (учитывает видимые символы)

    blocks = []
    for i, g in enumerate(games, 1):
        blk = build_game_block(g, entities, offset_ref)
        blocks.append(blk.strip())
        if i < len(games):
            blocks.append(f"\n{SEP}\n")
            offset_ref[0] += len(SEP) + 2

    return (title + "\n".join(blocks)).strip(), entities

# ---------------- TELEGRAM ----------------
def tg_send_single(text: str, entities: list[dict]):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if entities: payload["entities"] = entities
    resp = S.post(url, json=payload, timeout=25)
    if resp.status_code != 200:
        raise RuntimeError(f"Telegram error {resp.status_code}: {resp.text}")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    try:
        loaded_map = _load_json(RU_MAP_PATH, {})
        loaded_pending = _load_json(RU_PENDING_PATH, [])
        TEAM_CUSTOM_IDS.update(_load_json(TEAM_CUSTOM_IDS_PATH, {}))
        if isinstance(loaded_map, dict): RU_MAP.update(loaded_map)
        if isinstance(loaded_pending, list): RU_PENDING.extend(loaded_pending)

        text, entities = build_post_text_and_entities()
        tg_send_single(text, entities)

        _save_json(RU_PENDING_PATH, RU_PENDING)
        _save_json(RU_MAP_PATH, RU_MAP)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
