#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, re, json, time, unicodedata
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# ESPN endpoints
ESPN_SCORE_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
ESPN_BOX_WEB    = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/boxscore?event="
ESPN_BOX_SITE   = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/boxscore?event="
ESPN_SUMMARY    = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/summary?event="

# sports.ru
SPORTS_RU   = "https://www.sports.ru"
SRU_PERSON  = SPORTS_RU + "/basketball/person/"
SRU_PLAYER  = SPORTS_RU + "/basketball/player/"
SRU_SEARCH  = SPORTS_RU + "/search/?q="

# caches
RU_MAP_PATH     = "ru_map_nba.json"      # { athleteId: {"first":"Имя(ru)","last":"Фамилия(ru)"} | "Фамилия(legacy)" }
RU_PENDING_PATH = "ru_pending_nba.json"  # [{ id, first, last }]

RU_MAP: dict[str, object] = {}
RU_PENDING: list[dict] = []
_session_pending_ids: set[str] = set()

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
TEAM_CUSTOM_IDS_PATH = "team_emoji_ids.json"
TEAM_CUSTOM_IDS = {}

RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def ru_plural(n: int, f: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return f[2]
    if 2 <= n1 <= 4:  return f[1]
    if n1 == 1:      return f[0]
    return f[2]
def log(*a): print(*a, file=sys.stderr)

def make_session():
    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.6,
              status_forcelist=[429,500,502,503,504],
              allowed_methods=["GET","POST"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({
        "User-Agent": "NBA-DailyResultsBot/3.2 (+espn; sports.ru resolver)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
    })
    return s
S = make_session()

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

# ---------- utils ----------
def is_cyrillic(s: str) -> bool:
    return bool(s) and all(('А' <= ch <= 'я') or ch in " .-’ʼ'" for ch in s)

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

# ---------- sports.ru resolver ----------
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
    # case-insensitive ключи
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
    if not got:
        got = _sportsru_search(first, last)
    if got:
        RU_MAP[pid] = {"first": got[0], "last": got[1]}
        return
    low_last = (last or "").strip().lower()
    if low_last in EXCEPT_LAST:
        RU_MAP[pid] = {"first": "", "last": EXCEPT_LAST[low_last]}
        return
    RU_MAP[pid] = {"first": "", "last": (last or "").strip()}  # ← если ничего не нашли — оставляем латиницу

def resolve_ru_name(first_en: str, last_en: str, athlete_id: str) -> tuple[str,str]:
    if athlete_id:
        _improve_cached_if_needed(athlete_id, first_en or "", last_en or "")
        val = RU_MAP.get(athlete_id)
        if isinstance(val, dict): return (val.get("first",""), val.get("last",""))
        if isinstance(val, str):  return ("", val)  # legacy
    # без id — on-the-fly
    url = _sportsru_try_profile(first_en or "", last_en or "")
    got = _sportsru_from_profile(url) if url else None
    if not got:
        got = _sportsru_search(first_en or "", last_en or "")
    if got: return got
    low_last = (last_en or "").strip().lower()
    ru_last = EXCEPT_LAST.get(low_last) or (last_en or "").strip()  # ← латиница, если неизвестно
    if athlete_id: _queue_pending(athlete_id, first_en or "", last_en or "")
    return ("", ru_last)

# ---------- HTTP ----------
def _get_json(url: str) -> dict:
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        log("HTTP", r.status_code, url[:160])
        return {}
    try:
        return r.json()
    except Exception:
        return {}

# ---------- dates ----------
def pick_report_date() -> date:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return (now_et.date() - timedelta(days=1)) if now_et.hour < 8 else now_et.date()
def pick_candidate_days() -> list[date]:
    base = pick_report_date()
    return [base, base - timedelta(days=1), base - timedelta(days=2)]

# ---------- ESPN helpers ----------
def fetch_scoreboard(day: date) -> list[dict]:
    dates = day.strftime("%Y%m%d")
    j = _get_json(f"{ESPN_SCORE_BASE}/scoreboard?dates={dates}")
    events = j.get("events") or []
    out = []
    for ev in events:
        try:
            t = (ev.get("status") or {}).get("type") or {}
            completed = bool(t.get("completed"))
            state = str(t.get("state") or "").lower()
            if not (completed or state in {"post","final"}):  # только завершённые
                continue
            comp = (ev.get("competitions") or [])[0]
            competitors = comp.get("competitors") or []
            status_comp = (comp.get("status") or {}).get("type") or {}
            short = (status_comp.get("shortDetail") or t.get("shortDetail") or "").lower()
            ot_label = ""
            if "ot" in short:
                m = re.search(r'(\d+)\s*ot', short) or re.search(r'(\d)ot', short)
                ot_label = f" ({int(m.group(1))}ОТ)" if m else " (ОТ)"
            game = {"eventId": ev.get("id"), "competitors": [], "ot": ot_label, "leaders_by_abbr": {}}
            for c in competitors:
                team = c.get("team") or {}
                abbr = (team.get("abbreviation") or "").upper()
                if abbr == "GS": abbr = "GSW"
                # leaders этой команды
                leaders_raw = c.get("leaders") or []
                leaders = {}
                for ld in leaders_raw:
                    cat = (ld.get("name") or "").lower()
                    for item in (ld.get("leaders") or []):
                        ath = item.get("athlete") or {}
                        leaders.setdefault(cat, []).append({
                            "id": str(ath.get("id") or ""),
                            "first": (ath.get("firstName") or "").strip(),
                            "last": (ath.get("lastName") or "").strip(),
                            "value": float(item.get("value") or 0),
                            "name": (ath.get("displayName") or ath.get("fullName") or "").strip()
                        })
                score = int(float(c.get("score", 0) or 0))
                win = bool(c.get("winner", False))
                rec = ""
                for recobj in c.get("records") or []:
                    if recobj.get("type") == "total" and recobj.get("summary"):
                        rec = recobj["summary"]
                game["competitors"].append({
                    "abbr": abbr, "score": score, "winner": win,
                    "record": rec or "", "teamId": str(team.get("id") or ""),
                })
                game["leaders_by_abbr"][abbr] = leaders
            if len(game["competitors"]) == 2:
                out.append(game)
        except Exception as e:
            log("[scoreboard parse] skip:", e)
    return out

def fetch_boxscore(event_id: str) -> dict:
    j = _get_json(ESPN_BOX_WEB + str(event_id))
    if not j or not (j.get("boxscore") or j.get("players") or j.get("gamepackageJSON")):
        j = _get_json(ESPN_BOX_SITE + str(event_id))
    return j or {}

def fetch_summary(event_id: str) -> dict:
    return _get_json(ESPN_SUMMARY + str(event_id)) or {}

def _to_int_any(x, default=0) -> int:
    if x is None: return default
    if isinstance(x, (int, float)): return int(x)
    if isinstance(x, str):
        m = re.search(r"-?\d+", x)
        return int(m.group(0)) if m else default
    if isinstance(x, dict):
        for k in ("value","val"):
            if k in x:
                try: return int(float(x[k]))
                except Exception: pass
    return default

def _norm_key(k: str) -> str:
    k = (k or "").strip().lower()
    aliases = {
        "p":"pts","pts":"pts","points":"pts","point":"pts",
        "r":"reb","reb":"reb","rebs":"reb","totreb":"reb","rebounds":"reb","reboundstotal":"reb",
        "a":"ast","ast":"ast","assists":"ast","assist":"ast",
        "s":"stl","stl":"stl","steals":"stl","steal":"stl",
        "b":"blk","blk":"blk","blocks":"blk","block":"blk",
    }
    return aliases.get(k, k)

def _merge_statmap(dst: dict, src: dict | list | None, keys: list[str] | None = None):
    if not src: return
    if isinstance(src, list) and keys:
        n = min(len(keys), len(src))
        for i in range(n):
            nk = _norm_key(keys[i])
            dst[nk] = max(dst.get(nk, 0), _to_int_any(src[i], 0))
    elif isinstance(src, dict):
        for k, v in src.items():
            nk = _norm_key(k)
            dst[nk] = max(dst.get(nk, 0), _to_int_any(v, 0))

def _harvest_from_group(statmap: dict, grp: dict, athlete_item: dict | None = None):
    keys = [ _norm_key(k) for k in (grp.get("keys") or grp.get("labels") or []) ]
    stats_obj = grp.get("stats")
    if stats_obj is None and athlete_item is not None:
        stats_obj = athlete_item.get("stats")
    _merge_statmap(statmap, stats_obj, keys)
    # totals/extra
    _merge_statmap(statmap, grp.get("totals") or {})
    if athlete_item:
        ath = athlete_item.get("athlete") or {}
        _merge_statmap(statmap, ath.get("stats") or {})
        _merge_statmap(statmap, ath.get("totals") or {})
        _merge_statmap(statmap, athlete_item.get("totals") or {})

def parse_players_from_box(box: dict) -> dict:
    """ teamId -> [{"id","first","last","name","pts","reb","ast","stl","blk"}] """
    if "gamepackageJSON" in box:
        box = box["gamepackageJSON"]
    out: dict[str, list[dict]] = {}
    players_section = (box.get("boxscore", {}) or {}).get("players") or box.get("players") or []
    for team_block in players_section:
        team = team_block.get("team") or {}
        tid = str(team.get("id") or "")
        col: dict[str, dict] = {}

        # стандартные группы
        stats_groups = team_block.get("statistics") or []
        # иногда есть athletes на верхнем уровне блока
        athletes_direct = team_block.get("athletes") or []

        # 1) из groups -> athletes
        for grp in stats_groups:
            for a in (grp.get("athletes") or []):
                ath = a.get("athlete") or {}
                pid = str(ath.get("id") or a.get("id") or "")
                if not pid: continue
                first = (ath.get("firstName") or a.get("firstName") or "").strip()
                last  = (ath.get("lastName")  or a.get("lastName")  or "").strip()
                name_full = (ath.get("displayName") or a.get("displayName") or ath.get("fullName") or "").strip()
                if not (first and last):
                    parts = [p for p in re.split(r"\s+", name_full) if p]
                    if not first and parts: first = parts[0]
                    if not last:  last  = " ".join(parts[1:]) if len(parts) > 1 else (parts[0] if parts else "")
                m = col.setdefault(pid, {"id": pid, "first": first, "last": last,
                                         "name": name_full or (first + (" " + last if last else "")) or "Игрок",
                                         "pts":0,"reb":0,"ast":0,"stl":0,"blk":0})
                statmap = {}
                _harvest_from_group(statmap, grp, a)
                for k in ("pts","reb","ast","stl","blk"):
                    m[k] = max(m[k], int(statmap.get(k, 0)))

        # 2) athletes_direct (редкие структуры)
        for a in athletes_direct:
            pid = str(a.get("id") or a.get("athlete", {}).get("id") or "")
            if not pid: continue
            ath = a.get("athlete") or a
            first = (ath.get("firstName") or "").strip()
            last  = (ath.get("lastName")  or "").strip()
            name_full = (ath.get("displayName") or ath.get("fullName") or "").strip()
            if not (first and last):
                parts = [p for p in re.split(r"\s+", name_full) if p]
                if not first and parts: first = parts[0]
                if not last:  last  = " ".join(parts[1:]) if len(parts) > 1 else (parts[0] if parts else "")
            m = col.setdefault(pid, {"id": pid, "first": first, "last": last,
                                     "name": name_full or (first + (" " + last if last else "")) or "Игрок",
                                     "pts":0,"reb":0,"ast":0,"stl":0,"blk":0})
            statmap = {}
            _merge_statmap(statmap, a.get("stats") or {})
            _merge_statmap(statmap, a.get("totals") or {})
            _merge_statmap(statmap, ath.get("stats") or {})
            _merge_statmap(statmap, ath.get("totals") or {})
            for k in ("pts","reb","ast","stl","blk"):
                m[k] = max(m[k], int(statmap.get(k, 0)))

        out[tid] = list(col.values())
    return out

def augment_from_summary(players_by_team: dict, summary: dict):
    """Дополняем недостающие значения (pts,reb,ast,stl,blk) из summary."""
    box = (summary.get("boxscore") or {})
    teams = box.get("players") or []  # такая же структура, как в boxscore
    for team_block in teams:
        team = team_block.get("team") or {}
        tid = str(team.get("id") or "")
        if not tid: continue
        col = {p["id"]: p for p in players_by_team.get(tid, [])}
        # как в parse_players_from_box
        stats_groups = team_block.get("statistics") or []
        athletes_direct = team_block.get("athletes") or []
        for grp in stats_groups:
            for a in (grp.get("athletes") or []):
                ath = a.get("athlete") or {}
                pid = str(ath.get("id") or a.get("id") or "")
                if not pid: continue
                if pid not in col:
                    col[pid] = {"id": pid, "first": (ath.get("firstName") or "").strip(),
                                "last": (ath.get("lastName") or "").strip(),
                                "name": (ath.get("displayName") or ath.get("fullName") or "").strip(),
                                "pts":0,"reb":0,"ast":0,"stl":0,"blk":0}
                m = col[pid]
                statmap = {}
                _harvest_from_group(statmap, grp, a)
                for k in ("pts","reb","ast","stl","blk"):
                    m[k] = max(m[k], int(statmap.get(k, 0)))
        for a in athletes_direct:
            pid = str(a.get("id") or a.get("athlete", {}).get("id") or "")
            if not pid: continue
            b = col.setdefault(pid, {"id": pid, "first": (a.get("firstName") or "").strip(),
                                     "last": (a.get("lastName") or "").strip(),
                                     "name": (a.get("displayName") or "").strip(),
                                     "pts":0,"reb":0,"ast":0,"stl":0,"blk":0})
            statmap = {}
            _merge_statmap(statmap, a.get("stats") or {})
            _merge_statmap(statmap, a.get("totals") or {})
            aa = a.get("athlete") or {}
            _merge_statmap(statmap, aa.get("stats") or {})
            _merge_statmap(statmap, aa.get("totals") or {})
            for k in ("pts","reb","ast","stl","blk"):
                b[k] = max(b[k], int(statmap.get(k, 0)))
        players_by_team[tid] = list(col.values())

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
    apply("points","pts"); apply("rebounds","reb"); apply("assists","ast"); apply("steals","stl"); apply("blocks","blk")
    return list(by_id.values())

def _flame(pts:int, reb:int, ast:int, stl:int, blk:int) -> str:
    dd = sum(v>=10 for v in (pts,reb,ast))
    td = dd >= 3
    if pts >= 35 or td or (pts>=30 and dd>=2):
        return " 🔥"
    return ""

def display_name_ru(p: dict, ru_first: str, ru_last: str) -> str:
    # «И. Фамилия»; если русской фамилии нет — оставляем латиницу как есть
    initial = ru_first.strip()[:1].upper() if ru_first else _latin_initial_to_cyr(p.get("first") or (p.get("name","").split()[:1] or [""])[0])
    surname = (ru_last or "").strip()
    if not surname:
        last_en = (p.get("last") or (p.get("name","").split()[-1] if p.get("name") else ""))
        surname = (last_en or "").strip()  # ← латиница, без транслита
    return f"{initial}. {surname}"

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
    def score_key(p):
        return (p.get("pts",0), p.get("reb",0)+p.get("ast",0), p.get("stl",0)+p.get("blk",0))
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

    # игроки
    box = fetch_boxscore(game["eventId"])
    players_by_team = parse_players_from_box(box)

    # усиливаем из summary, чтобы добрать «пропавшие» AST/REB/STL/BLK
    try:
        summary = fetch_summary(game["eventId"])
        augment_from_summary(players_by_team, summary)
    except Exception as e:
        log("[summary fallback error]", e)

    def lines_for_team(c):
        arr = players_by_team.get(c["teamId"], [])
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

    if not any(l.strip() for l in lines):
        allp = (players_by_team.get(a["teamId"], []) or []) + (players_by_team.get(b["teamId"], []) or [])
        if allp:
            best = sorted(allp, key=lambda p:(p.get("pts",0), p.get("reb",0)+p.get("ast",0), p.get("stl",0)+p.get("blk",0)), reverse=True)[0]
            ru_first, ru_last = resolve_ru_name(best.get("first",""), best.get("last",""), best.get("id",""))
            lines.append(fmt_stat_line_ru(best, ru_first, ru_last, False))

    text = head + ("\n".join(l for l in lines if l.strip()))
    offset_ref[0] += len(text) - len(head)
    return text.strip()

def build_post_text_and_entities() -> tuple[str, list]:
    global TEAM_CUSTOM_IDS
    TEAM_CUSTOM_IDS = _load_json(TEAM_CUSTOM_IDS_PATH, {})

    chosen_day = None
    games = []
    for d in pick_candidate_days():
        games = fetch_scoreboard(d)
        if games:
            chosen_day = d
            break
    if not chosen_day: chosen_day = pick_report_date()

    title = f"НБА • {ru_date(chosen_day)} • {len(games)} {ru_plural(len(games), ('матч','матча','матчей'))}\n" \
            "Результаты надёжно спрятаны 👇\n" \
            f"{SEP}\n\n"

    entities: list[dict] = []
    offset_ref = [len(title)]

    blocks = []
    for i, g in enumerate(games, 1):
        blk = build_game_block(g, entities, offset_ref)
        blocks.append(blk.strip())
        if i < len(games):
            blocks.append(f"\n{SEP}\n")
            offset_ref[0] += len(SEP) + 2

    return (title + "\n".join(blocks)).strip(), entities

def tg_send(text: str, entities: list[dict]):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if entities: payload["entities"] = entities
    resp = S.post(url, json=payload, timeout=25)
    if resp.status_code != 200:
        raise RuntimeError(f"Telegram error {resp.status_code}: {resp.text}")

if __name__ == "__main__":
    try:
        loaded_map = _load_json(RU_MAP_PATH, {})
        loaded_pending = _load_json(RU_PENDING_PATH, [])
        if isinstance(loaded_map, dict): RU_MAP.update(loaded_map)
        if isinstance(loaded_pending, list): RU_PENDING.extend(loaded_pending)

        text, entities = build_post_text_and_entities()
        tg_send(text, entities)

        _save_json(RU_PENDING_PATH, RU_PENDING)
        _save_json(RU_MAP_PATH, RU_MAP)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
