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

# sports.ru
SPORTS_RU   = "https://www.sports.ru"
SRU_PERSON  = SPORTS_RU + "/basketball/person/"
SRU_PLAYER  = SPORTS_RU + "/basketball/player/"
SRU_SEARCH  = SPORTS_RU + "/search/?q="

# caches
RU_MAP_PATH     = "ru_map_nba.json"      # { athleteId: {"first":"...", "last":"..."} | "Фамилия" }
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
        "User-Agent": "NBA-DailyResultsBot/2.6 (+espn; sports.ru resolver)",
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

# ---------- transliteration (fallback when sports.ru not found) ----------
_TR_MAP = [
    ("sch","ш"),("sh","ш"),("ch","ч"),("kh","х"),("ts","ц"),("ya","я"),("yu","ю"),
    ("ye","е"),("yo","ё"),("zh","ж"),("ph","ф"),
]
_TR_LET = {
    "a":"а","b":"б","c":"к","d":"д","e":"е","f":"ф","g":"г","h":"х","i":"и","j":"дж","k":"к",
    "l":"л","m":"м","n":"н","o":"о","p":"п","q":"к","r":"р","s":"с","t":"т","u":"у","v":"в",
    "w":"в","x":"кс","y":"и","z":"з",
}
def translit_en_to_ru(s: str) -> str:
    t = s.strip().lower()
    for pat,rep in _TR_MAP:
        t = re.sub(pat, rep, t)
    out = "".join(_TR_LET.get(ch, ch) for ch in t)
    # капитaлизация
    if out: out = out[0].upper() + out[1:]
    return out or s

# ---------- sports.ru resolver ----------
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
        ru_first = parts[0]
        ru_last  = parts[-1]
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
        parsed = _rus_first_last_from_header(h.get_text(" ", strip=True))
        if parsed: return parsed
    except Exception:
        return None
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
    "Ingram":"Ингрэм","Barrett":"Барретт","Antetokounmpo":"Адетокумбо","Anthony":"Энтони",
    "Wagner":"Вагнер","Bane":"Бэйн","Young":"Янг","Alexander-Walker":"Александер-Уокер",
    "Brunson":"Брансон","Towns":"Таунс","Brown":"Браун","Hauser":"Хаузер","Thomas":"Томас",
    "Porter":"Портер","Mitchell":"Митчелл","Allen":"Аллен","Durant":"Дюрэнт","Sengun":"Шенгюн",
    "Cunningham":"Каннингем","Thompson":"Томпсон","Jackson Jr.":"Джексон-младший","Jackson":"Джексон",
    "Adebayo":"Адебайо","Jovic":"Йович","Williamson":"Уильямсон","Murphy":"Мерфи",
    "Wembanyama":"Вембаньяма","Vassell":"Васселл","Davis":"Дэвис","Flagg":"Флэгг","George":"Джордж",
    "Johnson":"Джонсон","Doncic":"Дончич","Dončić":"Дончич","Reaves":"Ривз","Edwards":"Эдвардс",
    "Randle":"Рэндл","Avdija":"Авдия","Grant":"Грант","Curry":"Карри","Kuminga":"Куминга",
    "LaVine":"Лавин","Monk":"Монк","Markkanen":"Маркканен","Harden":"Харден","Leonard":"Леонард",
    "Brooks":"Брукс","Booker":"Букер","Porzingis":"Порзингис","Gilgeous-Alexander":"Гилджес-Александер",
    "Demin":"Дёмин","Goldin":"Голдин",
}

def _queue_pending(pid: str, first: str, last: str):
    if not pid or pid in _session_pending_ids or pid in RU_MAP: return
    for it in RU_PENDING:
        if it.get("id") == pid: return
    RU_PENDING.append({"id": pid, "first": first, "last": last})
    _session_pending_ids.add(pid)

def resolve_ru_name(first_en: str, last_en: str, athlete_id: str) -> tuple[str,str]:
    """
    Возвращает (ru_first, ru_last).
    При неудаче: фамилию транслитерируем.
    """
    if athlete_id and athlete_id in RU_MAP:
        val = RU_MAP[athlete_id]
        if isinstance(val, dict):
            return (val.get("first",""), val.get("last",""))
        else:
            return ("", str(val))

    url = _sportsru_try_profile(first_en or "", last_en or "")
    if url:
        got = _sportsru_from_profile(url)
        if got:
            ru_first, ru_last = got
            if athlete_id: RU_MAP[athlete_id] = {"first": ru_first, "last": ru_last}
            return ru_first, ru_last

    got = _sportsru_search(first_en or "", last_en or "")
    if got:
        ru_first, ru_last = got
        if athlete_id: RU_MAP[athlete_id] = {"first": ru_first, "last": ru_last}
        return ru_first, ru_last

    # исключения или транслитерация
    ru_last = EXCEPT_LAST.get(last_en or "", "") or translit_en_to_ru(last_en or "")
    if athlete_id: _queue_pending(athlete_id, first_en or "", last_en or "")
    # имя нам нужно только для инициала: пусть останется пустым — инициал сделаем из EN.
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
            if not (completed or state in {"post","final"}):
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

                # leaders (фоллбэк/дополнение)
                leaders_raw = c.get("leaders") or []
                leaders = {}
                for ld in leaders_raw:
                    cat = (ld.get("name") or "").lower()
                    for item in (ld.get("leaders") or []):
                        try:
                            ath = item.get("athlete") or {}
                            leaders.setdefault(cat, []).append({
                                "id": str(ath.get("id") or ""),
                                "first": (ath.get("firstName") or "").strip(),
                                "last": (ath.get("lastName") or "").strip(),
                                "value": float(item.get("value") or 0),
                                "name": (ath.get("displayName") or ath.get("fullName") or "").strip()
                            })
                        except Exception:
                            pass

                try:
                    score = int(float(c.get("score", 0)))
                except Exception:
                    score = 0
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
    if not j or not (j.get("boxscore") or j.get("players")):
        j = _get_json(ESPN_BOX_SITE + str(event_id))
    return j or {}

def _to_int(x, default=0) -> int:
    if x is None: return default
    if isinstance(x, (int, float)): return int(x)
    s = str(x).strip()
    m = re.search(r"-?\d+", s)
    return int(m.group(0)) if m else default

def parse_players_from_box(box: dict) -> dict:
    """ teamId -> [{"id","first","last","name","pts","reb","ast","stl","blk"}] """
    out: dict[str, list[dict]] = {}
    players_section = (box.get("boxscore", {}) or {}).get("players") or box.get("players") or []
    for team_block in players_section:
        team = team_block.get("team") or {}
        tid = str(team.get("id") or "")
        col: dict[str, dict] = {}

        for grp in (team_block.get("statistics") or []):
            keys = [str(k).strip().lower() for k in (grp.get("keys") or grp.get("labels") or [])]
            for a in (grp.get("athletes") or []):
                ath = a.get("athlete") or {}
                pid = str(ath.get("id") or "")
                if not pid: continue

                # надёжные имена
                first = (ath.get("firstName") or "").strip()
                last  = (ath.get("lastName")  or "").strip()
                name_full = (ath.get("displayName") or ath.get("fullName") or "").strip()
                if not (first and last):
                    parts = [p for p in re.split(r"\s+", name_full) if p]
                    if not first and parts: first = parts[0]
                    if not last:  last  = " ".join(parts[1:]) if len(parts) > 1 else (parts[0] if parts else "")
                name_fallback = name_full or (first + (" " + last if last else "")) or "Игрок"

                stats_list = a.get("stats") or []
                statmap = {}
                n = min(len(keys), len(stats_list))
                for i in range(n):
                    statmap[keys[i]] = stats_list[i]

                pts = _to_int(statmap.get("pts") or statmap.get("points") or 0)
                reb = _to_int(statmap.get("reb") or statmap.get("rebs") or statmap.get("totreb") or statmap.get("rebounds") or 0)
                ast = _to_int(statmap.get("ast") or statmap.get("assists") or 0)
                stl = _to_int(statmap.get("stl") or statmap.get("steals") or 0)
                blk = _to_int(statmap.get("blk") or statmap.get("blocks") or 0)

                if pid not in col:
                    col[pid] = {"id": pid, "first": first, "last": last, "name": name_fallback,
                                "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk}
                else:
                    m = col[pid]
                    for k,v in (("pts",pts),("reb",reb),("ast",ast),("stl",stl),("blk",blk)):
                        m[k] = max(m[k], v)

        out[tid] = list(col.values())
    return out

# ---- merge box players with scoreboard leaders (fill missing stats) ----
def merge_with_leaders(players: list[dict], leaders: dict) -> list[dict]:
    if not players and not leaders: return players
    by_id = {p["id"]: p for p in players}
    def apply(cat, key):
        for item in leaders.get(cat, []) or []:
            pid = item.get("id","")
            if not pid: continue
            val = int(float(item.get("value") or 0))
            if pid in by_id:
                by_id[pid][key] = max(by_id[pid].get(key, 0), val)
            else:
                by_id[pid] = {
                    "id": pid,
                    "first": item.get("first",""),
                    "last": item.get("last",""),
                    "name": item.get("name",""),
                    "pts": 0, "reb": 0, "ast": 0, "stl": 0, "blk": 0
                }
                by_id[pid][key] = val
    apply("points","pts"); apply("rebounds","reb"); apply("assists","ast"); apply("steals","stl"); apply("blocks","blk")
    return list(by_id.values())

def _flame(pts:int, reb:int, ast:int, stl:int, blk:int) -> str:
    dd = sum(v>=10 for v in (pts,reb,ast))
    td = dd >= 3
    if pts >= 35 or td or (pts>=30 and dd>=2):
        return " 🔥"
    return ""

def _latin_initial_to_cyr(first_en: str) -> str:
    if not first_en:
        return ""
    ch = first_en.strip()[0].upper()
    table = {
        "A":"А","B":"Б","C":"К","D":"Д","E":"Е","F":"Ф","G":"Г","H":"Х","I":"И",
        "J":"Д","K":"К","L":"Л","M":"М","N":"Н","O":"О","P":"П","Q":"К","R":"Р",
        "S":"С","T":"Т","U":"У","V":"В","W":"В","X":"К","Y":"Й","Z":"З"
    }
    return table.get(ch, ch)

def display_name_ru(p: dict, ru_first: str, ru_last: str) -> str:
    # формируем "И. Фамилия" гарантированно по-русски
    initial = ""
    if ru_first:
        initial = ru_first.strip()[0].upper()
    else:
        # пробуем из EN first, иначе из display name
        base_first = (p.get("first") or "")
        if not base_first and p.get("name"):
            base_first = p["name"].split()[0]
        initial = _latin_initial_to_cyr(base_first) or "?"

    surname = (ru_last or "").strip()
    if not surname:
        # берём EN last → транслитерация, если пусто — из name последний токен
        last_en = (p.get("last") or "")
        if not last_en and p.get("name"):
            parts = p["name"].split()
            last_en = parts[-1] if len(parts) >= 1 else ""
        surname = translit_en_to_ru(last_en or "Игрок")

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
    if pts >= 30: return True
    if dd >= 2:  return True
    if reb >= 15 or ast >= 12: return True
    if stl >= 4 or blk >= 4:   return True
    return False

def select_highlights(players: list[dict], abbr: str) -> list[tuple[dict,bool]]:
    if not players: return []
    want_special = "demin" if abbr=="BKN" else ("goldin" if abbr=="MIA" else None)

    def key_score(p):
        return (p.get("pts",0), p.get("reb",0)+p.get("ast",0), p.get("stl",0)+p.get("blk",0))

    players_sorted = sorted(players, key=key_score, reverse=True)
    picks = [p for p in players_sorted if is_highlight(p)][:2]
    if not picks:
        picks = [players_sorted[0]]

    spec = None
    if want_special:
        spec = next((p for p in players_sorted if (p.get("last","") or p.get("name","")).strip().lower().endswith(want_special)), None)
        if spec and all(spec["id"] != x["id"] for x in picks):
            if len(picks) == 2:
                picks[1] = spec
            else:
                picks.append(spec)

    out = []
    for p in picks:
        out.append((p, bool(spec and p["id"] == spec["id"])))
    return out

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

    lines = []
    added = 0

    def lines_for_team(c):
        arr = players_by_team.get(c["teamId"], [])
        # дополним недостающие значения из leaders (assists, rebounds, blocks…)
        arr = merge_with_leaders(arr, game.get("leaders_by_abbr", {}).get(c["abbr"], {}))
        picks = select_highlights(arr, c["abbr"])
        out = []
        for p, bold in picks:
            ru_first, ru_last = resolve_ru_name(p.get("first",""), p.get("last",""), p.get("id",""))
            out.append(fmt_stat_line_ru(p, ru_first, ru_last, bold))
        return out

    la = lines_for_team(a)
    lb = lines_for_team(b)
    added += len(la) + len(lb)
    if la: lines.extend(la + [""])
    if lb: lines.extend(lb)

    if added == 0:
        all_players = merge_with_leaders(players_by_team.get(a["teamId"], []), game.get("leaders_by_abbr", {}).get(a["abbr"], {})) \
                    + merge_with_leaders(players_by_team.get(b["teamId"], []), game.get("leaders_by_abbr", {}).get(b["abbr"], {}))
        if all_players:
            best = sorted(all_players, key=lambda p: (p.get("pts",0), p.get("reb",0)+p.get("ast",0), p.get("stl",0)+p.get("blk",0)), reverse=True)[0]
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
    if not chosen_day:
        chosen_day = pick_report_date()

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

    if len(text) > 3900:
        parts = []
        t = text
        while len(t) > 3900:
            cut = t.rfind(SEP, 0, 3800)
            if cut == -1: cut = t.rfind("\n\n", 0, 3800)
            if cut == -1: cut = 3800
            parts.append(t[:cut].rstrip())
            t = t[cut:].lstrip()
        parts.append(t)
        first = True
        for part in parts:
            payload = {"chat_id": CHAT_ID, "text": part, "parse_mode": "HTML", "disable_web_page_preview": True}
            if first and entities: payload["entities"] = entities
            resp = S.post(url, json=payload, timeout=25)
            if resp.status_code != 200:
                raise RuntimeError(f"Telegram error {resp.status_code}: {resp.text}")
            first = False
            time.sleep(0.3)
    else:
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
