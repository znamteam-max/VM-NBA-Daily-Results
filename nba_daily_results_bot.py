#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU) — Sports.ru primary, Covers+ESPN+Sports.ru pairs, spoilers.

• Матчи/игроки: Sports.ru (русские фамилии), параллельно.
• Пары дня: объединение Covers + ESPN + найденных матчей Sports.ru (порядок: Covers → ESPN → Sports.ru).
• Если по паре нет страницы на Sports.ru — добираем счёт/игроков через ESPN.
• Счёт и строки игроков — под HTML-спойлером; видны только названия команд и эмодзи.
• В счёте: жирным число у победителя; после счёта — сезонный рекорд команды в скобках.
• Спец-правила: Дёмин (BKN) и Голдин (MIA) — подробно (топ-3 метрики > 0), всегда, если играли.
• Второй игрок добавляется, если: ≥20 очков ИЛИ дабл-дабл ИЛИ ≥6 перехватов/блок-шотов.
"""

import os, sys, re, json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except Exception:
    Retry = None
from bs4 import BeautifulSoup

# ---------- ENV ----------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TEAM_EMOJI_JSON = os.getenv("TEAM_EMOJI_JSON", "").strip()  # опционально: {"BOS":"<emoji>",...}

# ---------- HTTP ----------
HTTP_TIMEOUT = 9
def _make_adapter():
    if Retry is not None:
        r = Retry(total=3, connect=3, read=3, backoff_factor=0.4,
                  status_forcelist=[429,500,502,503,504],
                  allowed_methods=["GET","POST"])
        return HTTPAdapter(max_retries=r)
    return HTTPAdapter(max_retries=2)

def make_session():
    s = requests.Session()
    ad = _make_adapter()
    s.mount("https://", ad); s.mount("http://", ad)
    s.headers.update({
        # Важно: только ASCII в заголовках
        "User-Agent": "NBA-DailyResultsBot/3.4 (sports.ru primary, covers+espn pairs, spoilers)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
        "Connection": "close",
    })
    return s

S = make_session()
def log(*a): print(*a, file=sys.stderr)

# ---------- DATES ----------
RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def pick_report_date_london() -> date:
    now = datetime.now(ZoneInfo("Europe/London"))
    return now.date() if now.hour >= 11 else (now.date() - timedelta(days=1))

def candidate_days_for_feeds() -> list[date]:
    # даём запас по дням в ET и в Лондоне, чтобы не терять «поздние» игры
    now_et = datetime.now(ZoneInfo("America/New_York"))
    base_et = now_et.date() if now_et.hour >= 8 else (now_et.date() - timedelta(days=1))
    d_lon = pick_report_date_london()
    cands = {base_et, base_et - timedelta(days=1), base_et + timedelta(days=1), d_lon}
    return sorted(cands)

# ---------- TEAMS / EMOJI ----------
TEAM_RU_TO_ABBR = {
    "Атланта":"ATL","Бостон":"BOS","Бруклин":"BKN","Шарлотт":"CHA","Чикаго":"CHI",
    "Кливленд":"CLE","Даллас":"DAL","Денвер":"DEN","Детройт":"DET","Голден Стэйт":"GSW",
    "Хьюстон":"HOU","Индиана":"IND","Клипперс":"LAC","Лейкерс":"LAL","Мемфис":"MEM",
    "Майами":"MIA","Милуоки":"MIL","Миннесота":"MIN","Новый Орлеан":"NOP","Нью-Йорк":"NYK",
    "Оклахома-Сити":"OKC","Орландо":"ORL","Филадельфия":"PHI","Финикс":"PHX","Портленд":"POR",
    "Сакраменто":"SAC","Сан-Антонио":"SAS","Торонто":"TOR","Юта":"UTA","Вашингтон":"WAS",
}
ABBR_TO_TEAM_RU = {v:k for k,v in TEAM_RU_TO_ABBR.items()}

TEAM_EMOJI_FALLBACK = {
    "ATL":"🦅","BOS":"☘️","BKN":"🕸️","CHA":"🐝","CHI":"🐂","CLE":"🛡️","DAL":"🐎","DEN":"⛏️","DET":"🔧",
    "GSW":"🗡️","HOU":"🚀","IND":"💫","LAC":"✂️","LAL":"⭐","MEM":"🐻","MIA":"🔥","MIL":"🦌","MIN":"🐺",
    "NOP":"🪶","NYK":"🗽","OKC":"⚡","ORL":"✨","PHI":"🔔","PHX":"☀️","POR":"🧭","SAC":"👑","SAS":"🪙",
    "TOR":"🦖","UTA":"🎷","WAS":"🧙",
}
def load_team_emoji_map():
    if TEAM_EMOJI_JSON:
        try:
            d = json.loads(TEAM_EMOJI_JSON)
            if isinstance(d, dict):
                return {k.upper(): str(v) for k,v in d.items()}
        except Exception:
            pass
    return TEAM_EMOJI_FALLBACK
TEAM_EMOJI = load_team_emoji_map()
def team_emoji_by_abbr(abbr: str) -> str:
    return TEAM_EMOJI.get((abbr or "").upper(), "🏀")

def canonical_ru_team(raw: str) -> str | None:
    if not raw: return None
    txt = (raw or "").strip()
    txt = re.split(r"—|\-|/|\|", txt, maxsplit=1)[0].strip()
    txt = txt.replace("«","").replace("»","").replace("“","").replace("”","").replace('"',"")
    txt = re.sub(r"\(.*?\)", "", txt).strip()
    txt = re.sub(r"\s{2,}", " ", txt)
    if txt in TEAM_RU_TO_ABBR: return txt
    for key in TEAM_RU_TO_ABBR:
        if txt.startswith(key): return key
    for key in TEAM_RU_TO_ABBR:
        if key in txt: return key
    return None

# ---------- ESPN (резерв/пары/рекорды) ----------
ESPN_SB = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/scoreboard?dates={yyyy}{mm}{dd}"
ESPN_BOX = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/boxscore?event={eid}"

def _extract_record(rec_list) -> str:
    # ищем summary вида "2-1"; если нет — собираем wins/losses
    try:
        for r in rec_list or []:
            if r.get("type") == "total" and r.get("summary"):
                return str(r["summary"])
        wins = next((int(r.get("value")) for r in rec_list or [] if r.get("type")=="wins"), None)
        loss = next((int(r.get("value")) for r in rec_list or [] if r.get("type")=="losses"), None)
        if wins is not None and loss is not None:
            return f"{wins}-{loss}"
    except Exception:
        pass
    return ""

def fetch_espn_completed_for_day(the_day: date) -> list[dict]:
    url = ESPN_SB.format(yyyy=the_day.year, mm=str(the_day.month).zfill(2), dd=str(the_day.day).zfill(2))
    try:
        r = S.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200: return []
        j = r.json()
    except Exception:
        return []
    out = []
    for ev in j.get("events") or []:
        st = ((ev.get("status") or {}).get("type") or {})
        if not bool(st.get("completed", False)):
            continue
        comp = (ev.get("competitions") or [None])[0] or {}
        comps = comp.get("competitors") or []
        if len(comps) != 2: continue
        teams = []
        for c in comps:
            t = c.get("team") or {}
            ab = (t.get("abbreviation") or "").upper()
            if ab == "GS": ab = "GSW"
            record = _extract_record(c.get("records") or [])
            teams.append({
                "abbr": ab,
                "score": int(float(c.get("score", 0))),
                "winner": bool(c.get("winner", False)),
                "teamId": str(t.get("id") or ""),
                "record": record,
            })
        if len(teams) != 2: continue
        ot = 0
        try:
            period = int((ev.get("status") or {}).get("period") or 0)
            if period and period > 4: ot = period - 4
        except Exception:
            pass
        out.append({"eventId": str(ev.get("id") or ""), "teams": teams, "ot": ot})
    # уникализация по паре
    seen=set(); uniq=[]
    for g in out:
        pair = frozenset([g["teams"][0]["abbr"], g["teams"][1]["abbr"]])
        if pair in seen: continue
        seen.add(pair); uniq.append(g)
    return uniq

def fetch_espn_completed_multi(days: list[date]) -> list[dict]:
    all_events=[]
    seen=set()
    for d in days:
        evs = fetch_espn_completed_for_day(d)
        for e in evs:
            pair = frozenset([e["teams"][0]["abbr"], e["teams"][1]["abbr"]])
            if pair in seen: continue
            seen.add(pair); all_events.append(e)
    return all_events

def fetch_espn_players(event_id: str) -> dict:
    try:
        r = S.get(ESPN_BOX.format(eid=event_id), timeout=HTTP_TIMEOUT)
        if r.status_code != 200: return {}
        j = r.json()
    except Exception:
        return {}
    out = {}
    for team_block in (j.get("players") or []):
        team = team_block.get("team") or {}
        tid = str(team.get("id") or "")
        arr = []
        for grp in (team_block.get("statistics") or []):
            for a in (grp.get("athletes") or []):
                ath = a.get("athlete") or {}
                name = (ath.get("displayName") or "").strip()
                stats_map = {}
                for k,v in (a.get("stats") or {}).items(): stats_map[k.lower()] = v
                for k,v in (ath.get("stats") or {}).items(): stats_map.setdefault(k.lower(), v)
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
                if any(v for v in (pts,reb,ast,stl,blk)):
                    arr.append({"name": name, "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk})
        # merge by name (max)
        merged = {}
        for p in arr:
            key = p["name"]
            if key not in merged: merged[key] = p
            else:
                m = merged[key]
                for k in ("pts","reb","ast","stl","blk"):
                    m[k] = max(m[k], p[k])
        out[tid] = list(merged.values())
    return out

# ---------- COVERS (пары) ----------
COVERS_URLS = [
    "https://www.covers.com/sports/nba/matchups?selectedDate={ymd}",   # YYYY-MM-DD
    "https://www.covers.com/sports/nba/matchups?selectedDate={mdy}",   # MM/DD/YYYY
    "https://www.covers.com/sports/nba/matchups",                       # «сегодня»
]
COVERS_TEAM_TO_ABBR = {
    "Atlanta Hawks":"ATL","Boston Celtics":"BOS","Brooklyn Nets":"BKN","Charlotte Hornets":"CHA","Chicago Bulls":"CHI",
    "Cleveland Cavaliers":"CLE","Dallas Mavericks":"DAL","Denver Nuggets":"DEN","Detroit Pistons":"DET","Golden State Warriors":"GSW",
    "Houston Rockets":"HOU","Indiana Pacers":"IND","LA Clippers":"LAC","Los Angeles Lakers":"LAL","Memphis Grizzlies":"MEM",
    "Miami Heat":"MIA","Milwaukee Bucks":"MIL","Minnesota Timberwolves":"MIN","New Orleans Pelicans":"NOP","New York Knicks":"NYK",
    "Oklahoma City Thunder":"OKC","Orlando Magic":"ORL","Philadelphia 76ers":"PHI","Phoenix Suns":"PHX","Portland Trail Blazers":"POR",
    "Sacramento Kings":"SAC","San Antonio Spurs":"SAS","Toronto Raptors":"TOR","Utah Jazz":"UTA","Washington Wizards":"WAS",
}
TEAM_NAME_RE = "|".join(sorted(map(re.escape, COVERS_TEAM_TO_ABBR.keys()), key=len, reverse=True))
COVERS_PAIR_RE = re.compile(rf"({TEAM_NAME_RE})\s+(?:at|vs\.?|@)\s+({TEAM_NAME_RE})", re.I)

def fetch_covers_pairs_for_day(day: date) -> list[frozenset]:
    txt_variants = [day.strftime("%Y-%m-%d"), day.strftime("%m/%d/%Y")]
    pairs=[]
    seen=set()
    for url_tpl in COVERS_URLS:
        url = url_tpl.format(ymd=txt_variants[0], mdy=txt_variants[1])
        try:
            r = S.get(url, timeout=HTTP_TIMEOUT)
            if r.status_code != 200: continue
            page = r.text
        except Exception:
            continue
        for m in COVERS_PAIR_RE.finditer(page):
            t1, t2 = m.group(1), m.group(2)
            ab1 = COVERS_TEAM_TO_ABBR.get(t1.strip())
            ab2 = COVERS_TEAM_TO_ABBR.get(t2.strip())
            if not (ab1 and ab2): continue
            key = frozenset([ab1, ab2])
            if key in seen: continue
            seen.add(key); pairs.append(key)
        if pairs:
            break
    return pairs

def fetch_covers_pairs_multi(days: list[date]) -> list[frozenset]:
    out=[]; seen=set()
    for d in days:
        ps = fetch_covers_pairs_for_day(d)
        for p in ps:
            if p in seen: continue
            seen.add(p); out.append(p)
    return out

# ---------- SPORTS.RU ----------
def day_url(d: date) -> str:
    return f"https://www.sports.ru/stat/basketball/center/end/{d:%Y/%m/%d}.html"

def _normalize_match_url(u: str) -> str:
    full = "https://www.sports.ru" + u if u.startswith("/") else u
    p = urlparse(full)
    return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

def get_html(url: str):
    try:
        r = S.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200: return None
        return BeautifulSoup(r.text, "html.parser")
    except Exception:
        return None

def collect_day_match_links(d: date) -> list[str]:
    soup = get_html(day_url(d))
    if not soup: return []
    seen=set(); out=[]
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/basketball/match/" not in href: continue
        full = _normalize_match_url(href)
        if full not in seen:
            seen.add(full); out.append(full)
    return out

def _extract_teams_via_meta(soup: BeautifulSoup) -> tuple[str|None,str|None]:
    mt = soup.find("meta", attrs={"property":"og:title"})
    title = mt["content"] if mt and mt.get("content") else (soup.title.string if soup.title and soup.title.string else "")
    if not title: return (None, None)
    parts = [p.strip() for p in title.split("—")]
    if len(parts) >= 2:
        a = canonical_ru_team(parts[0]); b = canonical_ru_team(parts[1])
        return (a, b)
    return (None, None)

def _extract_teams_via_stat_headers(soup: BeautifulSoup) -> list[str]:
    found = []
    for tag in soup.find_all(["h3","h4"]):
        t = tag.get_text(" ", strip=True)
        if "статистика игроков" in t.lower():
            t0 = t.split(".")[0].strip()
            k = canonical_ru_team(t0)
            if k: found.append(k)
    seen=set(); out=[]
    for x in found:
        if x in seen: continue
        seen.add(x); out.append(x)
    return out

def _is_nba_match(soup: BeautifulSoup) -> bool:
    txt = soup.get_text(" ", strip=True).lower()
    if "нба" in txt:
        return True
    for a in soup.find_all("a", href=True):
        if "/nba/" in a["href"]:
            return True
    return False

def parse_match(url: str) -> dict | None:
    soup = get_html(url)
    if not soup: return None
    if not _is_nba_match(soup):
        return None

    page_text = soup.get_text(" ", strip=True)
    m_score = re.search(r"(\d+)\s:\s(\d+)", page_text)
    if not m_score: return None
    scoreA, scoreB = int(m_score.group(1)), int(m_score.group(2))
    low = page_text.lower()
    finished = ("заверш" in low) or ("итог" in low) or ("матч окончен" in low)

    tail = page_text[m_score.end(): m_score.end()+240]
    pairs = re.findall(r"\d+\s:\s\d+", tail)
    ot = max(len(pairs) - 4, 0) if pairs else 0

    teamA = teamB = None
    a1, b1 = _extract_teams_via_meta(soup)
    if a1 and b1: teamA, teamB = a1, b1
    if not (teamA and teamB) or teamA == teamB:
        headers = _extract_teams_via_stat_headers(soup)
        if len(headers) >= 2:
            if not teamA: teamA = headers[0]
            if not teamB or teamB == teamA:
                teamB = next((x for x in headers[1:] if x != teamA), teamB)
    if not (teamA and teamB) or teamA == teamB:
        return None

    abbrA = TEAM_RU_TO_ABBR.get(teamA,""); abbrB = TEAM_RU_TO_ABBR.get(teamB,"")
    if not abbrA or not abbrB: return None

    def take_team_rows(team_ru_key: str) -> list[dict]:
        rows=[]; key_low = team_ru_key.lower()
        hdr=None
        for tag in soup.find_all(["h3","h4"]):
            text = tag.get_text(" ", strip=True)
            lowtxt = text.lower()
            if "статистика игроков" in lowtxt and key_low in lowtxt.split(".")[0]:
                hdr = tag; break
        if not hdr: return rows
        table = hdr.find_next("table")
        if not table: return rows
        for tr in table.find_all("tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.find_all(["td","th"])]
            if not tds: continue
            if any(x.lower().startswith("игрок") for x in tds): continue
            name_idx=None
            for i,cell in enumerate(tds[:3]):
                if re.search(r"[^\d/:% ]", cell):
                    name_idx=i; break
            if name_idx is None: continue
            name = tds[name_idx]
            nums = tds[name_idx+1:]
            if len(nums) < 14: continue
            def as_int(x: str) -> int:
                try: return int(x)
                except:
                    try: return int(float(x))
                    except: return 0
            pts = as_int(nums[0]); reb = as_int(nums[7]); ast = as_int(nums[8])
            stl = as_int(nums[10]); blk = as_int(nums[12])
            rows.append({"name": name, "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk})
        return rows

    rowsA = take_team_rows(teamA); rowsB = take_team_rows(teamB)

    return {
        "teamA": {"name": teamA, "abbr": abbrA, "emoji": team_emoji_by_abbr(abbrA), "score": scoreA},
        "teamB": {"name": teamB, "abbr": abbrB, "emoji": team_emoji_by_abbr(abbrB), "score": scoreB},
        "finished": finished, "ot": ot,
        "players": {teamA: rowsA, teamB: rowsB},
        "url": url,
    }

# ---------- PLAYERS / FORMAT ----------
def initials_ru(full_name: str) -> str:
    parts = [p for p in re.split(r"\s+", (full_name or "").strip()) if p]
    if not parts: return full_name or ""
    if len(parts) == 1: return parts[0]
    first = parts[0]
    suffixes = {"мл.","ст."}
    if parts[-1].lower() in suffixes and len(parts) >= 3:
        last = parts[-2] + " " + parts[-1]
    else:
        last = parts[-1]
    return f"{first[0]}. {last}"

def hot_mark(p: dict) -> str:
    if (p["pts"] >= 35) or (p["reb"] >= 15) or (p["ast"] >= 12) or (p["stl"] >= 5) or (p["blk"] >= 5):
        return " 🔥"
    return ""

def format_player_line_regular(p: dict, bold=False) -> str:
    name = initials_ru(p["name"])
    if bold: name = f"<b>{name}</b>"
    parts = [f"{p['pts']} {ru_plural(p['pts'], ('очко','очка','очков'))}"]
    if p["reb"] >= 5: parts.append(f"{p['reb']} {ru_plural(p['reb'], ('подбор','подбора','подборов'))}")
    if p["ast"] >= 5: parts.append(f"{p['ast']} {ru_plural(p['ast'], ('передача','передачи','передач'))}")
    if p["stl"] >= 4: parts.append(f"{p['stl']} {ru_plural(p['stl'], ('перехват','перехвата','перехватов'))}")
    if p["blk"] >= 4: parts.append(f"{p['blk']} {ru_plural(p['blk'], ('блок-шот','блок-шота','блок-шотов'))}")
    return f"{name}: " + ", ".join(parts) + hot_mark(p)

def format_player_line_special_detail(p: dict, bold=True) -> str:
    name = initials_ru(p["name"])
    if bold: name = f"<b>{name}</b>"
    stats = [("очки", p["pts"]), ("подбор", p["reb"]), ("передача", p["ast"]),
             ("перехват", p["stl"]), ("блок-шот", p["blk"])]
    stats = [(lab,val) for (lab,val) in stats if val and val > 0]
    stats.sort(key=lambda x: x[1], reverse=True)
    chosen = stats[:3]
    parts=[]
    for lab,val in chosen:
        if lab == "очки": parts.append(f"{val} {ru_plural(val, ('очко','очка','очков'))}")
        elif lab == "подбор": parts.append(f"{val} {ru_plural(val, ('подбор','подбора','подборов'))}")
        elif lab == "передача": parts.append(f"{val} {ru_plural(val, ('передача','передачи','передач'))}")
        elif lab == "перехват": parts.append(f"{val} {ru_plural(val, ('перехват','перехвата','перехватов'))}")
        elif lab == "блок-шот": parts.append(f"{val} {ru_plural(val, ('блок-шот','блок-шота','блок-шотов'))}")
    return f"{name}: " + ", ".join(parts) + hot_mark(p)

def _score_key(p: dict): return (p["pts"], p["reb"] + p["ast"], p["stl"] + p["blk"])
def _is_double_double(p: dict) -> bool:
    cats = [p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]]
    return sum(v >= 10 for v in cats) >= 2
def second_player_condition(p: dict) -> bool:
    return (p["pts"] >= 20) or _is_double_double(p) or (p["stl"] >= 6) or (p["blk"] >= 6)

def pick_players_for_team(abbr: str, rows: list[dict]) -> list[tuple[dict,bool,bool]]:
    """[(player, bold, special_detail)] — спец: BKN Дёмин, MIA Голдин (подробно, минимум 3 метрики)."""
    if not rows: return []
    rows = sorted(rows, key=_score_key, reverse=True)
    out=[]
    top = rows[0]
    # спец-игроки: поддерживаем и кириллицу, и латиницу (на случай ESPN fallback)
    special_keys = []
    if abbr == "BKN":
        special_keys = ["дёмин", "demin"]
    elif abbr == "MIA":
        special_keys = ["голдин", "goldin"]
    special=None
    if special_keys:
        for p in rows:
            nm = (p["name"] or "").lower()
            if any(k in nm for k in special_keys):
                special = p; break
    if special and special["name"] == top["name"]:
        out.append((special, True, True))
    elif special:
        out.append((top, False, False)); out.append((special, True, True))
    else:
        out.append((top, False, False))
    if len(out) < 2:
        for p in rows[1:]:
            if p["name"] == top["name"]: continue
            if second_player_condition(p):
                out.append((p, False, False)); break
    return out[:2]

# ---------- SPOILERS ----------
def sp(s: str) -> str: return f'<span class="tg-spoiler">{s}</span>'
SEP = "–––––––––––––––––––––––"

# ---------- FAST SPORTS.RU ----------
def fetch_sports_day_fast(d: date, max_workers: int = 10) -> list[dict]:
    links = collect_day_match_links(d)
    if not links: return []
    out=[]
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(parse_match, u): u for u in links}
        for fut in as_completed(futs):
            try:
                info = fut.result()
                if info and info["finished"]:
                    out.append(info)
            except Exception:
                continue
    # убрать дубли
    seen=set(); uniq=[]
    for g in out:
        pair = (g["teamA"]["abbr"], g["teamB"]["abbr"])
        if pair in seen: continue
        seen.add(pair); uniq.append(g)
    return uniq

# ---------- HEADERS ----------
def format_score_line(name_ru: str, abbr: str, score: int, is_winner: bool, record: str, ot_str: str) -> str:
    score_txt = f"<b>{score}</b>" if is_winner else f"{score}"
    if ot_str and not is_winner:
        score_txt = f"{score_txt}{ot_str}"
    if record:
        score_txt = f"{score_txt} ({record})"
    return f"{TEAM_EMOJI.get(abbr,'🏀')} {name_ru}: {sp(score_txt)}"

# ---------- BLOCK BUILDERS ----------
def build_block_from_sports(info: dict, rec_by_abbr: dict[str,str]) -> str:
    A, B = info["teamA"], info["teamB"]
    ot_str = "" if info["ot"] == 0 else (" (ОТ)" if info["ot"] == 1 else f" ({info['ot']} ОТ)")
    a_win = A["score"] > B["score"]
    b_win = B["score"] > A["score"]
    lineA = format_score_line(A["name"], A["abbr"], A["score"], a_win, rec_by_abbr.get(A["abbr"], ""), "")
    lineB = format_score_line(B["name"], B["abbr"], B["score"], b_win, rec_by_abbr.get(B["abbr"], ""), ot_str)
    head = f"{lineA}\n{lineB}\n\n"

    rowsA = info["players"].get(A["name"], [])
    rowsB = info["players"].get(B["name"], [])
    a_lines = [sp(format_player_line_special_detail(p, True) if det else format_player_line_regular(p, bold))
               for (p,bold,det) in pick_players_for_team(A["abbr"], rowsA)]
    b_lines = [sp(format_player_line_special_detail(p, True) if det else format_player_line_regular(p, bold))
               for (p,bold,det) in pick_players_for_team(B["abbr"], rowsB)]
    a_lines = [x for x in a_lines if x.strip()]
    b_lines = [x for x in b_lines if x.strip()]
    lines=[]
    if a_lines: lines.extend(a_lines)
    if a_lines and b_lines: lines.append("")  # отступ между командами
    if b_lines: lines.extend(b_lines)
    return head + ("\n".join(lines) if lines else "")

def build_block_from_espn(e: dict) -> str:
    t1, t2 = e["teams"][0], e["teams"][1]
    a_abbr, b_abbr = t1["abbr"], t2["abbr"]
    a_name = ABBR_TO_TEAM_RU.get(a_abbr, a_abbr); b_name = ABBR_TO_TEAM_RU.get(b_abbr, b_abbr)
    ot_str = "" if e.get("ot",0) == 0 else (" (ОТ)" if e["ot"] == 1 else f" ({e['ot']} ОТ)")
    lineA = format_score_line(a_name, a_abbr, t1["score"], bool(t1["winner"]), t1.get("record",""), "")
    lineB = format_score_line(b_name, b_abbr, t2["score"], bool(t2["winner"]), t2.get("record",""), ot_str)
    head = f"{lineA}\n{lineB}\n\n"

    players_by_tid = fetch_espn_players(e["eventId"])
    rowsA = players_by_tid.get(t1["teamId"], [])
    rowsB = players_by_tid.get(t2["teamId"], [])
    a_lines = [sp(format_player_line_special_detail(p, True) if det else format_player_line_regular(p, bold))
               for (p,bold,det) in pick_players_for_team(a_abbr, rowsA)]
    b_lines = [sp(format_player_line_special_detail(p, True) if det else format_player_line_regular(p, bold))
               for (p,bold,det) in pick_players_for_team(b_abbr, rowsB)]
    a_lines = [x for x in a_lines if x.strip()]
    b_lines = [x for x in b_lines if x.strip()]
    lines=[]
    if a_lines: lines.extend(a_lines)
    if a_lines and b_lines: lines.append("")
    if b_lines: lines.extend(b_lines)
    return head + ("\n".join(lines) if lines else "")

# ---------- BUILD ----------
def build_post() -> str:
    # Дата заголовка
    d_title = pick_report_date_london()
    # Кандидаты для фидов (ET ±1 и Лондон)
    days = candidate_days_for_feeds()

    # Sports.ru (игроки/счёты/русские фамилии) — берём по дню заголовка
    sports_games = fetch_sports_day_fast(d_title)
    sports_by_pair = {frozenset([g["teamA"]["abbr"], g["teamB"]["abbr"]]): g for g in sports_games}

    # Пары дня: Covers + ESPN на множестве дат
    covers_pairs = fetch_covers_pairs_multi(days)
    espn_list = fetch_espn_completed_multi(days)
    espn_by_pair = {frozenset([g["teams"][0]["abbr"], g["teams"][1]["abbr"]]): g for g in espn_list}
    espn_pairs = list(espn_by_pair.keys())

    # Карта рекордов из ESPN (abbr -> record) на пару
    records_by_pair = {}
    for pair, ev in espn_by_pair.items():
        recs = {ev["teams"][0]["abbr"]: ev["teams"][0].get("record",""),
                ev["teams"][1]["abbr"]: ev["teams"][1].get("record","")}
        records_by_pair[pair] = recs

    # Объединение пар с сохранением порядка: Covers → ESPN → Sports.ru
    ordered_pairs = []
    seen = set()
    for p in covers_pairs:
        if p in seen: continue
        seen.add(p); ordered_pairs.append(p)
    for p in espn_pairs:
        if p in seen: continue
        seen.add(p); ordered_pairs.append(p)
    for p in sports_by_pair.keys():
        if p in seen: continue
        seen.add(p); ordered_pairs.append(p)

    # Заголовок по количеству окончательных пар
    title_count = len(ordered_pairs)
    title = f"НБА • {ru_date(d_title)} • {title_count} {ru_plural(title_count, ('матч','матча','матчей'))}\n"
    title += "Результаты надёжно спрятаны 👇\n"
    title += SEP + "\n\n"

    if not ordered_pairs:
        return title.rstrip()

    blocks=[]
    for i, pair in enumerate(ordered_pairs, 1):
        if pair in sports_by_pair:
            rec_map = records_by_pair.get(pair, {})
            blocks.append(build_block_from_sports(sports_by_pair[pair], rec_map))
        elif pair in espn_by_pair:
            blocks.append(build_block_from_espn(espn_by_pair[pair]))
        else:
            # редкий случай: пара есть, но ни один источник не дал контент
            a_abbr, b_abbr = tuple(pair)
            a_name = ABBR_TO_TEAM_RU.get(a_abbr, a_abbr); b_name = ABBR_TO_TEAM_RU.get(b_abbr, b_abbr)
            a_line = f"{TEAM_EMOJI.get(a_abbr,'🏀')} {a_name}: {sp('—')}"
            b_line = f"{TEAM_EMOJI.get(b_abbr,'🏀')} {b_name}: {sp('—')}"
            blocks.append(f"{a_line}\n{b_line}\n")

        if i < len(ordered_pairs):
            blocks.append("\n" + SEP + "\n\n")

    return (title + "".join(blocks)).strip()

# ---------- TELEGRAM ----------
def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = S.post(url, json={
        "chat_id": CHAT_ID, "text": text,
        "parse_mode": "HTML", "disable_web_page_preview": True,
    }, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text}")

# ---------- MAIN ----------
if __name__ == "__main__":
    try:
        text = build_post()
        tg_send(text)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
