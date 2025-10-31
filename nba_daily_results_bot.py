#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU)
• Матчи/счёт/игроки на русском: Sports.ru (боксскор). Если нельзя надёжно прочитать шапку таблицы — подхватываем только игроков через ESPN.
• Русские фамилии: профиль/поиск Sports.ru; если не нашли — латиница.
• Кастом-эмодзи команд: ENV TEAM_EMOJI_JSON; если значение — числовой ID, рендерим как <tg-emoji emoji-id="...">.
• Формат: названия видны; счёт и игроки — в спойлерах. Счёт победителя жирным. «(ОТ)» только при >4 периодов.
"""

import os, sys, re, json
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from urllib.parse import urljoin, quote_plus

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ========== ENV ==========
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TEAM_EMOJI_JSON = os.getenv("TEAM_EMOJI_JSON", "").strip()
DEBUG = os.getenv("DEBUG_NBA", "").strip() not in {"", "0", "false", "False"}

def logdbg(*a):
    if DEBUG:
        print("[DBG]", *a, file=sys.stderr)

# ========== DATES / RU ==========
RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def et_today() -> date:
    return datetime.now(ZoneInfo("America/New_York")).date()

# ========== HTTP ==========
def make_session():
    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.6,
              status_forcelist=[429,500,502,503,504],
              allowed_methods=["GET","POST"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({
        "User-Agent": "NBA-ResultsBot/7.1 (sportsru primary, espn fallback)",
        "Accept-Language": "ru,en;q=0.7",
    })
    return s
S = make_session()

def _get_json(url: str) -> dict:
    try:
        r = S.get(url, timeout=25)
        if r.status_code != 200: return {}
        return r.json()
    except Exception:
        return {}

# ========== CONSTANTS ==========
SPORTS_DAY_TMPL = "https://www.sports.ru/stat/basketball/center/end/{y:04d}/{m:02d}/{d:02d}.html"
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

# ESPN fallback
ESPN_SB = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/scoreboard?dates={ymd}"
ESPN_BOX = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/boxscore?event={eid}"

# ========== TEAM EMOJI ==========
def load_team_emojis() -> dict[str,str]:
    if not TEAM_EMOJI_JSON: return {}
    try:
        d = json.loads(TEAM_EMOJI_JSON)
        if isinstance(d, dict):
            return {k.upper(): str(v) for k,v in d.items()}
    except Exception:
        pass
    return {}
TEAM_EMOJI = load_team_emojis()
def team_emoji(abbr: str) -> str:
    val = TEAM_EMOJI.get((abbr or "").upper())
    if not val:  # дефолтная иконка
        return "🏀"
    if re.fullmatch(r"\d{6,}", val):  # ID кастом-эмодзи
        return f'<tg-emoji emoji-id="{val}"></tg-emoji>'
    return val

# ========== HELPERS ==========
def abbr_from_url(url: str, side: int) -> str | None:
    m = re.search(r"/basketball/match/([a-z0-9\-]+)-vs-([a-z0-9\-]+)/", url)
    if not m: return None
    slug = m.group(1 + side)
    return SLUG2ABBR.get(slug)

def fetch_day_links(d: date) -> list[str]:
    url = SPORTS_DAY_TMPL.format(y=d.year, m=d.month, d=d.day)
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        logdbg("SPORTS DAY FAIL", url, r.status_code); return []
    soup = BeautifulSoup(r.text, "html.parser")
    out, seen = [], set()
    for a in soup.find_all("a", href=True):
        h = a["href"]
        if not (h.startswith("/basketball/match/") or h.startswith(MATCH_PREFIX)): continue
        full = urljoin("https://www.sports.ru", h).split("#", 1)[0]
        if "vs" not in full: continue
        if not full.endswith("/"): full += "/"
        a_abbr, b_abbr = abbr_from_url(full, 0), abbr_from_url(full, 1)
        if not (a_abbr and b_abbr and a_abbr in TEAM_RU and b_abbr in TEAM_RU): continue
        if full not in seen:
            out.append(full); seen.add(full)
    logdbg("SPORTS LINKS", len(out))
    return out

def detect_ot(soup: BeautifulSoup) -> bool:
    for h3 in soup.find_all("h3"):
        t = (h3.get_text(" ", strip=True) or "").lower()
        if "период" in t or "по периодам" in t:
            tbl = h3.find_next("table")
            if not tbl: continue
            row = tbl.find("tr")
            if not row: continue
            pairs = re.findall(r"\b\d{1,3}\s*:\s*\d{1,3}\b", row.get_text(" ", strip=True))
            return len(pairs) > 4
    return False

# ========== SPORTS.RU PARSERS ==========
def norm_header(s: str) -> str:
    t = re.sub(r"\s+", " ", (s or "").strip().lower())
    t = t.replace("очки", "pts").replace("передачи","ast").replace("перед","ast")
    t = t.replace("подборы","reb").replace("подбор","reb")
    t = t.replace("перехваты","stl").replace("перехв","stl").replace("перех","stl")
    t = t.replace("блок-шоты","blk").replace("блокшоты","blk").replace("блок","blk")
    return t

def map_header_indices(tbl) -> dict[str,int]:
    # ищем thead, если его нет — пробуем первую строку из th
    thead = tbl.find("thead")
    headers = []
    if thead:
        trs = thead.find_all("tr")
        if trs:
            headers = [c.get_text(" ", strip=True) for c in trs[-1].find_all(["th","td"])]
    if not headers:
        first = tbl.find("tr")
        if first:
            headers = [c.get_text(" ", strip=True) for c in first.find_all(["th","td"])]
    idx = {}
    for i, h in enumerate(headers):
        nh = norm_header(h)
        if "pts" in nh and "pts" not in idx: idx["pts"] = i
        if re.search(r"\breb\b", nh) and "reb" not in idx: idx["reb"] = i
        if re.search(r"\bast\b", nh) and "ast" not in idx: idx["ast"] = i
        if re.search(r"\bstl\b", nh) and "stl" not in idx: idx["stl"] = i
        if re.search(r"\bblk\b", nh) and "blk" not in idx: idx["blk"] = i
    if idx:
        logdbg("HEAD OK:", headers, "→", idx)
    else:
        logdbg("HEAD MISSING")
    return idx

def extract_team_block(soup: BeautifulSoup, team_ru: str):
    # ищем ближайший к заголовку «Статистика игроков <Команда>»
    for h in soup.find_all(["h2","h3","h4"]):
        txt = h.get_text(" ", strip=True)
        if "статистика игроков" in txt.lower() and team_ru.lower() in txt.lower():
            tbl = h.find_next("table")
            if tbl: return tbl
    # fallback: первый встречный блок статистики игроков
    for h in soup.find_all(["h2","h3","h4"]):
        if "статистика игроков" in h.get_text(" ", strip=True).lower():
            tbl = h.find_next("table")
            if tbl: return tbl
    return None

def parse_players_from_table(tbl) -> tuple[list[dict], bool]:
    """Возвращает (players, header_ok). Если header_ok=False — не использовать (триггерим ESPN)."""
    idx = map_header_indices(tbl)
    if not idx:
        return [], False
    tbody = tbl.find("tbody") or tbl
    players = []
    for tr in tbody.find_all("tr"):
        cells = tr.find_all(["td","th"])
        if not cells: continue
        # имя — ищем первый «не номер», чаще в <a>
        name = ""
        for j in range(min(3, len(cells))):
            cand = cells[j].get_text(" ", strip=True)
            if re.fullmatch(r"[#№]?\d{1,3}", cand):  # номер
                continue
            a = cells[j].find("a")
            name = (a.get_text(" ", strip=True) if a else cand).strip()
            break
        if not name or name.isdigit():
            continue
        def val(key):
            i = idx.get(key, -1)
            if i < 0 or i >= len(cells): return 0
            raw = cells[i].get_text(" ", strip=True)
            m = re.search(r"-?\d+", raw)
            return int(m.group(0)) if m else 0
        players.append({
            "name_ru": name,
            "pts": val("pts"), "reb": val("reb"), "ast": val("ast"),
            "stl": val("stl"), "blk": val("blk"),
        })
    return players, True

def parse_sports_match(url: str) -> dict | None:
    r = S.get(url, timeout=25)
    if r.status_code != 200: return None
    soup = BeautifulSoup(r.text, "html.parser")

    a_abbr = abbr_from_url(url, 0); b_abbr = abbr_from_url(url, 1)
    if not (a_abbr and b_abbr): return None
    nameA = TEAM_RU.get(a_abbr, a_abbr); nameB = TEAM_RU.get(b_abbr, b_abbr)

    # Счёт: ищем «Итого/Всего» в таблицах; если не нашли — общий паттерн на странице
    scoreA = scoreB = 0
    ot = detect_ot(soup)

    # Пытаемся собрать игроков из таблиц
    tblA = extract_team_block(soup, nameA)
    tblB = extract_team_block(soup, nameB)
    playersA, okA = ([], False)
    playersB, okB = ([], False)
    if tblA:
        playersA, okA = parse_players_from_table(tblA)
        # из итоговой строки возьмём очки, если есть
        for tr in (tblA.find("tbody") or tblA).find_all("tr"):
            t0 = tr.find(["th","td"])
            if t0 and re.search(r"^(итого|всего)\s*$", t0.get_text(" ", strip=True), re.I):
                # возьмём очки из колонки pts
                idx = map_header_indices(tblA)
                if "pts" in idx:
                    tds = tr.find_all(["td","th"])
                    if idx["pts"] < len(tds):
                        m = re.search(r"\d+", tds[idx["pts"]].get_text(" ", strip=True))
                        if m: scoreA = int(m.group(0))
                break
    if tblB:
        playersB, okB = parse_players_from_table(tblB)
        for tr in (tblB.find("tbody") or tblB).find_all("tr"):
            t0 = tr.find(["th","td"])
            if t0 and re.search(r"^(итого|всего)\s*$", t0.get_text(" ", strip=True), re.I):
                idx = map_header_indices(tblB)
                if "pts" in idx:
                    tds = tr.find_all(["td","th"])
                    if idx["pts"] < len(tds):
                        m = re.search(r"\d+", tds[idx["pts"]].get_text(" ", strip=True))
                        if m: scoreB = int(m.group(0))
                break

    # если очков нет — возьмём первое совпадение на странице
    if not (scoreA and scoreB):
        txt = soup.get_text(" ", strip=True)
        m = re.search(r"\b(\d{1,3})\s*:\s*(\d{1,3})\b", txt)
        if m:
            scoreA = scoreA or int(m.group(1))
            scoreB = scoreB or int(m.group(2))

    logdbg("SPORTS TEAMS", nameA, nameB, "SCORE", scoreA, scoreB, "A_rows", len(playersA), "B_rows", len(playersB), "ok", okA, okB)

    return {
        "url": url,
        "teams": [
            {"abbr": a_abbr, "name_ru": nameA, "score": scoreA, "players": playersA if okA else []},
            {"abbr": b_abbr, "name_ru": nameB, "score": scoreB, "players": playersB if okB else []},
        ],
        "ot": bool(ot),
    }

# ========== ESPN FALLBACK (players only) ==========
def espn_scoreboard(d: date) -> list[dict]:
    j = _get_json(ESPN_SB.format(ymd=d.strftime("%Y%m%d")))
    return j.get("events") or []

def espn_find_event_map(d: date, abbrA: str, abbrB: str):
    for ev in espn_scoreboard(d):
        comp = (ev.get("competitions") or [{}])[0]
        abbrs=set(); mapping={}
        for c in (comp.get("competitors") or []):
            t=c.get("team") or {}
            ab = (t.get("abbreviation") or "").upper()
            if ab == "GS": ab = "GSW"
            mapping[str(t.get("id") or "")]=ab
            abbrs.add(ab)
        if {abbrA, abbrB} == abbrs:
            return ev.get("id"), mapping
    return None, {}

def espn_box_players(event_id: str) -> dict[str, list[dict]]:
    j = _get_json(ESPN_BOX.format(eid=event_id))
    out={}
    for t in (j.get("players") or []):
        team = t.get("team") or {}
        tid = str(team.get("id") or "")
        arr=[]
        for grp in (t.get("statistics") or []):
            for a in (grp.get("athletes") or []):
                ath = a.get("athlete") or {}
                nm  = (ath.get("displayName") or "").strip()
                stats={}
                for k,v in (a.get("stats") or {}).items(): stats[k.lower()] = v
                for k,v in (ath.get("stats") or {}).items(): stats.setdefault(k.lower(), v)
                def iget(*keys, default=0):
                    for k in keys:
                        if k in stats:
                            try: return int(stats[k])
                            except:
                                try: return int(float(stats[k]))
                                except: pass
                    return default
                pts=iget("points","pts"); reb=iget("rebounds","reb","reboundstotal")
                ast=iget("assists","ast"); stl=iget("steals","stl"); blk=iget("blocks","blk")
                if any([pts,reb,ast,stl,blk]):
                    arr.append({"name_ru": nm, "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk, "_src":"espn"})
        # merge by name
        merged={}
        for p in arr:
            key=p["name_ru"]
            if key not in merged: merged[key]=p
            else:
                m=merged[key]
                for k in ("pts","reb","ast","stl","blk"): m[k]=max(m[k], p[k])
        out[tid]=list(merged.values())
    return out

# ========== NAMES (Sports.ru profile/search) ==========
SPORTS_RU = "https://www.sports.ru"
SRU_PERSON = SPORTS_RU + "/basketball/person/"
SRU_PLAYER = SPORTS_RU + "/basketball/player/"
SRU_SEARCH = SPORTS_RU + "/search/?q="

def _slugify(first_last: str) -> str:
    base=re.sub(r"[^a-z0-9]+","-", first_last.lower()).strip("-")
    return base

def _sru_profile(first: str, last: str) -> str | None:
    slug=_slugify(f"{first} {last}")
    for root in (SRU_PERSON, SRU_PLAYER):
        url=root+slug+"/"
        try:
            r=S.get(url, timeout=12)
            if r.status_code==200 and ("/basketball/person/" in r.url or "/basketball/player/" in r.url):
                return url
        except Exception:
            pass
    return None

def _sru_name_from(url: str) -> str | None:
    try:
        r=S.get(url, timeout=12)
        if r.status_code!=200: return None
        soup=BeautifulSoup(r.text,"html.parser")
        h=soup.find(["h1","h2"])
        full=h.get_text(" ", strip=True) if h else ""
        full=" ".join(full.split())
        return full or None
    except Exception:
        return None

def _sru_search(first: str, last: str) -> str | None:
    try:
        q=quote_plus(f"{first} {last}")
        r=S.get(SRU_SEARCH+q, timeout=12)
        if r.status_code!=200: return None
        soup=BeautifulSoup(r.text,"html.parser")
        a=soup.select_one('a[href*="/basketball/person/"]') or soup.select_one('a[href*="/basketball/player/"]')
        if not a or not a.get("href"): return None
        href=a["href"]; 
        if href.startswith("/"): href=SPORTS_RU+href
        return _sru_name_from(href)
    except Exception:
        return None

def to_initials_ru(full_or_en: str) -> str:
    parts=[p for p in re.split(r"\s+", (full_or_en or "").strip()) if p]
    if not parts: return full_or_en or ""
    if len(parts)==1: return parts[0]
    first=parts[0]; last=parts[-1]
    if last.lower() in {"jr.","jr","sr.","sr"} and len(parts)>=3:
        last=parts[-2]+" "+parts[-1]
    return f"{first[0]}. {last}"

def normalize_name_ru(p: dict) -> str:
    if p.get("_src") != "espn":
        return to_initials_ru(p.get("name_ru",""))
    # ESPN → попытка русификации через профиль/поиск
    full = p.get("name_ru","")
    parts=[x for x in full.split() if x]
    if len(parts)>=2:
        first, last = parts[0], " ".join(parts[1:])
        url=_sru_profile(first, last) or _sru_search(first, last)
        if url:
            nm=_sru_name_from(url)
            if nm:
                # в заголовке обычно «Имя Фамилия» — делаем «И. Фамилия»
                parts_ru=[w for w in nm.split() if w]
                if len(parts_ru)>=2:
                    return f"{parts_ru[0][0]}. {parts_ru[-1]}"
                return nm
    return to_initials_ru(full)

# ========== PICK PLAYERS ==========
def is_dd(p: dict) -> bool: 
    return sum(v>=10 for v in [p["pts"],p["reb"],p["ast"],p["stl"],p["blk"]])>=2
def second_ok(p: dict) -> bool:
    return p["pts"]>=20 or is_dd(p) or p["stl"]>=5 or p["blk"]>=5
def score_key(p: dict): return (p["pts"], p["reb"]+p["ast"], p["stl"]+p["blk"])

def pick_team_players(abbr: str, rows: list[dict]) -> list[tuple[dict,bool,bool]]:
    if not rows: return []
    rows = sorted(rows, key=score_key, reverse=True)
    # спец: Демин/Голдин
    special=None
    key_tail = "demin" if abbr=="BKN" else ("goldin" if abbr=="MIA" else None)
    if key_tail:
        for p in rows:
            nm=(p.get("name_ru") or "").lower()
            if key_tail in nm or "дёмин" in nm or "голдин" in nm:
                special=p; break
    out=[]
    top=rows[0]
    if special and special is top:
        out.append((special, True, True))
    elif special:
        out.append((top, False, False)); out.append((special, True, True))
    else:
        out.append((top, False, False))
        for p in rows[1:]:
            if second_ok(p): out.append((p, False, False)); break
    return out[:2]

# ========== FORMAT ==========
def sp(s: str) -> str: return f'<span class="tg-spoiler">{s}</span>'
SEP = "–––––––––––––––––––––––"

def fmt_player_line(p: dict, bold=False, special=False) -> str:
    name = normalize_name_ru(p)
    if bold: name = f"<b>{name}</b>"
    if special:
        stats=[("pts",p["pts"]),("reb",p["reb"]),("ast",p["ast"]),("stl",p["stl"]),("blk",p["blk"])]
        stats=[(k,v) for k,v in stats if v>0]
        stats.sort(key=lambda kv: kv[1], reverse=True)
        stats=stats[:3]
        def f(lbl,val):
            if lbl=="pts": return f"{val} {ru_plural(val, ('очко','очка','очков'))}"
            if lbl=="reb": return f"{val} {ru_plural(val, ('подбор','подбора','подборов'))}"
            if lbl=="ast": return f"{val} {ru_plural(val, ('передача','передачи','передач'))}"
            if lbl=="stl": return f"{val} {ru_plural(val, ('перехват','перехвата','перехватов'))}"
            return f"{val} {ru_plural(val, ('блок-шот','блок-шота','блок-шотов'))}"
        line = f"{name}: " + ", ".join(f(k,v) for k,v in stats)
    else:
        chunks=[f"{p['pts']} {ru_plural(p['pts'], ('очко','очка','очков'))}"]
        if p["reb"]>=5: chunks.append(f"{p['reb']} {ru_plural(p['reb'], ('подбор','подбора','подборов'))}")
        if p["ast"]>=5: chunks.append(f"{p['ast']} {ru_plural(p['ast'], ('передача','передачи','передач'))}")
        if p["stl"]>=4: chunks.append(f"{p['stl']} {ru_plural(p['stl'], ('перехват','перехвата','перехватов'))}")
        if p["blk"]>=4: chunks.append(f"{p['blk']} {ru_plural(p['blk'], ('блок-шот','блок-шота','блок-шотов'))}")
        line = f"{name}: " + ", ".join(chunks)
    # 🔥
    if (p["pts"]>=35) or (p["reb"]>=15) or (p["ast"]>=12) or (p["stl"]>=5) or (p["blk"]>=5):
        line += " 🔥"
    return sp(line)

def build_block(game: dict, with_records: dict[str,str] | None = None) -> str:
    t1, t2 = game["teams"][0], game["teams"][1]
    name1, name2 = t1["name_ru"], t2["name_ru"]
    ab1, ab2 = t1["abbr"], t2["abbr"]
    e1, e2 = team_emoji(ab1), team_emoji(ab2)
    a_win = t1["score"] > t2["score"]; b_win = t2["score"] > t1["score"]
    rec1 = (with_records or {}).get(ab1, ""); rec2 = (with_records or {}).get(ab2, "")
    ot_tag = " (ОТ)" if game["ot"] else ""

    head = (
        f"{e1} {name1}: {sp(f'<b>{t1['score']}</b>' if a_win else f'{t1['score']}')}\n"
        f"{e2} {name2}: {sp(f'<b>{t2['score']}</b>' if b_win else f'{t2['score']}')}{ot_tag}\n\n"
    )
    # команда 1
    lines=[]
    p1 = pick_team_players(ab1, t1.get("players") or [])
    p2 = pick_team_players(ab2, t2.get("players") or [])
    for p,bold,spec in p1:
        lines.append(fmt_player_line(p, bold=bold, special=spec))
    if p1 and p2: lines.append("")
    for p,bold,spec in p2:
        lines.append(fmt_player_line(p, bold=bold, special=spec))
    return head + ("\n".join(lines) if lines else "")

# ========== ENRICH WITH ESPN WHEN NEEDED ==========
def enrich_players_with_espn(game: dict, day: date):
    need_a = not game["teams"][0]["players"]
    need_b = not game["teams"][1]["players"]
    if not (need_a or need_b): return

    eid, id2abbr = espn_find_event_map(day, game["teams"][0]["abbr"], game["teams"][1]["abbr"])
    if not eid or not id2abbr:
        logdbg("ESPN fallback: event not found for", game["teams"][0]["abbr"], game["teams"][1]["abbr"])
        return
    by_tid = espn_box_players(eid)
    # разложим по сторонам
    ab2list = {game["teams"][0]["abbr"]: [], game["teams"][1]["abbr"]: []}
    for tid, lst in by_tid.items():
        ab = id2abbr.get(tid)
        if ab in ab2list:
            ab2list[ab].extend(lst)
    for side in (0,1):
        if not game["teams"][side]["players"]:
            game["teams"][side]["players"] = ab2list.get(game["teams"][side]["abbr"], [])

# ========== BUILD POST ==========
def pick_report_day() -> date:
    d_today = et_today()
    # если на Sports.ru сегодня есть хотя бы один NBA-матч — берём сегодня, иначе вчера
    today_cnt = len(fetch_day_links(d_today))
    if today_cnt > 0:
        logdbg("DAY PICK", d_today, "(today)")
        return d_today
    d_yest = d_today - timedelta(days=1)
    logdbg("DAY PICK", d_yest, "(yesterday fallback)")
    return d_yest

def build_post(d: date) -> str:
    links = fetch_day_links(d)
    games=[]
    for u in links:
        try:
            g = parse_sports_match(u)
            if g: games.append(g)
        except Exception as e:
            logdbg("PARSE ERROR", u, repr(e))
    # Подтянем игроков из ESPN где нужно
    for g in games:
        enrich_players_with_espn(g, d)

    n=len(games)
    title = (
        f"НБА • {ru_date(d)} • {n} {ru_plural(n, ('матч','матча','матчей'))}\n"
        "Результаты надёжно спрятаны 👇\n"
        "–––––––––––––––––––––––\n\n"
    )
    if n==0: return title.rstrip()
    blocks=[]
    for i,g in enumerate(games):
        blocks.append(build_block(g))
        if i+1 < n: blocks.append("\n–––––––––––––––––––––––\n\n")
    return (title + "".join(blocks)).strip()

# ========== TELEGRAM ==========
def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    url=f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r=S.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }, timeout=25)
    if r.status_code!=200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text}")

# ========== MAIN ==========
if __name__ == "__main__":
    try:
        day = pick_report_day()
        post = build_post(day)
        tg_send(post)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
