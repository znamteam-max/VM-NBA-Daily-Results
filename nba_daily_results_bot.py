#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU)

• Контент: players/русские фамилии — sports.ru (match pages).
• Счёт и W-L (рекорды) — ESPN (site.api) по нескольким соседним датам (ET±1 и London).
• Формат: названия команд видны, счёт и игроки — в спойлерах. У победителя счёт жирным.
• Игроки:
  – 1–2 на команду; второй — если ≥20 очков ИЛИ дабл-дабл ИЛИ ≥6 STL/BLK.
  – Спец: Дёмин (BKN) и Голдин (MIA) — всегда включаем, показываем 3 максимальные метрики >0 (жирным).
• Лого: кастом-эмодзи через TEAM_EMOJI_JSON (abbr->custom_emoji_id). Иначе — дефолтные юникод-эмодзи.
"""

import os, sys, re, json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, urlunparse

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
TEAM_EMOJI_JSON = os.getenv("TEAM_EMOJI_JSON", "").strip()  # {"BOS":"<custom_emoji_id>", ...}
DEBUG = bool(os.getenv("DEBUG_NBA", "").strip())

# -------- HTTP --------
HTTP_TIMEOUT = 12

def _mk_adapter():
    if Retry is not None:
        r = Retry(total=3, connect=3, read=3, backoff_factor=0.4,
                  status_forcelist=[429,500,502,503,504],
                  allowed_methods=["GET","POST"])
        return HTTPAdapter(max_retries=r)
    return HTTPAdapter(max_retries=2)

def make_session():
    s = requests.Session()
    ad = _mk_adapter()
    s.mount("https://", ad); s.mount("http://", ad)
    s.headers.update({
        "User-Agent": "NBA-DailyResultsBot/4.3 (sports.ru + espn scores/records, spoilers, custom_emoji)",
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
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def pick_report_date_london() -> date:
    now = datetime.now(ZoneInfo("Europe/London"))
    return now.date() if now.hour >= 8 else (now.date() - timedelta(days=1))

def candidate_days_for_espn() -> list[date]:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    base_et = now_et.date() if now_et.hour >= 8 else (now_et.date() - timedelta(days=1))
    lon = pick_report_date_london()
    c = {base_et - timedelta(days=1), base_et, base_et + timedelta(days=1), lon}
    return sorted(c)

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
    "GSW":"🗡️","HOU":"🚀","IND":"💫","LAC":"✂️","LAL":"⭐","MEM":"🐻","MIA":"🔥","MIL":"🦌","MIN":"🐺",
    "NOP":"🪶","NYK":"🗽","OKC":"⚡","ORL":"✨","PHI":"🔔","PHX":"☀️","POR":"🧭","SAC":"👑","SAS":"🪙",
    "TOR":"🦖","UTA":"🎷","WAS":"🧙",
}

def load_custom_emoji():
    if not TEAM_EMOJI_JSON: return {}
    try:
        d = json.loads(TEAM_EMOJI_JSON)
        if isinstance(d, dict):
            return {k.upper(): str(v) for k,v in d.items() if v}
    except Exception:
        pass
    return {}

CUSTOM_EMOJI = load_custom_emoji()

def emoji_token(abbr: str) -> str:
    return f"{{EMO:{(abbr or '').upper()}}}"

# -------- SPORTS.RU (match pages) --------
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
    log(f"[DBG] SPORTS LINKS {len(out)}")
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
            tx = node.get_text(" ", strip=True)
            if "игрок" in tx.lower():
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
    if not (teamA and teamB):
        return None

    a_abbr = TEAM_RU_TO_ABBR.get(teamA,""); b_abbr = TEAM_RU_TO_ABBR.get(teamB,"")
    if not a_abbr or not b_abbr: return None

    ancA = _anchor_team_players(soup, teamA)
    ancB = _anchor_team_players(soup, teamB)
    if not ancA or not ancB:
        return None

    tabA = _find_table_after(ancA)
    tabB = _find_table_after(ancB)
    if not tabA or not tabB:
        return None

    rowsA = _parse_players_table(tabA)
    rowsB = _parse_players_table(tabB)
    if not (rowsA or rowsB):
        return None

    # Счёт попытаемся достать локально (может не найтись) — потом дополним ESPN
    scoreA = scoreB = 0
    rx = re.compile(r"\b(\d{2,3})\s*:\s*(\d{2,3})\b")
    near = ancB.find_next(string=True)
    tries = 0
    while near and tries < 40 and (scoreA == 0 and scoreB == 0):
        m = rx.search(str(near))
        if m:
            scoreA, scoreB = int(m.group(1)), int(m.group(2)); break
        near = near.next_element
        tries += 1

    log(f"[DBG] OK {teamA}-{teamB} SCORE {scoreA}:{scoreB} A_rows {len(rowsA)} B_rows {len(rowsB)}")
    return {
        "teamA": {"name": teamA, "abbr": a_abbr, "score": scoreA},
        "teamB": {"name": teamB, "abbr": b_abbr, "score": scoreB},
        "players": {teamA: rowsA, teamB: rowsB},
        "records": {},  # дополним из ESPN
        "url": url,
    }

# -------- ESPN (scores + records) --------
ESPN_SB = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={ymd}"

def _espn_record(c: dict) -> str:
    for r in c.get("records") or []:
        if r.get("type") == "total" and r.get("summary"):
            return r["summary"]
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
            abbr_h = (th.get("abbreviation") or "").upper()
            abbr_a = (ta.get("abbreviation") or "").upper()
            if abbr_h == "GS": abbr_h = "GSW"
            if abbr_a == "GS": abbr_a = "GSW"

            status = (ev.get("status") or {}).get("type") or {}
            if not bool(status.get("completed", False)):
                continue  # берем только финалы

            out.append({
                "eventId": str(ev.get("id") or ""),
                "home": {
                    "abbr": abbr_h, "teamId": str(th.get("id") or ""),
                    "score": _intish(home.get("score", 0)),
                    "winner": bool(home.get("winner", False)),
                    "record": _espn_record(home),
                },
                "away": {
                    "abbr": abbr_a, "teamId": str(ta.get("id") or ""),
                    "score": _intish(away.get("score", 0)),
                    "winner": bool(away.get("winner", False)),
                    "record": _espn_record(away),
                },
            })
        except Exception:
            continue
    return out

def fetch_espn_events_multi(days: list[date]) -> dict[frozenset, dict]:
    seen={}
    for d in days:
        for e in fetch_espn_events_for_day(d):
            key = frozenset([e["home"]["abbr"], e["away"]["abbr"]])
            if key in seen: continue
            seen[key] = e
    return seen

# -------- Игроки/формат --------
def initials_ru(full: str) -> str:
    parts = [p for p in re.split(r"\s+", (full or "").strip()) if p]
    if not parts: return full or ""
    if len(parts) == 1: return parts[0]
    first = parts[0]; last = parts[-1]
    if last.lower() in {"jr.","jr","мл.","ст.","sr.","sr"} and len(parts)>=3:
        last = parts[-2] + " " + last
    return f"{first[0]}. {last}"

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
    stats=[("pts",p["pts"]),("reb",p["reb"]),("ast",p["ast"]),("stl",p["stl"]),("blk",p["blk"])]
    stats=[(k,v) for k,v in stats if v>0]
    stats.sort(key=lambda kv: kv[1], reverse=True)
    chosen=stats[:3]
    return f"{name}: " + ", ".join(ru_forms(k,v) for k,v in chosen) + hot_mark(p)

# -------- Спойлер --------
def sp(s: str) -> str: return f'<span class="tg-spoiler">{s}</span>'
SEP = "–––––––––––––––––––––––"

# -------- Блоки --------
def format_score_line(name_ru: str, abbr: str, score: int, winner: bool, record: str) -> str:
    score_txt = f"<b>{score}</b>" if winner else f"{score}"
    if record:
        score_txt += f" ({record})"
    return f"{emoji_token(abbr)} {name_ru}: {sp(score_txt)}"

def build_block(info: dict) -> str:
    A,B = info["teamA"], info["teamB"]
    recA = info.get("records", {}).get(A["abbr"], "")
    recB = info.get("records", {}).get(B["abbr"], "")

    a_win = A["score"] > B["score"]; b_win = B["score"] > A["score"]
    head = (
        f"{format_score_line(A['name'], A['abbr'], A['score'], a_win, recA)}\n"
        f"{format_score_line(B['name'], B['abbr'], B['score'], b_win, recB)}\n\n"
    )
    rowsA = info["players"].get(A["name"], []); rowsB = info["players"].get(B["name"], [])
    al = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p,bold,det) in pick_team_players(A["abbr"], rowsA)]
    bl = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p,bold,det) in pick_team_players(B["abbr"], rowsB)]
    lines=[]
    if al: lines.extend(al)
    if al and bl: lines.append("")
    if bl: lines.extend(bl)
    return head + ("\n".join(lines) if lines else "")

# -------- Сбор матчей дня --------
def fetch_sports_games_for_day(d: date) -> list[dict]:
    out=[]
    for url in collect_day_links(d):
        info = parse_sports_match(url)
        if info:
            out.append(info)
    return out

def enrich_scores_and_records_from_espn(games: list[dict]):
    if not games: return
    espn_by_pair = fetch_espn_events_multi(candidate_days_for_espn())
    for info in games:
        A,B = info["teamA"], info["teamB"]
        key = frozenset([A["abbr"], B["abbr"]])
        ev = espn_by_pair.get(key)
        if not ev:
            continue
        # записи
        rec_map = {
            ev["home"]["abbr"]: ev["home"].get("record",""),
            ev["away"]["abbr"]: ev["away"].get("record",""),
        }
        info["records"] = rec_map
        # если счёт 0:0 — подменим на ESPN
        if (A["score"] == 0 and B["score"] == 0):
            if ev["home"]["abbr"] == A["abbr"]:
                A["score"] = ev["home"]["score"]; B["score"] = ev["away"]["score"]
            else:
                A["score"] = ev["away"]["score"]; B["score"] = ev["home"]["score"]

def build_post() -> str:
    d_title = pick_report_date_london()
    games = fetch_sports_games_for_day(d_title)
    # дополним счёт/рекорды из ESPN
    enrich_scores_and_records_from_espn(games)

    title_count = len(games)
    title = f"НБА • {ru_date(d_title)} • {title_count} {ru_plural(title_count, ('матч','матча','матчей'))}\n"
    title += "Результаты надёжно спрятаны 👇\n"
    title += SEP + "\n\n"

    if title_count == 0:
        return title.rstrip()

    blocks=[]
    for i, g in enumerate(games, 1):
        blocks.append(build_block(g))
        if i < title_count:
            blocks.append("\n" + SEP + "\n\n")

    return (title + "".join(blocks)).strip()

# -------- Telegram (custom emoji entities) --------
def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")

    entities=[]
    out_parts=[]
    last=0
    for m in re.finditer(r"\{EMO:([A-Z]{2,3})\}", text):
        abbr = m.group(1)
        out_parts.append(text[last:m.start()])
        start_offset = sum(len(p) for p in out_parts)
        if abbr in CUSTOM_EMOJI:
            out_parts.append("⬤")  # плейсхолдер 1 символ
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

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": final_text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if entities:
        payload["entities"] = entities

    r = S.post(url, json=payload, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text}")

# -------- MAIN --------
if __name__ == "__main__":
    try:
        text = build_post()
        tg_send(text)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
