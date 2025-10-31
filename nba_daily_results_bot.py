#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU)

• Дата отчёта (ET): если в Нью-Йорке сейчас < 08:00 — берём вчера, иначе сегодня.
• Матчи и игроки: Sports.ru (находим ВСЕ ссылки за день, парсим страницы матчей).
• Лига: только НБА — фильтр по странице матча (ищем «НБА»).
• Рекорды W-L и контроль количества пар: ESPN (scoreboard) — ТОЛЬКО пары и W-L.
• Эмодзи: стабильные юникод-эмодзи для каждой команды.
• Вывод:
    - Названия+эмодзи видны.
    - Счёт (жирным у победителя, W-L в скобках; у проигравшего пометка ОТ) и блок игроков — под спойлером.
    - Между счётом и игроками — пустая строка; между командами в блоке игроков — тоже пустая строка.
• Игроки:
    - Для каждой команды 1–2 строки.
    - Всегда берём лучшего по очкам.
    - Второго добавляем, если: ≥20 очков ИЛИ дабл-дабл ИЛИ ≥6 STL/BLK.
    - Егор Дёмин (BKN) и Влад Голдин (MIA) включаются при наличии; имя жирным, 3 лучших показателя (>0).

Зависимости: requests, beautifulsoup4
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

# ------------ ENV / DEBUG ------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()
HTTP_TIMEOUT = 12
DEBUG = os.getenv("DEBUG_NBA", "").strip().lower() in {"1","true","yes","on"}

def dbg(*a):
    if DEBUG:
        print("[DBG]", *a, file=sys.stderr)

# ------------ HTTP ------------
def _mk_adapter():
    if Retry is not None:
        r = Retry(total=3, connect=3, read=3, backoff_factor=0.5,
                  status_forcelist=[429,500,502,503,504],
                  allowed_methods=["GET","POST"])
        return HTTPAdapter(max_retries=r)
    return HTTPAdapter(max_retries=2)

def make_session():
    s = requests.Session()
    ad = _mk_adapter()
    s.mount("https://", ad); s.mount("http://", ad)
    s.headers.update({
        "User-Agent": "NBA-DailyResultsBot/5.1 (sportsru primary, espn W-L, unicode emoji, spoilers)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
        "Connection": "close",
        "Referer": "https://www.sports.ru/",
    })
    return s

S = make_session()

def _get_json(url: str) -> dict:
    try:
        r = S.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200: return {}
        return r.json()
    except Exception:
        return {}

# ------------ DATES / RU FORMS ------------
RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def report_day_et() -> date:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return now_et.date() if now_et.hour >= 8 else (now_et.date() - timedelta(days=1))

# ------------ TEAMS / EMOJI ------------
TEAM_RU_TO_ABBR = {
    "Атланта":"ATL","Бостон":"BOS","Бруклин":"BKN","Шарлотт":"CHA","Чикаго":"CHI",
    "Кливленд":"CLE","Даллас":"DAL","Денвер":"DEN","Детройт":"DET","Голден Стэйт":"GSW",
    "Хьюстон":"HOU","Индиана":"IND","Клипперс":"LAC","Лейкерс":"LAL","Мемфис":"MEM",
    "Майами":"MIA","Милуоки":"MIL","Миннесота":"MIN","Новый Орлеан":"NOP","Нью-Йорк":"NYK",
    "Оклахома-Сити":"OKC","Орландо":"ORL","Филадельфия":"PHI","Финикс":"PHX","Портленд":"POR",
    "Сакраменто":"SAC","Сан-Антонио":"SAS","Торонто":"TOR","Юта":"UTA","Вашингтон":"WAS",
}
ABBR_TO_RU = {v:k for k,v in TEAM_RU_TO_ABBR.items()}
ABBR_FIX = {"WSH":"WAS","SA":"SAS","NO":"NOP","NY":"NYK","PHO":"PHX","UTAH":"UTA","GS":"GSW"}

TEAM_EMOJI = {
    "ATL":"🦅","BOS":"☘️","BKN":"🕸️","CHA":"🐝","CHI":"🐂","CLE":"🛡️","DAL":"🐎","DEN":"⛏️","DET":"🔧",
    "GSW":"🗡️","HOU":"🚀","IND":"💫","LAC":"✂️","LAL":"⭐","MEM":"🐻","MIA":"🔥","MIL":"🦌","MIN":"🐺",
    "NOP":"🪶","NYK":"🗽","OKC":"⚡","ORL":"✨","PHI":"🔔","PHX":"☀️","POR":"🧭","SAC":"👑","SAS":"🪙",
    "TOR":"🦖","UTA":"🎷","WAS":"🧙",
}

def normalize_abbr(abbr: str) -> str:
    a = (abbr or "").upper().strip()
    return ABBR_FIX.get(a, a)

def emoji_token(abbr: str) -> str:
    a = normalize_abbr(abbr)
    em = TEAM_EMOJI.get(a, "🏀")
    dbg("EMOJI PICK", a, "->", em)
    return em

# ------------ SPORTS.RU SCRAPER ------------
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

def _is_nba_page(soup: BeautifulSoup) -> bool:
    txt = soup.get_text(" ", strip=True)
    if re.search(r"\bНБА\b", txt, flags=re.I): return True
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if "/nba" in href: return True
    return False

def _canonical_ru_team(raw: str) -> str | None:
    if not raw: return None
    t = raw.replace("«","").replace("»","").strip()
    t = re.sub(r"\(.*?\)", "", t).strip()
    for k in TEAM_RU_TO_ABBR:
        if t.startswith(k) or k in t: return k
    return None

def parse_sports_match(url: str) -> dict | None:
    dbg("PARSE START", url)
    soup = _soup(url)
    if not soup or not _is_nba_page(soup):
        dbg("PARSE SKIP (no soup or not NBA)")
        return None

    text = soup.get_text(" ", strip=True)
    m = re.search(r"(\d+)\s*:\s*(\d+)", text)
    if not m:
        dbg("PARSE NO SCORE")
        return None
    scoreA, scoreB = int(m.group(1)), int(m.group(2))
    tail = text[m.end():m.end()+240]
    extra_pairs = re.findall(r"\d+\s*:\s*\d+", tail)
    ot = max(len(extra_pairs) - 4, 0) if extra_pairs else 0

    meta = soup.find("meta", attrs={"property": "og:title"})
    title = meta.get("content") if meta and meta.get("content") else (soup.title.string if soup.title else "")
    teamA = teamB = None
    if title and "—" in title:
        left, right = [x.strip() for x in title.split("—", 1)]
        teamA = _canonical_ru_team(left)
        teamB = _canonical_ru_team(right)

    if not (teamA and teamB) or teamA == teamB:
        heads = []
        for h in soup.find_all(["h2","h3","h4"]):
            t = h.get_text(" ", strip=True)
            if "статистика игроков" in t.lower():
                k = _canonical_ru_team(t.split(".")[0])
                if k: heads.append(k)
        if len(heads) >= 2:
            teamA = teamA or heads[0]
            teamB = teamB or next((x for x in heads[1:] if x != teamA), None)

    if not (teamA and teamB) or teamA == teamB:
        dbg("PARSE NO TEAMS")
        return None

    a_abbr = TEAM_RU_TO_ABBR.get(teamA, "")
    b_abbr = TEAM_RU_TO_ABBR.get(teamB, "")
    if not a_abbr or not b_abbr:
        dbg("PARSE NO ABBR", teamA, teamB)
        return None

    def read_rows_for(team_ru_key: str) -> list[dict]:
        anchor = None
        want = team_ru_key.lower()
        for h in soup.find_all(["h2","h3","h4"]):
            t = h.get_text(" ", strip=True).lower()
            if "статистика игроков" in t and want in t:
                anchor = h; break
        if not anchor:
            return []
        table = anchor.find_next("table")
        if not table: return []
        header_tr = None
        for tr in table.find_all("tr"):
            if tr.find("th"):
                header_tr = tr; break
        if not header_tr: return []

        header_cells = [th.get_text(" ", strip=True) for th in header_tr.find_all("th")]
        def find_col(keys: list[str]) -> int | None:
            for i, htxt in enumerate(header_cells):
                ht = htxt.replace(" ", "").upper()
                for k in keys:
                    if k in ht: return i
            return None

        idx_pts = find_col(["О","ОЧК","PTS"])
        idx_reb = find_col(["ПБ","ПОДБ","REB"])
        idx_ast = find_col(["АП","ПЕРЕД","AST"])
        idx_stl = find_col(["ПХ","ПЕРЕХ","STL"])
        idx_blk = find_col(["БШ","БЛОК","BLK"])

        fallback = any(v is None for v in (idx_pts,idx_reb,idx_ast,idx_stl,idx_blk))

        rows=[]; body=[]; take=False
        for tr in table.find_all("tr"):
            if tr is header_tr: take=True; continue
            if take: body.append(tr)

        for tr in body:
            a = tr.find("a", href=True)
            if not a: continue
            name = a.get_text(" ", strip=True)
            if not name: continue
            tds = tr.find_all("td")
            if not tds: continue

            def as_int(x: str) -> int:
                try: return int(x)
                except:
                    try: return int(float(x))
                    except: return 0

            if fallback:
                name_td = a.find_parent("td"); pos=0
                for i, td in enumerate(tds):
                    if td is name_td: pos=i; break
                nums = [td.get_text(" ", strip=True) for td in tds[pos+1:]]
                if len(nums) < 13: continue
                pts = as_int(nums[0]); reb = as_int(nums[7]); ast = as_int(nums[8])
                stl = as_int(nums[10]); blk = as_int(nums[12])
            else:
                cells = [td.get_text(" ", strip=True) for td in tds]
                def cell(i): return cells[i] if i is not None and i < len(cells) else "0"
                pts = as_int(cell(idx_pts)); reb = as_int(cell(idx_reb))
                ast = as_int(cell(idx_ast)); stl = as_int(cell(idx_stl)); blk = as_int(cell(idx_blk))

            if not any([pts,reb,ast,stl,blk]): continue
            rows.append({"name": name, "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk})
        return rows

    rowsA = read_rows_for(teamA)
    rowsB = read_rows_for(teamB)
    dbg("PARSE TEAMS", teamA, teamB, "OT", ot, "SCORES", scoreA, scoreB)
    dbg("PARSE ROWS", teamA, len(rowsA), teamB, len(rowsB))
    finished = bool(rowsA or rowsB)

    return {
        "teamA": {"name": teamA, "abbr": a_abbr, "score": scoreA},
        "teamB": {"name": teamB, "abbr": b_abbr, "score": scoreB},
        "ot": ot,
        "finished": finished,
        "players": {teamA: rowsA, teamB: rowsB},
        "url": _normalize_match_url(url),
    }

def collect_day_sports_matches(d: date) -> dict[frozenset, dict]:
    soup = _soup(day_url(d))
    dbg("SPORTS DAY URL", day_url(d), "OK" if soup else "ERR")
    if not soup: return {}
    links=set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/basketball/match/" in href:
            links.add(_normalize_match_url(href))
    dbg("SPORTS LINKS", len(links))
    out={}
    for u in sorted(links):
        dbg("LINK", u)
        try:
            info = parse_sports_match(u)
            if not info or not info["finished"]:
                continue
            pair = frozenset([normalize_abbr(info["teamA"]["abbr"]), normalize_abbr(info["teamB"]["abbr"])])
            if pair not in out:
                out[pair] = info
        except Exception as e:
            dbg("LINK ERR", u, repr(e))
            continue
    dbg("SPORTS MATCHES OK", len(out))
    return out

# ------------ ESPN: пары и W-L ------------
ESPN_SB = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={ymd}"

def _espn_record(c: dict) -> str:
    for r in c.get("records") or []:
        if r.get("type") == "total" and r.get("summary"): return r["summary"]
    return ""

def espn_pairs_and_records(d: date) -> dict[frozenset, dict]:
    j = _get_json(ESPN_SB.format(ymd=d.strftime("%Y%m%d")))
    out={}
    for ev in (j.get("events") or []):
        try:
            status = (ev.get("status") or {}).get("type") or {}
            if not bool(status.get("completed", False)):
                continue
            comp = (ev.get("competitions") or [None])[0] or {}
            comps = comp.get("competitors") or []
            if len(comps) != 2: continue
            home = next(c for c in comps if c.get("homeAway")=="home")
            away = next(c for c in comps if c.get("homeAway")=="away")
            th=(home.get("team") or {}); ta=(away.get("team") or {})
            abbr_h = normalize_abbr(th.get("abbreviation") or "")
            abbr_a = normalize_abbr(ta.get("abbreviation") or "")
            def as_int(x):
                try: return int(float(x))
                except: return 0
            period = int((status.get("period") or 0) or 0)
            ot = max(period-4, 0) if period>4 else 0
            e = {
                "eventId": str(ev.get("id") or ""),
                "home": {
                    "abbr": abbr_h, "teamId": str(th.get("id") or ""),
                    "score": as_int(home.get("score", 0)),
                    "winner": bool(home.get("winner", False)),
                    "record": _espn_record(home),
                },
                "away": {
                    "abbr": abbr_a, "teamId": str(ta.get("id") or ""),
                    "score": as_int(away.get("score", 0)),
                    "winner": bool(away.get("winner", False)),
                    "record": _espn_record(away),
                },
                "ot": ot,
            }
            key = frozenset([abbr_h, abbr_a])
            if key not in out: out[key] = e
        except Exception:
            continue
    dbg("ESPN COMPLETED", len(out))
    return out

# ------------ PLAYERS / FORMAT ------------
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

def score_key(p: dict): return (p["pts"], p["reb"]+p["ast"], p["stl"]+p["blk"])

def pick_team_players(abbr: str, rows: list[dict]) -> list[tuple[dict,bool,bool]]:
    if not rows: return []
    rows = sorted(rows, key=score_key, reverse=True)
    ab = normalize_abbr(abbr)
    special_keys = []
    if ab=="BKN": special_keys = ["дёмин","demin"]
    if ab=="MIA": special_keys = ["голдин","goldin"]
    special=None
    for p in rows:
        nm = (p["name"] or "").lower()
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

def sp(s: str) -> str: return f'<span class="tg-spoiler">{s}</span>'

# ------------ RENDER ------------
def format_score_line(name_ru: str, abbr: str, score: int, winner: bool, record: str, ot_str: str) -> str:
    score_txt = f"<b>{score}</b>" if winner else f"{score}"
    if ot_str and not winner: score_txt += ot_str
    if record: score_txt += f" ({record})"
    return f"{emoji_token(abbr)} {name_ru}: {sp(score_txt)}"

def build_block_from_sports(info: dict, records: dict[str,str]) -> str:
    A,B = info["teamA"], info["teamB"]
    ot_str = "" if info["ot"]==0 else (" (ОТ)" if info["ot"]==1 else f" ({info['ot']} ОТ)")
    a_win = A["score"] > B["score"]; b_win = B["score"] > A["score"]

    line1 = format_score_line(A['name'], A['abbr'], A['score'], a_win, records.get(normalize_abbr(A['abbr']), ""), "")
    line2 = format_score_line(B['name'], B['abbr'], B['score'], b_win, records.get(normalize_abbr(B['abbr']), ""), ot_str)
    head = f"{line1}\n{line2}\n\n"

    rowsA = info["players"].get(A["name"], [])
    rowsB = info["players"].get(B["name"], [])
    al = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p,bold,det) in pick_team_players(A["abbr"], rowsA)]
    bl = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p,bold,det) in pick_team_players(B["abbr"], rowsB)]

    lines=[]
    if al: lines.extend(al)
    if al and bl: lines.append("")  # пустая строка между командами
    if bl: lines.extend(bl)
    return head + ("\n".join(lines) if lines else "")

# ------------ BUILD POST ------------
ESPN_SB = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={ymd}"

def build_post() -> str:
    d_et = report_day_et()
    dbg("DAY", d_et.isoformat())

    sports_by_pair = collect_day_sports_matches(d_et)     # пары + игроки (только НБА)
    espn_by_pair   = espn_pairs_and_records(d_et)         # пары + W-L

    # порядок как у ESPN, чтобы не терять матчи
    ordered_pairs = list(espn_by_pair.keys()) if espn_by_pair else list(sports_by_pair.keys())
    title_count = len(ordered_pairs)

    title = f"НБА • {ru_date(d_et)} • {title_count} {ru_plural(title_count, ('матч','матча','матчей'))}\n"
    title += "Результаты надёжно спрятаны 👇\n"
    title += "–––––––––––––––––––––––\n\n"

    if title_count == 0:
        return title.rstrip()

    blocks=[]
    for i, pair in enumerate(ordered_pairs, 1):
        s_info = sports_by_pair.get(pair)
        if s_info:
            rec_map = {}
            e = espn_by_pair.get(pair)
            if e:
                rec_map[normalize_abbr(e["home"]["abbr"])] = e["home"].get("record","")
                rec_map[normalize_abbr(e["away"]["abbr"])] = e["away"].get("record","")
            blocks.append(build_block_from_sports(s_info, rec_map))
        else:
            # если на Sports.ru почему-то нет матча — пропускаем, т.к. игроки нужны только оттуда
            continue
        if i < title_count:
            blocks.append("\n" + "–––––––––––––––––––––––" + "\n\n")

    return (title + "".join(blocks)).strip()

# ------------ TELEGRAM ------------
def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = S.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text}")

# ------------ MAIN ------------
if __name__ == "__main__":
    try:
        text = build_post()
        tg_send(text)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
