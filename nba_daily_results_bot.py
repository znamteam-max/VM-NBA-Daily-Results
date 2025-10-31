#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU) — Sports.ru only

• Берём пары, счёт и БОКССКОРЫ только со страниц матчей Sports.ru.
• Таблицы: строго «Статистика игроков» (проверяем наличие колонки «Игрок»).
• Очки команды: из строки «Итого» или сумма очков игроков.
• Формат: счёт и игроки в спойлерах; у победителя счёт жирным.
• Игроки: от 1 до 2 на команду. Второй — если ≥20 очков ИЛИ дабл-дабл ИЛИ ≥6 STL/BLK.
• Спец: Дёмин (BKN) / Голдин (MIA) — жирным, 3 наибольших показателя >0.
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

# ========= ENV / CONFIG =========
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TEAM_EMOJI_JSON = os.getenv("TEAM_EMOJI_JSON", "").strip()  # JSON: {"BOS":"☘️", ...}
DEBUG = os.getenv("DEBUG_NBA", "0") == "1"

HTTP_TIMEOUT = 9

def log(*a):
    if DEBUG:
        print(*a, file=sys.stderr)

def _mk_adapter():
    if Retry:
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
        "User-Agent": "NBA-DailyResultsBot/4.4 (sports.ru only, players-table strict)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
        "Connection": "close",
    })
    return s

S = make_session()

# ========= RU dates/plurals =========
RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def report_day_london() -> date:
    now = datetime.now(ZoneInfo("Europe/London"))
    return now.date() if now.hour >= 11 else (now.date() - timedelta(days=1))

# ========= Teams / Emojis =========
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
def load_team_emojis():
    if TEAM_EMOJI_JSON:
        try:
            d = json.loads(TEAM_EMOJI_JSON)
            if isinstance(d, dict):
                return {k.upper(): str(v) for k,v in d.items()}
        except Exception:
            pass
    return TEAM_EMOJI_DEFAULT
TEAM_EMOJI = load_team_emojis()
def emoji(abbr: str) -> str: return TEAM_EMOJI.get((abbr or "").upper(), "🏀")

# ========= Sports.ru helpers =========
def day_url(d: date) -> str:
    return f"https://www.sports.ru/stat/basketball/center/end/{d:%Y/%m/%d}.html"

def _normalize_url(u: str) -> str:
    full = "https://www.sports.ru" + u if u.startswith("/") else u
    p = urlparse(full); return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

def _soup(url: str):
    try:
        r = S.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200: return None
        return BeautifulSoup(r.text, "html.parser")
    except Exception:
        return None

def collect_day_match_urls(d: date) -> list[str]:
    soup = _soup(day_url(d))
    if not soup: return []
    out, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/basketball/match/" not in href:
            continue
        u = _normalize_url(href)
        if u in seen:
            continue
        # Берём ТОЛЬКО матчевые страницы формата team-vs-team, отсекаем архивы по датам и прочее
        if not re.search(r"/basketball/match/[^/]+-vs-[^/]+/?$", u):
            continue
        seen.add(u)
        out.append(u)
    log("[DBG] SPORTS LINKS", len(out))
    return out

def _canonical_ru_team(raw: str) -> str | None:
    if not raw: return None
    t = raw.replace("«","").replace("»","").strip()
    t = re.sub(r"\(.*?\)", "", t).strip()
    for k in TEAM_RU_TO_ABBR:
        if t.startswith(k) or k in t:
            return k
    return None

def _is_players_table(table) -> bool:
    """Ровно таблица 'Статистика игроков': в хедере должна быть колонка с 'Игрок'
       и колонка очков (любой из: 'О', 'Очки', 'PTS', 'Оч')."""
    if table is None:
        return False
    # найдём первую строку-хедер (часто это <thead><tr>, но бывает и просто первый <tr>)
    hdr_tr = None
    thead = table.find("thead")
    if thead:
        hdr_tr = thead.find("tr")
    if not hdr_tr:
        hdr_tr = table.find("tr")
    if not hdr_tr:
        return False

    hdr_cells = [c.get_text(" ", strip=True) for c in hdr_tr.find_all(["th","td"])]
    hl = [h.lower() for h in hdr_cells]

    has_player_col = any("игрок" in h for h in hl)
    has_points_col = any(
        (h in {"о", "очки", "pts"}) or ("оч" in h) or ("pts" in h)
        for h in hl
    )
    return bool(has_player_col and has_points_col)

# ========= Parse a match page (players-table strict) =========
def parse_match(url: str) -> dict | None:
    soup = _soup(url)
    if not soup:
        return None

    # Попробуем вытащить пары команд из og:title (например: "Шарлотт — Орландо")
    teamA_from_title = teamB_from_title = None
    meta = soup.find("meta", attrs={"property":"og:title"})
    title = meta.get("content") if meta and meta.get("content") else (soup.title.string if soup.title else "")
    if title and "—" in title:
        left, right = [x.strip() for x in title.split("—", 1)]
        teamA_from_title = _canonical_ru_team(left)
        teamB_from_title = _canonical_ru_team(right)

    # Соберём все таблицы, которые выглядят как "Статистика игроков"
    tables = [t for t in soup.find_all("table") if _is_players_table(t)]
    if len(tables) < 2:
        log("[DBG] PLAYERS-TABLES < 2, skip", url)
        return None

    # Для каждой таблицы попробуем найти ближайший предыдущий заголовок (h2/h3/h4) с названием команды
    def nearest_team_for_table(table) -> str | None:
        h = table.find_previous(["h2","h3","h4"])
        steps = 0
        while h and steps < 6:  # далеко не уходим
            txt = h.get_text(" ", strip=True)
            cand = _canonical_ru_team(txt.split(".")[0])
            if cand:
                return cand
            h = h.find_previous(["h2","h3","h4"])
            steps += 1
        return None

    tables_with_teams = []
    for t in tables:
        team_guess = nearest_team_for_table(t)
        tables_with_teams.append((team_guess, t))

    # Если у таблиц не нашлись явные команды — подстрахуемся og:title
    # Возьмём первые две таблицы и соотнесём их по порядку с og:title
    if not any(x[0] for x in tables_with_teams) and (teamA_from_title and teamB_from_title):
        tA, tB = tables[:2]
        tables_with_teams = [(teamA_from_title, tA), (teamB_from_title, tB)]

    # Теперь должны иметь минимум две таблицы с разными командами
    pairs = []
    seen_teams = set()
    for guess, t in tables_with_teams:
        if not guess:
            continue
        if guess in seen_teams:
            continue
        pairs.append((guess, t))
        seen_teams.add(guess)
        if len(pairs) == 2:
            break

    # Если всё ещё нет двух разных команд — попробуем забрать по порядку и подставить из og:title
    if len(pairs) < 2 and (teamA_from_title and teamB_from_title):
        need = 2 - len(pairs)
        for t in tables:
            if all(t is not p[1] for p in pairs):
                pairs.append((None, t))
                if len(pairs) == 2:
                    break
        # заполним названия
        names_fill = [teamA_from_title, teamB_from_title]
        for i in range(len(pairs)):
            if pairs[i][0] is None:
                pairs[i] = (names_fill[i], pairs[i][1])

    if len(pairs) < 2:
        log("[DBG] STILL <2 teams for", url)
        return None

    # Парсим таблицу игроков по заголовкам колонок
    def to_int(x: str) -> int:
        s = (x or "").strip()
        if s in {"—","-",""}:
            return 0
        m = re.match(r"^-?\d+", s)
        return int(m.group(0)) if m else 0

    def parse_players_and_pts(table) -> tuple[list[dict], int]:
        # найдём хедер
        thead = table.find("thead")
        hdr_tr = thead.find("tr") if thead else table.find("tr")
        hdr_cells = [c.get_text(" ", strip=True) for c in hdr_tr.find_all(["th","td"])]
        hl = [h.lower() for h in hdr_cells]

        # индексы
        name_idx = None
        pts_idx = reb_idx = ast_idx = stl_idx = blk_idx = None
        for i, h in enumerate(hl):
            if "игрок" in h: name_idx = i
            if (h in {"о","очки","pts"}) or ("оч" in h) or ("pts" in h): pts_idx = i
            if (h in {"пб","подборы","reb"}) or ("подбор" in h) or ("reb" in h): reb_idx = i
            if (h in {"ап","передачи","ast"}) or ("передач" in h) or ("ast" in h): ast_idx = i
            if (h in {"пх","перехваты","stl"}) or ("перехват" in h) or ("stl" in h): stl_idx = i
            if (h in {"бш","блок-шоты","blk","блокшоты"}) or ("блок" in h) or ("blk" in h): blk_idx = i

        if name_idx is None or pts_idx is None:
            return [], 0

        players = []
        team_pts = None
        # все строки после хедера
        rows = []
        if thead:
            tbody = table.find("tbody")
            if tbody:
                rows = tbody.find_all("tr")
        if not rows:
            rows = table.find_all("tr")[1:]

        for tr in rows:
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td","th"])]
            if not cells:
                continue
            name_cell = (cells[name_idx] if len(cells) > name_idx else "").strip()

            # «Итого»
            if name_cell.lower() in {"итого","итог","total"}:
                if len(cells) > pts_idx:
                    team_pts = to_int(cells[pts_idx])
                continue

            if not re.search(r"[A-Za-zА-Яа-яЁё]", name_cell):
                continue

            def g(idx):
                return to_int(cells[idx]) if (idx is not None and idx < len(cells)) else 0

            p = {
                "name": name_cell,
                "pts": g(pts_idx),
                "reb": g(reb_idx),
                "ast": g(ast_idx),
                "stl": g(stl_idx),
                "blk": g(blk_idx),
            }
            if any(p[k] for k in ("pts","reb","ast","stl","blk")):
                players.append(p)

        if team_pts is None:
            team_pts = sum(p["pts"] for p in players)
        return players, team_pts

    # Разобрали две команды
    (A_ru, A_tbl), (B_ru, B_tbl) = pairs[0], pairs[1]
    if not (A_ru and B_ru):
        log("[DBG] team names missing", url)
        return None

    a_abbr = TEAM_RU_TO_ABBR.get(A_ru, "")
    b_abbr = TEAM_RU_TO_ABBR.get(B_ru, "")
    if not (a_abbr and b_abbr):
        return None

    A_rows, A_pts = parse_players_and_pts(A_tbl)
    B_rows, B_pts = parse_players_and_pts(B_tbl)

    info = {
        "teamA": {"name": A_ru, "abbr": a_abbr, "emoji": emoji(a_abbr), "score": A_pts},
        "teamB": {"name": B_ru, "abbr": b_abbr, "emoji": emoji(b_abbr), "score": B_pts},
        "players": {A_ru: A_rows, B_ru: B_rows},
        "finished": True,
        "url": url,
    }
    log(f"[DBG] PARSED {A_ru}-{B_ru} SCORE {A_pts}-{B_pts} A_rows {len(A_rows)} B_rows {len(B_rows)}")
    return info

# ========= Players pick/format =========
def initials_ru(full: str) -> str:
    parts = [p for p in re.split(r"\s+", (full or "").strip()) if p]
    if not parts: return ""
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

    # спец-игроки
    special_keys = []
    if abbr=="BKN": special_keys = ["дёмин","demin"]
    if abbr=="MIA": special_keys = ["голдин","goldin"]
    special=None
    for p in rows:
        nm = (p["name"] or "").lower()
        if any(k in nm for k in special_keys):
            special = p; break

    out=[]
    top = rows[0]
    if special and special["name"] == top["name"]:
        out.append((special, True, True))
    elif special:
        out.append((top, False, False)); out.append((special, True, True))
    else:
        out.append((top, False, False))

    if len(out) < 2:
        for p in rows[1:]:
            if p["name"] == top["name"]: continue
            if second_ok(p):
                out.append((p, False, False)); break
    return out[:2]

def format_player_regular(p: dict, bold=False) -> str:
    name = initials_ru(p["name"])
    if bold: name = f"<b>{name}</b>"
    parts = [ru_forms("pts", p["pts"])]
    if p["reb"]>=5: parts.append(ru_forms("reb", p["reb"]))
    if p["ast"]>=5: parts.append(ru_forms("ast", p["ast"]))
    if p["stl"]>=4: parts.append(ru_forms("stl", p["stl"]))
    if p["blk"]>=4: parts.append(ru_forms("blk", p["blk"]))
    return f"{name}: " + ", ".join(parts) + hot_mark(p)

def format_player_special(p: dict) -> str:
    name = f"<b>{initials_ru(p['name'])}</b>"
    stats=[("pts",p["pts"]),("reb",p["reb"]),("ast",p["ast"]),("stl",p["stl"]),("blk",p["blk"])]
    stats=[(k,v) for k,v in stats if v>0]
    stats.sort(key=lambda kv: kv[1], reverse=True)
    chosen=stats[:3]
    return f"{name}: " + ", ".join(ru_forms(k,v) for k,v in chosen) + hot_mark(p)

# ========= Spoiler / formatting =========
def sp(s: str) -> str: return f'<span class="tg-spoiler">{s}</span>'
SEP = "–––––––––––––––––––––––"

def format_score_line(name_ru: str, abbr: str, score: int, winner: bool) -> str:
    score_txt = f"<b>{score}</b>" if winner else f"{score}"
    return f"{emoji(abbr)} {name_ru}: {sp(score_txt)}"

def build_block(info: dict) -> str:
    A,B = info["teamA"], info["teamB"]
    a_win = A["score"] > B["score"]; b_win = B["score"] > A["score"]

    head = (
        f"{format_score_line(A['name'], A['abbr'], A['score'], a_win)}\n"
        f"{format_score_line(B['name'], B['abbr'], B['score'], b_win)}\n\n"
    )

    rowsA = info["players"].get(A["name"], []); rowsB = info["players"].get(B["name"], [])
    al = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p,bold,det) in pick_team_players(A["abbr"], rowsA)]
    bl = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p,bold,det) in pick_team_players(B["abbr"], rowsB)]

    lines=[]
    if al: lines.extend(al)
    if al and bl: lines.append("")  # пустая строка между командами
    if bl: lines.extend(bl)
    return head + ("\n".join(lines) if lines else "")

# ========= Build post =========
def build_post() -> str:
    d = report_day_london()
    urls = collect_day_match_urls(d)

    games=[]
    for u in urls:
        try:
            info = parse_match(u)
            if info and info.get("finished"):
                games.append(info)
        except Exception as e:
            log("[DBG] PARSE ERROR", u, repr(e))

    title = f"НБА • {ru_date(d)} • {len(games)} {ru_plural(len(games), ('матч','матча','матчей'))}\n"
    title += "Результаты надёжно спрятаны 👇\n"
    title += SEP + "\n\n"

    if not games:
        return title.rstrip()

    blocks=[]
    for i, g in enumerate(games, 1):
        blocks.append(build_block(g))
        if i < len(games):
            blocks.append("\n" + SEP + "\n\n")

    return (title + "".join(blocks)).strip()

# ========= Telegram =========
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

# ========= MAIN =========
if __name__ == "__main__":
    try:
        text = build_post()
        tg_send(text)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
