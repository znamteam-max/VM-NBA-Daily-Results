#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU) — Sports.ru only

• Матчи и игроки берутся со страницы дня:
  https://www.sports.ru/stat/basketball/center/end/YYYY/MM/DD.html
• Фильтруем только НБА (по слагам команд).
• Внутри матча парсим таблицы «статистика игроков» (устойчиво к <th>/<td> и разным заголовкам).
• Пост: один месседж, счёт и строки игроков спрятаны в спойлеры; победитель — жирным.
• Дёмин (BKN) и Голдин (MIA) включаются обязательно, для них показываем 3 крупнейших ненулевых показателя.

Переменные окружения:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID — обязательны.
  TEAM_EMOJI_JSON — JSON-словарь { "LAL": "🟡", "BOS": "☘️", ... } из вашего пака.
  DEBUG_NBA=1 — подробные логи.
  NBA_DATE_MODE=today|best  — today = всегда «сегодня по ET» (по умолчанию), best = «где больше игр».

"""

import os, sys, re, json
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ========= ENV & LOG =========
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()
DEBUG     = os.getenv("DEBUG_NBA", "").strip() not in {"", "0", "false", "False"}
DATE_MODE = ( os.getenv("NBA_DATE_MODE", "today").strip().lower() or "today" )  # "today" | "best"

def logdbg(*a):
    if DEBUG:
        print("[DBG]", *a, file=sys.stderr)

# ========= DATE HELPERS =========
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
              status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    # без юникода в UA, чтобы не поймать UnicodeEncodeError на некоторых раннерах
    s.headers.update({
        "User-Agent": "NBA-ResultsBot/5.2 (sportsru-only)",
        "Accept-Language": "ru,en;q=0.7",
    })
    return s
S = make_session()

# ========= CONSTANTS / MAPS =========
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

# кастом-эмодзи из вашего пака
def load_team_emojis() -> dict[str, str]:
    raw = os.getenv("TEAM_EMOJI_JSON", "").strip()
    if not raw:
        return {}
    try:
        mp = json.loads(raw)
        if isinstance(mp, dict):
            # убедимся, что все значения — строки-эмодзи
            return {k.upper(): (str(v) or "🏀") for k, v in mp.items()}
    except Exception:
        return {}
    return {}

TEAM_EMOJI = load_team_emojis()

def team_emoji(abbr: str) -> str:
    return TEAM_EMOJI.get(abbr.upper(), "🏀")

# ========= LINKS FOR DAY =========
def fetch_day_links(d: date) -> list[str]:
    url = DAY_URL_TMPL.format(y=d.year, m=d.month, d=d.day)
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        logdbg("SPORTS DAY URL FAIL", url, r.status_code)
        return []
    logdbg("SPORTS DAY URL", url, "OK")
    soup = BeautifulSoup(r.text, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        h = a["href"]
        if not (h.startswith("/basketball/match/") or h.startswith(MATCH_PREFIX)):
            continue
        if "/live/" in h:
            continue
        full = urljoin("https://www.sports.ru", h).split("#", 1)[0]
        if "vs" not in full:
            continue
        if not full.endswith("/"):
            full += "/"
        # проверим, что обе команды — НБА
        a_abbr = abbr_from_url(full, 0)
        b_abbr = abbr_from_url(full, 1)
        if not (a_abbr and b_abbr and a_abbr in TEAM_RU and b_abbr in TEAM_RU):
            continue
        links.append(full)
    # dedup
    out, seen = [], set()
    for u in links:
        if u not in seen:
            out.append(u); seen.add(u)
    logdbg("SPORTS LINKS", len(out))
    return out

def count_games_for_day(d: date) -> int:
    return len(fetch_day_links(d))

# ========= UTILS =========
def abbr_from_url(url: str, side: int) -> str | None:
    m = re.search(r"/basketball/match/([a-z0-9\-]+)-vs-([a-z0-9\-]+)/", url)
    if not m: return None
    slug = m.group(1 + side)
    return SLUG2ABBR.get(slug)

def _int_first(text: str) -> int:
    m = re.search(r"-?\d+", text or "")
    return int(m.group(0)) if m else 0

def norm_col(text: str) -> str:
    t = re.sub(r"\s+", "", (text or "").lower())
    t = (t.replace("очки", "оч").replace("очков", "оч")
            .replace("передачи", "перед").replace("перед", "аст")
            .replace("подборы", "подбор").replace("перехваты","перех")
            .replace("блок-шоты","блокшоты").replace("блокшот","блокшоты"))
    # часто встречаются чистые англ. аббревиатуры
    return t

def col_metric(t: str) -> str | None:
    if t in {"оч","pts"}: return "pts"
    if t in {"подбор","reb"}: return "reb"
    if t in {"аст","ast","пас"}: return "ast"
    if t in {"перех","stl"}: return "stl"
    if t in {"блокшоты","blk","бш","блок"}: return "blk"
    return None

# ========= TABLE PARSER =========
def header_to_index(tbl) -> dict[str,int]:
    # 1) thead → последняя строка
    thead = tbl.find("thead")
    if thead:
        rows = thead.find_all("tr")
        if rows:
            cells = rows[-1].find_all(["th","td"])
            headers = [c.get_text(" ", strip=True) for c in cells]
            idx_map = {}
            for i, h in enumerate(headers):
                m = col_metric(norm_col(h))
                if m and m not in idx_map:
                    idx_map[m] = i
            if idx_map:
                logdbg("HEAD (thead):", headers, "→", idx_map)
                return idx_map
    # 2) перебирать первые 3 строки tbody и искать «похожую на header»
    tbody = tbl.find("tbody") or tbl
    for tr in tbody.find_all("tr")[:3]:
        cells = tr.find_all(["th","td"])
        headers = [c.get_text(" ", strip=True) for c in cells]
        ok_letters = sum(1 for x in headers if re.search(r"[A-Za-zА-Яа-я]", x))
        ok_digits  = sum(1 for x in headers if re.search(r"\d", x))
        if ok_letters >= 4 and ok_digits <= 1:  # шапка обычно буквенная
            idx_map = {}
            for i, h in enumerate(headers):
                m = col_metric(norm_col(h))
                if m and m not in idx_map:
                    idx_map[m] = i
            if idx_map:
                logdbg("HEAD (tbody):", headers, "→", idx_map)
                return idx_map
    logdbg("HEAD not found")
    return {}

def parse_stats_table_near(h3) -> tuple[list[dict], int]:
    """Из <h3> …статистика игроков → ближайшая <table>; возвращает (players, team_total_pts)."""
    tbl = h3.find_next("table")
    if not tbl:
        return [], 0

    idx_map = header_to_index(tbl)
    tbody = tbl.find("tbody") or tbl

    players = []
    team_pts = 0

    for tr in tbody.find_all("tr"):
        cells = tr.find_all(["th","td"])
        if not cells:
            continue

        name_cell = cells[0]
        first_text = name_cell.get_text(" ", strip=True)

        # «Итого» / «Всего»
        if re.search(r"^(итого|всего)\s*$", first_text, flags=re.IGNORECASE):
            if "pts" in idx_map and idx_map["pts"] < len(cells):
                team_pts = _int_first(cells[idx_map["pts"]].get_text(" ", strip=True))
            continue

        # Игрок
        a = name_cell.find("a")
        name_ru = (a.get_text(" ", strip=True) if a else first_text).strip()
        if not name_ru:
            continue

        def val(metric):
            if metric not in idx_map or idx_map[metric] >= len(cells):
                return 0
            return _int_first(cells[idx_map[metric]].get_text(" ", strip=True))

        p = {
            "name_ru": name_ru,
            "pts": val("pts"),
            "reb": val("reb"),
            "ast": val("ast"),
            "stl": val("stl"),
            "blk": val("blk"),
        }
        if any(p[k] for k in ("pts","reb","ast","stl","blk")):
            players.append(p)

    return players, team_pts

def detect_ot(soup: BeautifulSoup) -> bool:
    for h3 in soup.find_all("h3"):
        txt = (h3.get_text(" ", strip=True) or "").lower()
        if "период" in txt:
            tbl = h3.find_next("table")
            if not tbl: continue
            row = tbl.find("tr")
            if not row: continue
            s = row.get_text(" ", strip=True)
            pairs = re.findall(r"\b\d{1,2}\s*:\s*\d{1,2}\b", s)
            if len(pairs) > 4:
                return True
    txt = soup.get_text(" ", strip=True)
    return bool(re.search(r"\bОТ\b|Овертайм", txt, flags=re.IGNORECASE))

def parse_sports_match(url: str) -> dict | None:
    a_abbr = abbr_from_url(url, 0); b_abbr = abbr_from_url(url, 1)
    if not (a_abbr and b_abbr and a_abbr in TEAM_RU and b_abbr in TEAM_RU):
        return None

    r = S.get(url, timeout=25)
    if r.status_code != 200:
        logdbg("PARSE START", url, "HTTP", r.status_code)
        return None
    soup = BeautifulSoup(r.text, "html.parser")

    # H3 блоки «статистика игроков»
    h3s = [h for h in soup.find_all("h3") if re.search(r"статистика игроков", h.get_text(" ", strip=True), re.I)]
    if len(h3s) < 2:
        logdbg("PARSE START", url, "NO H3 STATS")
        return None

    def team_from_h3(h3):
        t = h3.get_text(" ", strip=True)
        m = re.match(r"\s*(.+?)\.\s*статистика игроков\s*$", t, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip('«»" ')
        return (t.replace("статистика игроков", "").strip()).strip('«»" ')

    teamA_name = team_from_h3(h3s[0]) or TEAM_RU.get(a_abbr, a_abbr)
    teamB_name = team_from_h3(h3s[1]) or TEAM_RU.get(b_abbr, b_abbr)

    playersA, scoreA = parse_stats_table_near(h3s[0])
    playersB, scoreB = parse_stats_table_near(h3s[1])

    # если «Итого» не поймали, выдернем общий «XX:YY»
    if not (scoreA and scoreB):
        txt = soup.get_text(" ", strip=True)
        m = re.search(r"\b(\d{1,3})\s*:\s*(\d{1,3})\b", txt)
        if m:
            scoreA = scoreA or int(m.group(1))
            scoreB = scoreB or int(m.group(2))

    ot = detect_ot(soup)

    logdbg("PARSE TEAMS", teamA_name, teamB_name, "SCORES", scoreA or 0, scoreB or 0,
           "A_rows", len(playersA), "B_rows", len(playersB))

    return {
        "url": url,
        "teams": [
            {"abbr": a_abbr, "name_ru": teamA_name, "score": scoreA or 0, "players": playersA},
            {"abbr": b_abbr, "name_ru": teamB_name, "score": scoreB or 0, "players": playersB},
        ],
        "ot": ot,
    }

# ========= PICKING & FORMAT =========
def is_double_double(p: dict) -> bool:
    cats = [p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]]
    return sum(1 for v in cats if v >= 10) >= 2

def pick_players_for_team(team_abbr: str, plist: list[dict]) -> list[dict]:
    if not plist:
        return []
    plist_sorted = sorted(plist, key=lambda x: (x["pts"], x["reb"], x["ast"]), reverse=True)
    picked = []

    want_last = "Дёмин" if team_abbr == "BKN" else ("Голдин" if team_abbr == "MIA" else None)
    special = None
    if want_last:
        for p in plist:
            if p["name_ru"].split()[-1].strip().lower() == want_last.lower():
                special = p; break

    best = plist_sorted[0]; picked.append(best)

    if special and special not in picked:
        picked.append(special)
    else:
        for p in plist_sorted[1:]:
            if p in picked: 
                continue
            if p["pts"] >= 20 or is_double_double(p) or p["stl"] >= 5 or p["blk"] >= 5:
                picked.append(p)
                break

    return picked[:2]

def initials_plus_surname(name_ru: str) -> str:
    parts = [w for w in name_ru.split() if w]
    if len(parts) <= 1: return name_ru
    return f"{parts[0][0]}. {parts[-1]}"

def fmt_player_line(p: dict, force_top3: bool = False) -> str:
    name = initials_plus_surname(p["name_ru"])
    pts, reb, ast, stl, blk = p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]

    if force_top3:
        extras = [
            ("подбор","подбора","подборов", reb),
            ("передача","передачи","передач", ast),
            ("перехват","перехвата","перехватов", stl),
            ("блок-шот","блок-шота","блок-шотов", blk),
        ]
        extras = [(w1,w2,w3,val) for (w1,w2,w3,val) in extras if val and val>0]
        extras.sort(key=lambda t: t[3], reverse=True)
        extras = extras[:3]
        parts = [f"{name}: {pts} {ru_plural(pts, ('очко','очка','очков'))}"]
        for (w1,w2,w3,val) in extras:
            parts.append(f"{val} {ru_plural(val,(w1,w2,w3))}")
        return "<span class=\"tg-spoiler\">" + ", ".join(parts) + "</span>"

    parts = [f"{name}: {pts} {ru_plural(pts, ('очко','очка','очков'))}"]
    if reb >= 5: parts.append(f"{reb} {ru_plural(reb, ('подбор','подбора','подборов'))}")
    if ast >= 5: parts.append(f"{ast} {ru_plural(ast, ('передача','передачи','передач'))}")
    if stl >= 4: parts.append(f"{stl} {ru_plural(stl, ('перехват','перехвата','перехватов'))}")
    if blk >= 4: parts.append(f"{blk} {ru_plural(blk, ('блок-шот','блок-шота','блок-шотов'))}")
    return "<span class=\"tg-spoiler\">" + ", ".join(parts) + "</span>"

# ========= BUILD POST =========
def build_post(d: date) -> str:
    links = fetch_day_links(d)
    games = []
    for u in links:
        try:
            g = parse_sports_match(u)
            if g:
                games.append(g)
        except Exception as e:
            logdbg("PARSE ERROR", u, repr(e))

    n = len(games)
    title = (
        f"НБА • {ru_date(d)} • {n} {ru_plural(n, ('матч','матча','матчей'))}\n"
        "Результаты надёжно спрятаны 👇\n"
        "–––––––––––––––––––––––\n\n"
    )
    if n == 0:
        return title.rstrip()

    lines = []
    for idx, g in enumerate(games):
        t1, t2 = g["teams"][0], g["teams"][1]
        a_win = t1["score"] > t2["score"]; b_win = t2["score"] > t1["score"]

        name1 = TEAM_RU.get(t1["abbr"], t1["name_ru"])
        name2 = TEAM_RU.get(t2["abbr"], t2["name_ru"])
        e1 = team_emoji(t1["abbr"]); e2 = team_emoji(t2["abbr"])

        scoreA = f"<b>{t1['score']}</b>" if a_win else f"{t1['score']}"
        scoreB = f"<b>{t2['score']}</b>" if b_win else f"{t2['score']}"
        ot_tag = " (ОТ)" if g["ot"] else ""

        lines.append(f"{e1} {name1}: <span class=\"tg-spoiler\">{scoreA}</span>")
        lines.append(f"{e2} {name2}: <span class=\"tg-spoiler\">{scoreB}</span>{ot_tag}")
        lines.append("")

        picked1 = pick_players_for_team(t1["abbr"], t1["players"])
        for p in picked1:
            force = (t1["abbr"] == "BKN" and p["name_ru"].split()[-1] == "Дёмин") or \
                    (t1["abbr"] == "MIA" and p["name_ru"].split()[-1] == "Голдин")
            lines.append(fmt_player_line(p, force_top3=force))

        if picked1:
            lines.append("")

        picked2 = pick_players_for_team(t2["abbr"], t2["players"])
        for p in picked2:
            force = (t2["abbr"] == "BKN" and p["name_ru"].split()[-1] == "Дёмин") or \
                    (t2["abbr"] == "MIA" and p["name_ru"].split()[-1] == "Голдин")
            lines.append(fmt_player_line(p, force_top3=force))

        if idx + 1 < len(games):
            lines.append("")
            lines.append("–––––––––––––––––––––––")
            lines.append("")

    return title + "\n".join(lines).strip()

# ========= TELEGRAM =========
def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    r = S.post(url, json=payload, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text}")

# ========= MAIN =========
def pick_report_day() -> date:
    if DATE_MODE == "best":
        d_today = et_today()
        d_yest  = d_today - timedelta(days=1)
        n_today = count_games_for_day(d_today)
        n_yest  = count_games_for_day(d_yest)
        logdbg(f"DAY CANDIDATES ET today={d_today}({n_today}) / yest={d_yest}({n_yest})")
        return d_today if n_today >= n_yest else d_yest
    else:
        # всегда сегодня по ET
        d = et_today()
        logdbg("DAY PICKED (today)", d)
        return d

if __name__ == "__main__":
    try:
        day = pick_report_day()
        post = build_post(day)
        tg_send(post)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
