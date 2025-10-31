#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU)
Источник данных: Sports.ru (страницы конкретных матчей).
- День: https://www.sports.ru/stat/basketball/center/end/YYYY/MM/DD.html
- Матч: https://www.sports.ru/basketball/match/<slug>/

Что делает:
• На странице дня собирает ссылки матчей вида ".../<team-a>-vs-<team-b>/".
• Жёстко фильтрует только НБА по слугам (оба участника должны мапиться в аббревиатуры НБА).
• Внутри матча ищет <h3> "… статистика игроков" → ближайшую таблицу → читает игроков и «Итого».
• Выбирает 1–2 «выдающихся» игрока на команду:
  - Всегда лучший по очкам,
  - плюс ещё один, если: ≥20 очков, ИЛИ дабл-дабл (по PTS/REB/AST/STL/BLK),
    ИЛИ ≥5 перехватов, ИЛИ ≥5 блок-шотов.
• Если играли Егор Дёмин (BKN) или Влад Голдин (MIA) — включаем обязательно и показываем 3 наибольших ненулевых показателя.
• Сообщение одним постом; счёт и строки со статистикой спрятаны в спойлеры.
• Жирным выделяется счёт победившей команды.
"""

import os, sys, re, json, time
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ---------- ENV ----------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()
DEBUG     = bool(os.getenv("DEBUG_NBA", "").strip())

def logdbg(*a):
    if DEBUG:
        print("[DBG]", *a, file=sys.stderr)

# ---------- DATES ----------
def pick_report_date() -> date:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return (now_et.date() - timedelta(days=1)) if now_et.hour < 8 else now_et.date()

RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

# ---------- HTTP ----------
def make_session():
    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.7,
              status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({
        "User-Agent": "NBA-DailyResultsBot/4.1 (sports.ru)",
        "Accept-Language": "ru,en;q=0.7",
    })
    return s
S = make_session()

# ---------- CONSTANTS / MAPS ----------
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
def abbr_from_url(url: str, side: int) -> str | None:
    m = re.search(r"/basketball/match/([a-z0-9\-]+)-vs-([a-z0-9\-]+)/", url)
    if not m: return None
    slug = m.group(1 + side)
    return SLUG2ABBR.get(slug)

def team_emoji(_abbr: str) -> str:
    # Простая заглушка — обычный смайл. (Кастом-эмодзи через entities — отдельной версией.)
    return "🏀"

# ---------- DAY LINKS ----------
def fetch_day_links(d: date) -> list[str]:
    url = DAY_URL_TMPL.format(y=d.year, m=d.month, d=d.day)
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        logdbg("SPORTS DAY URL FAIL", url, r.status_code); return []
    logdbg("SPORTS DAY URL", url, "OK")
    soup = BeautifulSoup(r.text, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        h = a["href"]
        if not (h.startswith("/basketball/match/") or h.startswith(MATCH_PREFIX)):
            continue
        if "/live/" in h:  # прямой лайв без итога — пропустим
            continue
        full = urljoin("https://www.sports.ru", h)
        if "vs" not in full:    # не матч «А vs B»
            continue
        links.append(full if full.endswith("/") else full + "/")
    # dedup
    out, seen = [], set()
    for u in links:
        if u not in seen:
            out.append(u); seen.add(u)
    logdbg("SPORTS LINKS", len(out))
    return out

# ---------- PARSE MATCH ----------
HEAD_TEAM_RE = re.compile(r"\s*([^.]+)\.\s*статистика игроков\s*$", re.IGNORECASE)

def _norm_header(text: str) -> str:
    t = re.sub(r"\s+", "", text.strip().lower())
    # разные варианты на sports.ru
    t = t.replace("передачи", "перед")
    t = t.replace("перехваты", "перех")
    t = t.replace("блок-шоты", "блокшоты")
    t = t.replace("блок-шот", "блокшоты")
    t = t.replace("подборы", "подбор")
    return t

def _metric_of_header(norm: str) -> str | None:
    if norm.startswith("очки"): return "pts"
    if norm.startswith("подбор"): return "reb"
    if norm.startswith("перед"): return "ast"
    if norm.startswith("перех"): return "stl"
    if norm.startswith("блокшоты"): return "blk"
    return None

def _int_first(text: str) -> int:
    m = re.search(r"-?\d+", text or "")
    return int(m.group(0)) if m else 0

def parse_sports_match(url: str) -> dict | None:
    # Сначала отфильтруем НЕ-НБА по слугам:
    a_abbr = abbr_from_url(url, 0)
    b_abbr = abbr_from_url(url, 1)
    if not (a_abbr and b_abbr and a_abbr in TEAM_RU and b_abbr in TEAM_RU):
        return None  # не НБА

    r = S.get(url, timeout=25)
    if r.status_code != 200:
        logdbg("PARSE START", url, "HTTP", r.status_code); return None
    soup = BeautifulSoup(r.text, "html.parser")

    # Собираем два блока "… статистика игроков"
    blocks = []
    for h3 in soup.find_all("h3"):
        txt = (h3.get_text(" ", strip=True) or "")
        m = HEAD_TEAM_RE.match(txt)
        if not m: 
            continue
        team_name = m.group(1).strip().strip('«»"')
        # следующая таблица
        tbl = h3.find_next("table")
        if not tbl:
            continue
        blocks.append((team_name, tbl))

    # Нужны два первых (на Sports.ru их именно два — по командам)
    if len(blocks) < 2:
        logdbg("PARSE START", url, "NO TEAM TABLES"); 
        return None

    def parse_table(team_name: str, tbl) -> tuple[list[dict], int]:
        # определим индекс колонок по заголовку
        header_map = {}
        header_row = None
        for tr in tbl.find_all("tr"):
            ths = tr.find_all(["th","td"])
            # заголовок содержит слова «очки/подборы/...», а не только числа
            if ths and any(re.search(r"[А-Яа-яA-Za-z]", c.get_text()) for c in ths):
                header_row = [c.get_text(" ", strip=True) for c in ths]
                break
        if not header_row:
            return [], 0
        for idx, htxt in enumerate(header_row):
            metric = _metric_of_header(_norm_header(htxt))
            if metric and metric not in header_map:
                header_map[metric] = idx

        players = []
        team_total_pts = 0
        for tr in tbl.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            # «Итого» — команда:
            first_txt = tds[0].get_text(" ", strip=True)
            if re.search(r"^итого$", first_txt, re.IGNORECASE):
                # очки команды
                if "pts" in header_map:
                    team_total_pts = _int_first(tds[header_map["pts"]].get_text(" ", strip=True))
                continue
            # иначе — игрок
            # имя:
            name_cell = tds[0]
            a = name_cell.find("a")
            name_ru = (a.get_text(" ", strip=True) if a else name_cell.get_text(" ", strip=True)).strip()
            # метрики:
            def val(metric):
                if metric not in header_map: 
                    return 0
                return _int_first(tds[header_map[metric]].get_text(" ", strip=True))
            p = {
                "name_ru": name_ru,
                "pts": val("pts"),
                "reb": val("reb"),
                "ast": val("ast"),
                "stl": val("stl"),
                "blk": val("blk"),
            }
            # отбрасываем пустышки
            if any(p[k] for k in ("pts","reb","ast","stl","blk")):
                players.append(p)
        return players, team_total_pts

    teamA_name, tblA = blocks[0]
    teamB_name, tblB = blocks[1]
    playersA, scoreA = parse_table(teamA_name, tblA)
    playersB, scoreB = parse_table(teamB_name, tblB)

    # Бывает, что «Итого» строки нет (редко) — тогда попробуем взять общий счёт из шапки
    if not (scoreA and scoreB):
        # ищем «XX:YY» возле шапки
        txt = soup.get_text("\n", strip=True)
        m = re.search(r"\b(\d{1,3})\s*:\s*(\d{1,3})\b", txt)
        if m:
            scoreA = scoreA or int(m.group(1))
            scoreB = scoreB or int(m.group(2))

    ot = False
    # Детект ОТ по количеству четвертей в блоке счёта по периодам (если таблица есть)
    per_table = None
    for h3 in soup.find_all("h3"):
        if "помесячно" in (h3.get_text(" ", strip=True).lower()):
            # на всякий — редко встречается; пропустим
            continue
        txt = h3.get_text(" ", strip=True).lower()
        if "периоды" in txt or "по периодам" in txt:
            per_table = h3.find_next("table")
            break
    if per_table:
        # посчитаем пары чисел в первой «сводной» строке
        row = per_table.find("tr")
        if row:
            s = row.get_text(" ", strip=True)
            pairs = re.findall(r"\b\d{1,2}\s*:\s*\d{1,2}\b", s)
            ot = len(pairs) > 4

    game = {
        "url": url,
        "teams": [
            {"name_ru": teamA_name, "abbr": a_abbr, "score": scoreA or 0, "players": playersA},
            {"name_ru": teamB_name, "abbr": b_abbr, "score": scoreB or 0, "players": playersB},
        ],
        "ot": ot,
    }
    logdbg("PARSE TEAMS", teamA_name, teamB_name, "SCORES", scoreA or 0, scoreB or 0,
           "A_rows", len(playersA), "B_rows", len(playersB))
    return game

# ---------- PICKING & FORMAT ----------
def is_double_double(p: dict) -> bool:
    cats = [p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]]
    return sum(1 for v in cats if v >= 10) >= 2

def pick_players_for_team(team_abbr: str, plist: list[dict]) -> list[dict]:
    if not plist:
        return []
    plist_sorted = sorted(plist, key=lambda x: (x["pts"], x["reb"], x["ast"]), reverse=True)
    picked = []

    # спец-игроки
    want_last = "Дёмин" if team_abbr == "BKN" else ("Голдин" if team_abbr == "MIA" else None)
    special = None
    if want_last:
        for p in plist:
            if p["name_ru"].split()[-1].strip().lower() == want_last.lower():
                special = p; break

    best = plist_sorted[0]
    picked.append(best)

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
    if len(parts) <= 1:
        return name_ru
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

# ---------- BUILD POST ----------
def build_post(d: date) -> str:
    links = fetch_day_links(d)

    games = []
    for u in links:
        try:
            g = parse_sports_match(u)
            if g and (g["teams"][0]["score"] or g["teams"][1]["score"]):
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
        a_win = t1["score"] > t2["score"]
        b_win = t2["score"] > t1["score"]

        e1 = team_emoji(t1["abbr"])
        e2 = team_emoji(t2["abbr"])
        name1 = TEAM_RU.get(t1["abbr"], t1["name_ru"])
        name2 = TEAM_RU.get(t2["abbr"], t2["name_ru"])

        scoreA = f"<b>{t1['score']}</b>" if a_win else f"{t1['score']}"
        scoreB = f"<b>{t2['score']}</b>" if b_win else f"{t2['score']}"
        ot_tag = " (ОТ)" if g["ot"] else ""

        lines.append(f"{e1} {name1}: <span class=\"tg-spoiler\">{scoreA}</span>")
        lines.append(f"{e2} {name2}: <span class=\"tg-spoiler\">{scoreB}</span>{ot_tag}")
        lines.append("")  # отступ

        # команда 1
        picked1 = pick_players_for_team(t1["abbr"], t1["players"])
        for p in picked1:
            force = (t1["abbr"] == "BKN" and p["name_ru"].split()[-1] == "Дёмин") or \
                    (t1["abbr"] == "MIA" and p["name_ru"].split()[-1] == "Голдин")
            lines.append(fmt_player_line(p, force_top3=force))

        if picked1:
            lines.append("")  # раздел между командами в блоке игроков

        # команда 2
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

# ---------- TELEGRAM ----------
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

# ---------- MAIN ----------
if __name__ == "__main__":
    try:
        day = pick_report_date()
        logdbg("DAY", day.isoformat())
        post = build_post(day)
        tg_send(post)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
