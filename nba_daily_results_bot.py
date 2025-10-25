#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU)  — SPORTS.RU ONLY

Делает один пост с результатами НБА за игровой день:
• Источник матчей и бокскоров: sports.ru (итоги дня + страницы матчей)
  - День: https://www.sports.ru/stat/basketball/center/end/YYYY/MM/DD.html
  - Матч: https://www.sports.ru/basketball/match/{slug}/

Формат игрока:
  «И. Фамилия: N очков[, X подборов][, Y передач][, Z перехвата][, T блок-шотов] [🔥]»
Правила включения показателей:
  - подборы ≥ 5, передачи ≥ 5,
  - перехваты ≥ 4 (всегда показываем), блок-шоты ≥ 4 (всегда показываем)
Выбор игроков:
  - 1..2 лучших у каждой команды по “вкладу” (pts*2 + reb + ast + stl*2 + blk*2)
  - Всегда включать Дёмина (Бруклин) и Голдина (Майами), если они играли (жирным).
ОТ:
  - считаем по количеству пар «N : N» в разбивке по четвертям (>4 → nОТ).

Зависимости: requests, beautifulsoup4, lxml
"""

import os, sys, re, json, time, unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ---------------------- ENV ----------------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# ---------------------- CONST --------------------
BASE = "https://www.sports.ru"
DAY_URL = BASE + "/stat/basketball/center/end/{yyyy}/{mm}/{dd}.html"

RU_MONTHS = {
    1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
    7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"
}

# Эмодзи команд. Замените значения на ваши кастом-эмодзи из набора t.me/addemoji/got_ball_team.
TEAM_EMO = {
    "Атланта": "🦅", "Бостон": "☘️", "Бруклин": "🕸️", "Шарлотт": "🐝",
    "Чикаго": "🐂", "Кливленд": "🛡️", "Даллас": "🐎", "Денвер": "⛏️",
    "Детройт": "🔧", "Голден Стэйт": "🗡️", "Хьюстон": "🚀", "Индиана": "💫",
    "Клипперс": "✂️", "Лейкерс": "⭐️", "Мемфис": "🐻", "Майами": "🔥",
    "Милуоки": "🦌", "Миннесота": "🐺", "Новый Орлеан": "🪶", "Нью-Йорк": "🗽",
    "Оклахома-Сити": "⚡", "Орландо": "✨", "Филадельфия": "🔔", "Финикс": "☀️",
    "Портленд": "🧭", "Сакраменто": "👑", "Сан-Антонио": "🪙", "Торонто": "🦖",
    "Юта": "🎷", "Вашингтон": "🧙",
}
EMO_FALLBACK = "🏀"

# Спец-игроки для обязательного включения (если играли)
FORCE_BY_TEAM = {
    "Бруклин": "Дёмин",   # Егор Дёмин
    "Майами":   "Голдин", # (пример: Голдин; оставить как на sports.ru)
}

# ---------------------- HTTP ---------------------
def make_session():
    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.7,
              status_forcelist=[429,500,502,503,504],
              allowed_methods=["GET","POST"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({
        "User-Agent":"NBA-DailyResults-SportsRu/1.0 (+https://www.sports.ru)",
        "Accept-Language":"ru-RU,ru;q=0.9,en;q=0.5",
    })
    return s

S = make_session()

def get(url, timeout=25):
    r = S.get(url, timeout=timeout)
    r.raise_for_status()
    return r

# ---------------------- DATES --------------------
def ru_date(d: date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def pick_report_date_et() -> date:
    """
    Если сейчас в Нью-Йорке до 08:00, считаем, что отчётный день — вчера (игровой день ещё «закрывается»).
    """
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return (now_et.date() - timedelta(days=1)) if now_et.hour < 8 else now_et.date()

def best_day_page(target_et: date) -> tuple[date, str]:
    """
    Возвращает (реальная_дата для sports.ru, url дня), выбирая из [target, target-1, target+1]
    страницу, где больше всего ссылок на матчи НБА.
    """
    candidates = [target_et, target_et - timedelta(days=1), target_et + timedelta(days=1)]
    best = (target_et, DAY_URL.format(yyyy=target_et.year, mm=f"{target_et.month:02}", dd=f"{target_et.day:02}"))
    best_count = -1
    for d in candidates:
        url = DAY_URL.format(yyyy=d.year, mm=f"{d.month:02}", dd=f"{d.day:02}")
        try:
            soup = BeautifulSoup(get(url).text, "lxml")
        except Exception:
            continue
        links = soup.select('a[href^="/basketball/match/"]')
        # Пройдём по ссылкам и прикинем, сколько реально матчей НБА среди них
        cnt = 0
        for a in links:
            if not re.fullmatch(r"\d+\s*:\s*\d+", (a.get_text() or "").strip()):
                continue
            cnt += 1
        if cnt > best_count:
            best_count = cnt
            best = (d, url)
    return best

# ---------------------- PARSE DAY ----------------
@dataclass
class GameLink:
    url: str
    t1: str
    t2: str
    s1: int
    s2: int

def parse_day_nba(url: str) -> list[GameLink]:
    """
    Со страницы 'итоги дня' собираем пары (t1, score, t2) и ссылки на страницу матча.
    Командные имена — русские. Счёт — финальный.
    """
    soup = BeautifulSoup(get(url).text, "lxml")
    out: list[GameLink] = []

    # На странице много турниров; берём все якоря-«счёты» и потом фильтруем по самой странице матча (НБА).
    for a_score in soup.select('a[href^="/basketball/match/"]'):
        txt = (a_score.get_text() or "").strip()
        if not re.fullmatch(r"\d+\s*:\s*\d+", txt):
            continue

        # Обе соседние команды — ближайшие <a> с /basketball/club/
        prev_team = a_score.find_previous("a", href=re.compile(r"^/basketball/club/"))
        next_team = a_score.find_next("a", href=re.compile(r"^/basketball/club/"))
        if not prev_team or not next_team:
            continue

        t1 = prev_team.get_text(strip=True)
        t2 = next_team.get_text(strip=True)
        try:
            s1, s2 = [int(x) for x in re.split(r"\s*:\s*", txt)]
        except Exception:
            continue

        url_match = urljoin(BASE, a_score.get("href"))
        out.append(GameLink(url_match, t1, t2, s1, s2))

    # Отфильтруем не-НБА: на странице матча должен быть хлебный «НБА»
    nba_only: list[GameLink] = []
    for g in out:
        try:
            msoup = BeautifulSoup(get(g.url).text, "lxml")
            if msoup.find("a", string=re.compile(r"\bНБА\b")):
                nba_only.append(g)
        except Exception:
            pass

    return nba_only

# ---------------------- PARSE MATCH ----------------
@dataclass
class Player:
    name: str
    pts: int
    reb: int
    ast: int
    stl: int
    blk: int
    minutes: str

def _extract_ot_count(page_text: str) -> int:
    # Ищем строку, где 4+ пар «N : N», разделённых двумя+ пробелами
    m = re.search(r'(?:\d+\s*:\s*\d+)(?:\s{2,}\d+\s*:\s*\d+){3,}', page_text)
    if not m:
        return 0
    pairs = re.findall(r'\d+\s*:\s*\d+', m.group(0))
    return max(0, len(pairs) - 4)

def _clean_lines(text: str) -> list[str]:
    lines = [ln.strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]
    # убираем строки, где только номер (джерси)
    return [ln for ln in lines if not re.fullmatch(r"\d{1,3}", ln)]

def _parse_players_block(text_block: str) -> list[Player]:
    """
    Парсинг «… статистика игроков» через чистый текст:
    - строка с ФИО (на рус.)
    - следующая строка с числами (очки могут отсутствовать явным первым числом -> считаем 0)
    """
    lines = _clean_lines(text_block)
    players: list[Player] = []
    i = 0
    while i < len(lines) - 1:
        name_line = lines[i]
        stats_line = lines[i + 1]

        # отсекаем заголовки/примечания
        if "Игрок" in name_line or "О –" in name_line or "Видео" in name_line or "Лента событий" in name_line:
            i += 1
            continue

        if not re.search(r'(\d+/\d+|\d+:\d+)', stats_line):
            i += 1
            continue

        # Токены
        tokens = stats_line.replace('%', ' ').split()
        pts = 0
        if tokens and '/' not in tokens[0]:
            try:
                pts = int(tokens[0])
                tokens = tokens[1:]
            except Exception:
                pass

        # пропустить три стрельбовых блока (x/y, %, x/y, %, x/y, %)
        j = 0
        segs = 0
        while j < len(tokens) and segs < 3:
            if '/' in tokens[j]:
                j += 1
                if j < len(tokens) and tokens[j].isdigit():
                    j += 1
                segs += 1
            else:
                j += 1
        rest = tokens[j:]

        nums = []
        minutes = ""
        for tok in rest:
            if re.fullmatch(r"\d+:\d+", tok):
                minutes = tok
                break
            if tok.isdigit():
                nums.append(int(tok))

        # порядок: ПБ, АП, Ф, ПХ, П, БШ
        while len(nums) < 6:
            nums.append(0)
        reb, ast, _fouls, stl, _to, blk = nums[:6]

        players.append(Player(name=name_line, pts=int(pts), reb=int(reb),
                              ast=int(ast), stl=int(stl), blk=int(blk),
                              minutes=minutes))
        i += 2

    return players

def parse_match_players_by_team(match_html: str, team1_ru: str, team2_ru: str) -> tuple[list[Player], list[Player], int]:
    """
    Возвращает (игроки_команды1, игроки_команды2, ot_count)
    """
    soup = BeautifulSoup(match_html, "lxml")
    page_text = soup.get_text("\n", strip=False)
    ot_count = _extract_ot_count(page_text)

    # Выделяем две секции «{Команда}. статистика игроков»
    def block_for(team_ru: str) -> str:
        pattern = rf"{re.escape(team_ru)}\.\s*статистика игроков"
        m = re.search(pattern, page_text, flags=re.IGNORECASE)
        if not m:
            return ""
        start = m.end()
        # до следующего заголовка уровня «###» или следующей секции статистики
        tail = page_text[start:]
        m2 = re.search(r"\n### |\n[A-ЯЁA-Z].+статистика игроков", tail)
        return tail[:m2.start()] if m2 else tail

    b1 = block_for(team1_ru)
    b2 = block_for(team2_ru)

    p1 = _parse_players_block(b1) if b1 else []
    p2 = _parse_players_block(b2) if b2 else []

    # Отфильтруем DNP (нет минут или 00:00)
    p1 = [p for p in p1 if p.minutes and p.minutes != "00:00"]
    p2 = [p for p in p2 if p.minutes and p.minutes != "00:00"]

    return p1, p2, ot_count

# ---------------------- FORMAT --------------------
def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100
    n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def make_initial_surname(full_ru: str) -> str:
    """
    «Имя Фамилия» → «И. Фамилия», плюс нормализация «младший/старший».
    Если имя составное (Ар Джей), берём первую букву первого слова.
    """
    parts = [p for p in re.split(r"\s+", full_ru.strip()) if p]
    if not parts:
        return full_ru
    first = parts[0]
    last = parts[-1]
    # «-младший», «младший», «старший»
    last = re.sub(r"(?i)\bмладший\b", "мл.", last)
    last = re.sub(r"(?i)\bстарший\b", "ст.", last)
    # Тире-составные фамилии оставляем как есть
    init = (first[0] + ".") if first else ""
    return f"{init} {last}".strip()

def standout_score(p: Player) -> int:
    return p.pts*2 + p.reb + p.ast + p.stl*2 + p.blk*2

def must_show_stat_line(p: Player) -> bool:
    return (p.pts >= 25) or (p.reb >= 12) or (p.ast >= 10) or (p.stl >= 4) or (p.blk >= 4)

def format_player_line(p: Player, bold: bool=False) -> str:
    name = make_initial_surname(p.name)
    if bold: name = f"<b>{name}</b>"

    parts = [f"{p.pts} {ru_plural(p.pts, ('очко','очка','очков'))}"]
    if p.reb >= 5:
        parts.append(f"{p.reb} {ru_plural(p.reb, ('подбор','подбора','подборов'))}")
    if p.ast >= 5:
        parts.append(f"{p.ast} {ru_plural(p.ast, ('передача','передачи','передач'))}")
    if p.stl >= 4:
        parts.append(f"{p.stl} {ru_plural(p.stl, ('перехват','перехвата','перехватов'))}")
    if p.blk >= 4:
        parts.append(f"{p.blk} {ru_plural(p.blk, ('блок-шот','блок-шота','блок-шотов'))}")

    flame = " 🔥" if (p.pts >= 35 or p.reb >= 20 or p.ast >= 15 or p.stl >= 6 or p.blk >= 6) else ""
    return f"{name}: {', '.join(parts)}{flame}"

def pick_players_for_team(players: list[Player], team_ru: str) -> list[tuple[Player,bool]]:
    """
    Возвращает до 2 игроков: [(player, bold?)]
    - Спец-вставки: Дёмин/Голдин — если в ростере и играли, то включаем и делаем жирным.
    - Остальные — по must_show_stat_line, отсортировано по «вкладу».
    - Если никого не набралось — берём топ по очкам.
    """
    chosen: list[tuple[Player,bool]] = []

    special_last = FORCE_BY_TEAM.get(team_ru, None)
    if special_last:
        for p in players:
            if re.search(special_last, p.name, flags=re.IGNORECASE):
                chosen.append((p, True))
                break

    # кандидаты по правилам
    cands = [p for p in players if must_show_stat_line(p)]
    cands.sort(key=standout_score, reverse=True)

    for p in cands:
        if len(chosen) >= 2: break
        # не дублировать спец-игрока
        if any(x[0].name == p.name for x in chosen):
            continue
        chosen.append((p, False))

    if not chosen and players:
        # никто не прошёл — берём топ-скорера
        top = max(players, key=lambda x: x.pts)
        chosen.append((top, False))

    return chosen[:2]

def team_emoji(team_ru: str) -> str:
    return TEAM_EMO.get(team_ru, EMO_FALLBACK)

def build_game_block(g: GameLink) -> str:
    # страница матча
    html = get(g.url).text
    p1, p2, ot = parse_match_players_by_team(html, g.t1, g.t2)

    # шапка
    e1 = team_emoji(g.t1)
    e2 = team_emoji(g.t2)
    ot_tag = "" if ot == 0 else f" ({'ОТ' if ot==1 else str(ot)+'ОТ'})"
    head = f"{e1} {g.t1}: <b>{g.s1}</b>\n{e2} {g.t2}: {g.s2}{ot_tag}\n\n"

    # игроки
    lines = []
    for p, bold in pick_players_for_team(p1, g.t1):
        lines.append(format_player_line(p, bold))
    if lines: lines.append("")  # пустая строка между командами
    for p, bold in pick_players_for_team(p2, g.t2):
        lines.append(format_player_line(p, bold))

    return head + "\n".join([ln for ln in lines if ln.strip()])

# ---------------------- MESSAGE -------------------
def build_post() -> str:
    # Выбираем лучший «день» на sports.ru под наш ET-день
    et_day = pick_report_date_et()
    chosen_date, url = best_day_page(et_day)

    games = parse_day_nba(url)
    title = f"НБА • {ru_date(chosen_date)} • {len(games)} {ru_plural(len(games), ('матч','матча','матчей'))}\n"
    title += "Результаты надёжно спрятаны 👇\n"
    title += "–––––––––––––––––––––––\n\n"

    if not games:
        return title.rstrip()

    blocks = []
    for i, g in enumerate(games, 1):
        try:
            blk = build_game_block(g)
            blocks.append(blk)
        except Exception as e:
            blocks.append("— данные по матчу временно недоступны")
        if i < len(games):
            blocks.append("–––––––––––––––––––––––")

    return (title + "\n".join(blocks)).strip()

# ---------------------- TELEGRAM -------------------
def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    MAX = 3700  # защитный лимит
    t = text
    parts = []
    while t:
        if len(t) <= MAX:
            parts.append(t); break
        cut = t.rfind("\n–––––––––––––––––––––––\n", 0, MAX)
        if cut == -1:
            cut = t.rfind("\n\n", 0, MAX)
        if cut == -1:
            cut = MAX
        parts.append(t[:cut].rstrip())
        t = t[cut:].lstrip()

    for part in parts:
        resp = S.post(url, json={
            "chat_id": CHAT_ID,
            "text": part,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=25)
        if resp.status_code != 200:
            raise RuntimeError(f"Telegram error {resp.status_code}: {resp.text}")
        time.sleep(0.3)

# ---------------------- MAIN ----------------------
if __name__ == "__main__":
    try:
        text = build_post()
        tg_send(text)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
