#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU)

• Источник матчей/статистики: ESPN (public)
  - Scoreboard: https://site.web.api.espn.com/apis/v2/sports/basketball/nba/scoreboard?dates=YYYYMMDD
  - Boxscore : https://site.web.api.espn.com/apis/v2/sports/basketball/nba/boxscore?event={eventId}

• Имена игроков (фамилия по-русски):
  1) профиль на sports.ru/basketball/person|player/{slug}/ -> заголовок -> последнее слово;
  2) поиск на sports.ru;
  3) словарь исключений;
  4) фоллбэк: латиница.
  Кэш: ru_map_nba.json (id игрока ESPN -> "Фамилия"), очередь: ru_pending_nba.json

• Формат:
  - Заголовок: НБА • {дата} • {N матчей}
  - По матчу: 2 строки со счётом, у победителя жирным только число, рядом эмодзи-логотип;
  - Далее 1–3 игрока у каждой команды (топ по очкам):
      очки — всегда; подборы ≥5; передачи ≥5; перехваты ≥4; блок-шоты ≥4.
"""

import os, sys, re, json, time, unicodedata
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ---------- ENV ----------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# ---------- ESPN ----------
ESPN_BASE = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba"

# ---------- SPORTS.RU ----------
SPORTS_RU = "https://www.sports.ru"
SRU_PERSON = SPORTS_RU + "/basketball/person/"
SRU_PLAYER = SPORTS_RU + "/basketball/player/"
SRU_SEARCH = SPORTS_RU + "/search/?q="

# ---------- CACHE FILES ----------
RU_MAP_PATH     = "ru_map_nba.json"      # { athleteId(str): "Фамилия" }
RU_PENDING_PATH = "ru_pending_nba.json"  # [{id, first, last}]

# Глобальные контейнеры кэша (НЕ переназначаем в коде — только изменяем содержимое)
RU_MAP: dict[str, str] = {}
RU_PENDING: list[dict] = []
_session_pending_ids: set[str] = set()

# ---------- RUSSIAN DATE ----------
RU_MONTHS = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}

def ru_date(d: date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def ru_plural(n: int, forms: tuple[str, str, str]) -> str:
    # очко/очка/очков
    n = abs(int(n)) % 100
    n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

# ---------- HTTP ----------
def make_session():
    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.6,
              status_forcelist=[429,500,502,503,504],
              allowed_methods=["GET","POST"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({
        "User-Agent": "NBA-DailyResultsBot/1.1 (+espn; sports.ru resolver)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
    })
    return s

S = make_session()

def _get_json(url: str) -> dict:
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        return {}
    try:
        return r.json()
    except Exception:
        return {}

def log(*a): print(*a, file=sys.stderr)

# ---------- PICK DATE ----------
def pick_report_date() -> date:
    # По умолчанию ориентируемся на Восточное время США, как по НБА
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return (now_et.date() - timedelta(days=1)) if now_et.hour < 8 else now_et.date()

# ---------- TEAM NAMES / EMOJIS ----------
TEAM_RU = {
    "ATL": ("Атланта", "🦅"),
    "BOS": ("Бостон", "☘️"),
    "BKN": ("Бруклин", "🕸️"),
    "CHA": ("Шарлотт", "🐝"),
    "CHI": ("Чикаго", "🐂"),
    "CLE": ("Кливленд", "🛡️"),
    "DAL": ("Даллас", "🐎"),
    "DEN": ("Денвер", "⛏️"),
    "DET": ("Детройт", "🔧"),
    "GSW": ("Голден Стэйт", "🗡️"),
    "HOU": ("Хьюстон", "🚀"),
    "IND": ("Индиана", "💫"),
    "LAC": ("Клипперс", "✂️"),
    "LAL": ("Лейкерс", "⭐"),
    "MEM": ("Мемфис", "🐻"),
    "MIA": ("Майами", "🔥"),
    "MIL": ("Милуоки", "🦌"),
    "MIN": ("Миннесота", "🐺"),
    "NOP": ("Новый Орлеан", "🪶"),
    "NYK": ("Нью-Йорк", "🗽"),
    "OKC": ("Оклахома-Сити", "⚡"),
    "ORL": ("Орландо", "✨"),
    "PHI": ("Филадельфия", "🔔"),
    "PHX": ("Финикс", "☀️"),
    "POR": ("Портленд", "🧭"),
    "SAC": ("Сакраменто", "👑"),
    "SAS": ("Сан-Антонио", "🪙"),
    "TOR": ("Торонто", "🦖"),
    "UTA": ("Юта", "🎷"),
    "WAS": ("Вашингтон", "🧙"),
}

def team_ru_and_emoji(abbr: str) -> tuple[str,str]:
    abbr = (abbr or "").upper()
    if abbr in TEAM_RU:
        return TEAM_RU[abbr]
    return (abbr, "🏀")

# ---------- CACHE I/O ----------
def _load_json(path: str, default):
    if not os.path.exists(path):
        return default
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

# ---------- RU NAME RESOLVER (SURNAME ONLY) ----------
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

def _sportsru_from_profile(url: str) -> str | None:
    try:
        r = S.get(url, timeout=20)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        h = soup.find(["h1","h2"])
        if not h: return None
        full = " ".join(h.get_text(" ", strip=True).split())
        parts = [p for p in re.split(r"\s+", full) if p]
        if len(parts) >= 2:
            # фамилия — последнее слово (обрабатывает «Александер-Уокер», т.к. дефис сохранится)
            return parts[-1]
    except Exception:
        return None
    return None

def _sportsru_search(first: str, last: str) -> str | None:
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

# Частичные исключения (фамилия -> русская фамилия)
EXCEPT_LAST = {
    "Ingram":"Ингрэм","Barrett":"Барретт","Antetokounmpo":"Адетокумбо","Anthony":"Энтони",
    "Wagner":"Вагнер","Bane":"Бэйн","Young":"Янг","Alexander-Walker":"Александер-Уокер",
    "Brunson":"Брансон","Towns":"Таунс","Brown":"Браун","Hauser":"Хаузер","Thomas":"Томас",
    "Porter":"Портер","Mitchell":"Митчелл","Allen":"Аллен","Durant":"Дюрэнт","Sengun":"Шенгюн",
    "Cunningham":"Каннингем","Thompson":"Томпсон","Jackson Jr.":"Джексон-младший","Jackson":"Джексон",
    "Adebayor":"Адебайо","Adebayo":"Адебайо","Jovic":"Йович","Williamson":"Уильямсон","Murphy":"Мерфи",
    "Wembanyama":"Вембаньяма","Vassell":"Васселл","Davis":"Дэвис","Flagg":"Флэгг","George":"Джордж",
    "Johnson":"Джонсон","Doncic":"Дончич","Dončić":"Дончич","Reaves":"Ривз","Edwards":"Эдвардс",
    "Randle":"Рэндл","Avdija":"Авдия","Grant":"Грант","Curry":"Карри","Kuminga":"Куминга",
    "LaVine":"Лавин","Monk":"Монк","Markkanen":"Маркканен","Harden":"Харден","Leonard":"Леонард",
    "Brooks":"Брукс","Booker":"Букер","Allen Jr.":"Аллен",
    "Jokic":"Йокич","Embiid":"Эмбиид","Tatum":"Тэйтум","Lillard":"Лиллард","Morant":"Морант",
    "Irving":"Ирвинг","James":"Джеймс","Westbrook":"Уэстбрук","Paul":"Пол","Butler":"Батлер",
    "DeRozan":"Дерозан","Siakam":"Сиакам","VanVleet":"Ванвлит","Holiday":"Холидэй","Middleton":"Миддлтон",
    "Lopez":"Лопес","Gobert":"Гобер","Porzingis":"Порзингис","Gilgeous-Alexander":"Гилджес-Александер",
}

def _queue_pending(pid: str, first: str, last: str):
    if not pid or pid in _session_pending_ids:
        return
    if pid in RU_MAP:
        return
    for it in RU_PENDING:
        if it.get("id") == pid:
            return
    RU_PENDING.append({"id": pid, "first": first, "last": last})
    _session_pending_ids.add(pid)

def resolve_ru_surname(first: str, last: str, athlete_id: str) -> str:
    # 0) кэш по id
    if athlete_id and athlete_id in RU_MAP:
        return RU_MAP[athlete_id]

    # нормализуем last (с учётом Jr./III)
    last_clean = last.strip()
    if last_clean in {"Jr.","Jr", "III", "II"}:
        last_clean = f"{first.strip()} {last_clean}"

    # 1) sports.ru (slug)
    url = _sportsru_try_profile(first, last)
    if url:
        ru = _sportsru_from_profile(url)
        if ru:
            return ru

    # 2) sports.ru (поиск)
    ru = _sportsru_search(first, last)
    if ru:
        return ru

    # 3) исключения
    if last_clean in EXCEPT_LAST:
        return EXCEPT_LAST[last_clean]
    if last in EXCEPT_LAST:
        return EXCEPT_LAST[last]

    # 4) латиница (в очередь на уточнение)
    if athlete_id:
        _queue_pending(athlete_id, first, last)
    return last or first  # временно латиница

# ---------- ESPN HELPERS ----------
def fetch_scoreboard(day: date) -> list[dict]:
    dates = day.strftime("%Y%m%d")
    j = _get_json(f"{ESPN_BASE}/scoreboard?dates={dates}")
    events = j.get("events") or []
    out = []
    for ev in events:
        try:
            st = ev.get("status", {}).get("type", {}).get("completed", False)
            if not st:
                continue  # финалы
            comp = (ev.get("competitions") or [])[0]
            competitors = comp.get("competitors") or []
            game = {"eventId": ev.get("id"), "competitors": []}
            for c in competitors:
                team = c.get("team") or {}
                abbr = team.get("abbreviation")
                # ESPN иногда отдаёт "GS" вместо "GSW" (редко) — нормализатор
                if abbr == "GS": abbr = "GSW"
                score = int(float(c.get("score", 0)))
                win = c.get("winner", False)
                rec = ""
                for recobj in c.get("records") or []:
                    if recobj.get("type") == "total" and recobj.get("summary"):
                        rec = recobj["summary"]  # "2-0"
                game["competitors"].append({
                    "abbr": abbr,
                    "score": score,
                    "winner": bool(win),
                    "record": rec or "",
                    "teamId": str(team.get("id") or ""),
                })
            if len(game["competitors"]) == 2:
                out.append(game)
        except Exception as e:
            log("[scoreboard parse] skip:", e)
    return out

def fetch_boxscore(event_id: str) -> dict:
    j = _get_json(f"{ESPN_BASE}/boxscore?event={event_id}")
    return j or {}

def parse_players_from_box(box: dict) -> dict:
    """
    Возвращает словарь teamId -> список игроков:
    [{"id","first","last","pts","reb","ast","stl","blk"}]
    """
    out = {}
    for t in (box.get("players") or []):
        team = t.get("team") or {}
        tid = str(team.get("id") or "")
        arr = []
        for grp in (t.get("statistics") or []):
            # grp["athletes"] содержит игроков с полями stats + athlete
            for a in (grp.get("athletes") or []):
                ath = a.get("athlete") or {}
                pid = str(ath.get("id") or "")
                name = ath.get("displayName") or ""
                # разбор имени
                parts = [p for p in re.split(r"\s+", name.strip()) if p]
                first = parts[0] if parts else ""
                last = " ".join(parts[1:]) if len(parts) > 1 else parts[0] if parts else ""

                # базовые числа
                stats_map = {}
                for k, v in (a.get("stats") or {}).items():
                    stats_map[k.lower()] = v
                for k, v in (ath.get("stats") or {}).items():
                    stats_map.setdefault(k.lower(), v)

                def iget(*keys, default=0):
                    for k in keys:
                        if k in stats_map:
                            try:
                                return int(stats_map[k])
                            except Exception:
                                try:
                                    return int(float(stats_map[k]))
                                except Exception:
                                    pass
                    return default

                pts = iget("points","pts")
                reb = iget("rebounds","reb","totreb","reboundstotal")
                ast = iget("assists","ast")
                stl = iget("steals","stl")
                blk = iget("blocks","blk")

                # пропускаем тех, кто вообще не играл (у некоторых нет очков/минут)
                if all(v in (None, 0) for v in [pts, reb, ast, stl, blk]):
                    continue

                arr.append({
                    "id": pid, "first": first, "last": last,
                    "pts": int(pts or 0), "reb": int(reb or 0),
                    "ast": int(ast or 0), "stl": int(stl or 0), "blk": int(blk or 0),
                })
        # Сведём дубликаты (берём максимумы по показателям)
        merged = {}
        for p in arr:
            if p["id"] not in merged:
                merged[p["id"]] = p
            else:
                m = merged[p["id"]]
                for k in ("pts","reb","ast","stl","blk"):
                    m[k] = max(m[k], p[k])
        out[tid] = list(merged.values())
    return out

# ---------- PLAYER LINE BUILD ----------
def fmt_stat_line_ru(p: dict, ru_surname: str) -> str:
    pts, reb, ast, stl, blk = p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]
    parts = [f"{ru_surname}: {pts} {ru_plural(pts, ('очко','очка','очков'))}"]
    if reb >= 5:
        parts.append(f"{reb} {ru_plural(reb, ('подбор','подбора','подборов'))}")
    if ast >= 5:
        parts.append(f"{ast} {ru_plural(ast, ('передача','передачи','передач'))}")
    if stl >= 4:
        parts.append(f"{stl} {ru_plural(stl, ('перехват','перехвата','перехватов'))}")
    if blk >= 4:
        parts.append(f"{blk} {ru_plural(blk, ('блок-шот','блок-шота','блок-шотов'))}")
    return ", ".join(parts)

# ---------- GAME BLOCK ----------
def build_game_block(game: dict) -> str:
    comp = game["competitors"]
    if len(comp) != 2:
        return ""
    a, b = comp[0], comp[1]
    # сопоставим эмодзи/название
    name_a, emo_a = team_ru_and_emoji(a["abbr"])
    name_b, emo_b = team_ru_and_emoji(b["abbr"])

    def score_line(cname, cemo, score, record, is_winner):
        s = f"<b>{score}</b>" if is_winner else f"{score}"
        rec = f" ({record})" if record else ""
        return f"{cemo} {cname}: {s}{rec}"

    head = score_line(name_a, emo_a, a["score"], a["record"], a["winner"]) + "\n" + \
           score_line(name_b, emo_b, b["score"], b["record"], b["winner"]) + "\n\n"

    # игроки
    box = fetch_boxscore(game["eventId"])
    players_by_team = parse_players_from_box(box)

    body_lines = []

    # для каждой команды: берём ТОП-3 по очкам
    def team_players_lines(team_obj):
        tid = team_obj["teamId"]
        plist = players_by_team.get(tid, [])
        plist.sort(key=lambda x: (x["pts"], x["reb"], x["ast"]), reverse=True)
        top = plist[:3]
        lines = []
        for p in top:
            ru_surname = resolve_ru_surname(p["first"], p["last"], p["id"])
            line = fmt_stat_line_ru(p, ru_surname)
            lines.append(line)
        return lines

    body_lines += team_players_lines(a)
    if body_lines: body_lines.append("")  # разделитель между командами
    body_lines += team_players_lines(b)

    return head + "\n".join([l for l in body_lines if l.strip()])

# ---------- POST ----------
def build_post(day: date) -> str:
    games = fetch_scoreboard(day)
    title = f"НБА • {ru_date(day)} • {len(games)} {ru_plural(len(games), ('матч','матча','матчей'))}\n"
    title += "Результаты надёжно спрятаны 👇\n"
    title += "–––––––––––––––––––––––\n\n"

    blocks = []
    for i, g in enumerate(games, 1):
        try:
            blk = build_game_block(g)
            if blk.strip():
                blocks.append(blk)
            else:
                blocks.append("— данные по матчу временно недоступны")
        except Exception as e:
            log("[game block error]", e)
            blocks.append("— данные по матчу временно недоступны")
        if i < len(games):
            blocks.append("–––––––––––––––––––––––")

    return (title + "\n".join(blocks)).strip()

# ---------- TELEGRAM ----------
def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    MAX = 3500
    t = text
    parts = []
    while t:
        if len(t) <= MAX:
            parts.append(t); break
        cut = t.rfind("\n\n", 0, MAX)
        if cut == -1: cut = MAX
        parts.append(t[:cut]); t = t[cut:].lstrip()

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

# ---------- MAIN ----------
if __name__ == "__main__":
    try:
        # загрузим кэши БЕЗ переназначения глобальных переменных
        loaded_map = _load_json(RU_MAP_PATH, {})
        loaded_pending = _load_json(RU_PENDING_PATH, [])

        RU_MAP.clear()
        RU_MAP.update(loaded_map)

        RU_PENDING.clear()
        RU_PENDING.extend(loaded_pending)

        day = pick_report_date()
        text = build_post(day)
        tg_send(text)

        # сохраним очереди (если появились новые)
        _save_json(RU_PENDING_PATH, RU_PENDING)
        _save_json(RU_MAP_PATH, RU_MAP)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
