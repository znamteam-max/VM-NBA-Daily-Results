#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU), ESPN + sports.ru

Главные изменения:
• Правильные эндпойнты ESPN: https://site.api.espn.com/apis/site/v2/sports/basketball/nba/...
• Фильтр финалов: status.type.completed == true ИЛИ state in {"post","final"}
• Boxscore: разбор через labels[] ↔ stats[] (ESPN отдаёт массивы), собираем PTS/REB/AST/STL/BLK
• Русские фамилии — через sports.ru (слуг + поиск) + кэш ru_map_nba.json / очередь ru_pending_nba.json
• Спец-правило: если команда BKN — Дёмин жирным; MIA — Голдин жирным (если играли)

Источник про ESPN endpoints: site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard (+boxscore/summary). 
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

# ---------- ESPN (исправлено) ----------
ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
# scoreboard?dates=YYYYMMDD
# boxscore?event={eventId}

# ---------- SPORTS.RU ----------
SPORTS_RU = "https://www.sports.ru"
SRU_PERSON = SPORTS_RU + "/basketball/person/"
SRU_PLAYER = SPORTS_RU + "/basketball/player/"
SRU_SEARCH = SPORTS_RU + "/search/?q="

# ---------- CACHE FILES ----------
RU_MAP_PATH     = "ru_map_nba.json"      # { athleteId(str): "Фамилия" }
RU_PENDING_PATH = "ru_pending_nba.json"  # [{id, first, last}]

RU_MAP: dict[str, str] = {}
RU_PENDING: list[dict] = []
_session_pending_ids: set[str] = set()

# ---------- RUS DATES ----------
RU_MONTHS = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def ru_plural(n: int, forms: tuple[str, str, str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def log(*a): print(*a, file=sys.stderr)

# ---------- HTTP ----------
def make_session():
    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.6,
              status_forcelist=[429,500,502,503,504],
              allowed_methods=["GET","POST"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({
        "User-Agent": "NBA-DailyResultsBot/1.3 (+espn; sports.ru resolver)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
    })
    return s
S = make_session()

def _get_json(url: str) -> dict:
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        log("HTTP", r.status_code, url[:120])
        return {}
    try:
        return r.json()
    except Exception:
        return {}

# ---------- PICK DATE(S) ----------
def pick_report_date() -> date:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    # утром по ET берём вчера, иначе — сегодня
    return (now_et.date() - timedelta(days=1)) if now_et.hour < 8 else now_et.date()

def pick_candidate_days() -> list[date]:
    base = pick_report_date()
    return [base, base - timedelta(days=1), base - timedelta(days=2)]

# ---------- TEAMS / EMOJIS ----------
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
    if abbr == "GS": abbr = "GSW"  # редкая аномалия в ESPN
    return TEAM_RU.get(abbr, (abbr, "🏀"))

# ---------- CACHE I/O ----------
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

# ---------- RU SURNAME RESOLVER ----------
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
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.text, "html.parser")
        h = soup.find(["h1","h2"])
        if not h: return None
        full = " ".join(h.get_text(" ", strip=True).split())
        parts = [p for p in re.split(r"\s+", full) if p]
        if len(parts) >= 2:
            return parts[-1]  # фамилия — последнее слово
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

# исключения (фамилия -> русская)
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
    # спец-правило
    "Demin":"Дёмин","Goldin":"Голдин",
}

def _queue_pending(pid: str, first: str, last: str):
    if not pid or pid in _session_pending_ids: return
    if pid in RU_MAP: return
    for it in RU_PENDING:
        if it.get("id") == pid: return
    RU_PENDING.append({"id": pid, "first": first, "last": last})
    _session_pending_ids.add(pid)

def resolve_ru_surname(first: str, last: str, athlete_id: str) -> str:
    if athlete_id and athlete_id in RU_MAP:
        return RU_MAP[athlete_id]

    last_clean = last.strip()
    if last_clean in {"Jr.","Jr", "III", "II"}:
        last_clean = f"{first.strip()} {last_clean}"

    url = _sportsru_try_profile(first, last)
    if url:
        ru = _sportsru_from_profile(url)
        if ru:
            if athlete_id: RU_MAP[athlete_id] = ru
            return ru

    ru = _sportsru_search(first, last)
    if ru:
        if athlete_id: RU_MAP[athlete_id] = ru
        return ru

    if last_clean in EXCEPT_LAST: 
        ru = EXCEPT_LAST[last_clean]
    elif last in EXCEPT_LAST:
        ru = EXCEPT_LAST[last]
    else:
        ru = last or first

    if athlete_id: _queue_pending(athlete_id, first, last)
    return ru

# ---------- ESPN HELPERS ----------
def fetch_scoreboard(day: date) -> list[dict]:
    dates = day.strftime("%Y%m%d")
    j = _get_json(f"{ESPN_BASE}/scoreboard?dates={dates}")
    events = j.get("events") or []
    out = []
    for ev in events:
        try:
            t = (ev.get("status") or {}).get("type") or {}
            completed = bool(t.get("completed"))
            state = str(t.get("state") or "").lower()
            if not (completed or state in {"post", "final"}):
                continue  # только финалы

            comp = (ev.get("competitions") or [])[0]
            competitors = comp.get("competitors") or []
            game = {"eventId": ev.get("id"), "competitors": []}
            for c in competitors:
                team = c.get("team") or {}
                abbr = (team.get("abbreviation") or "").upper()
                if abbr == "GS": abbr = "GSW"
                try:
                    score = int(float(c.get("score", 0)))
                except Exception:
                    score = 0
                win = bool(c.get("winner", False))
                rec = ""
                for recobj in c.get("records") or []:
                    if recobj.get("type") == "total" and recobj.get("summary"):
                        rec = recobj["summary"]  # "2-0"
                game["competitors"].append({
                    "abbr": abbr,
                    "score": score,
                    "winner": win,
                    "record": rec or "",
                    "teamId": str(team.get("id") or ""),
                })
            if len(game["competitors"]) == 2:
                out.append(game)
        except Exception as e:
            log("[scoreboard parse] skip:", e)
    return out

def fetch_boxscore(event_id: str) -> dict:
    # тот же «site.api», что и scoreboard
    return _get_json(f"{ESPN_BASE}/boxscore?event={event_id}") or {}

def _to_int(x, default=0) -> int:
    if x is None: return default
    if isinstance(x, (int, float)): return int(x)
    s = str(x).strip()
    # значения типа "+5" или "12-20" — оставим только первую целую часть
    m = re.search(r"-?\d+", s)
    return int(m.group(0)) if m else default

def parse_players_from_box(box: dict) -> dict:
    """
    Возвращает словарь teamId -> список игроков:
    [{"id","first","last","pts","reb","ast","stl","blk"}]
    ESPN boxscore: для каждой команды есть statistics[] c полем labels[] и athletes[], где athlete.stats — массив строк.
    """
    out = {}
    teams = (box.get("players") or box.get("boxscore", {}).get("players") or [])
    for t in teams:
        team = t.get("team") or {}
        tid = str(team.get("id") or "")
        players: dict[str, dict] = {}

        for grp in (t.get("statistics") or []):
            labels = [str(x).strip().lower() for x in (grp.get("labels") or [])]
            for a in (grp.get("athletes") or []):
                ath = a.get("athlete") or {}
                pid = str(ath.get("id") or "")
                if not pid: continue

                name = ath.get("displayName") or ath.get("shortName") or ""
                parts = [p for p in re.split(r"\s+", name.strip()) if p]
                first = parts[0] if parts else ""
                last = " ".join(parts[1:]) if len(parts) > 1 else (parts[0] if parts else "")

                stats_list = a.get("stats") or []
                statmap = {}
                n = min(len(labels), len(stats_list))
                for i in range(n):
                    statmap[labels[i]] = stats_list[i]

                # некоторые ответы ESPN также дублируют числа в athlete.stats (dict); аккуратно дополним
                for k, v in (ath.get("stats") or {}).items():
                    k2 = str(k).strip().lower()
                    if k2 not in statmap:
                        statmap[k2] = v

                pts = _to_int(statmap.get("pts") or statmap.get("points") or 0)
                reb = _to_int(statmap.get("reb") or statmap.get("rebs") or statmap.get("rebounds") or 0)
                ast = _to_int(statmap.get("ast") or statmap.get("assists") or 0)
                stl = _to_int(statmap.get("stl") or statmap.get("steals") or 0)
                blk = _to_int(statmap.get("blk") or statmap.get("blocks") or 0)

                if pid not in players:
                    players[pid] = {"id": pid, "first": first, "last": last,
                                    "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk}
                else:
                    m = players[pid]
                    for k, v in (("pts", pts), ("reb", reb), ("ast", ast), ("stl", stl), ("blk", blk)):
                        m[k] = max(m[k], v)

        out[tid] = list(players.values())
    return out

# ---------- PLAYER LINE ----------
def _flame(pts:int, reb:int, ast:int, stl:int, blk:int) -> str:
    dbl = sum(v>=10 for v in (pts,reb,ast,stl,blk))
    if pts >= 35 or dbl >= 3 or (pts>=30 and dbl>=2):
        return " 🔥"
    return ""

def fmt_stat_line_ru(p: dict, ru_surname: str, bold_name: bool = False) -> str:
    pts, reb, ast, stl, blk = p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]
    name_part = f"<b>{ru_surname}</b>" if bold_name else ru_surname
    parts = [f"{name_part}: {pts} {ru_plural(pts, ('очко','очка','очков'))}"]
    if reb >= 5: parts.append(f"{reb} {ru_plural(reb, ('подбор','подбора','подборов'))}")
    if ast >= 5: parts.append(f"{ast} {ru_plural(ast, ('передача','передачи','передач'))}")
    if stl >= 4: parts.append(f"{stl} {ru_plural(stl, ('перехват','перехвата','перехватов'))}")
    if blk >= 4: parts.append(f"{blk} {ru_plural(blk, ('блок-шот','блок-шота','блок-шотов'))}")
    return ", ".join(parts) + _flame(pts,reb,ast,stl,blk)

# ---------- GAME BLOCK ----------
def build_game_block(game: dict) -> str:
    comp = game["competitors"]
    if len(comp) != 2: return ""
    a, b = comp[0], comp[1]
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

    def pick_two_with_special(team_obj) -> list[tuple[dict,bool]]:
        """
        Возвращает [(player, bold_flag)] (2 шт.), гарантируя:
        • если команда BKN — включаем Демина (last заканчивается на 'demin'), если он в бокскоре;
        • если команда MIA — включаем Голдина (last заканчивается на 'goldin'), если он в бокскоре.
        """
        tid = team_obj["teamId"]
        abbr = team_obj["abbr"]
        lst = players_by_team.get(tid, [])
        # сортировка по полезности: очки → (REB+AST) → +/- (если есть)
        lst.sort(key=lambda x: (x.get("pts",0), x.get("reb",0)+x.get("ast",0)), reverse=True)
        top = lst[:2]

        special_last = "demin" if abbr=="BKN" else ("goldin" if abbr=="MIA" else None)
        special_player = None
        if special_last:
            for p in lst:
                if p.get("last","").strip().lower().endswith(special_last):
                    special_player = p
                    break

        if special_player:
            if not any(sp["id"] == special_player["id"] for sp in top):
                if top:
                    top = [top[0], special_player]
                else:
                    top = [special_player]
            out = []
            for p in top:
                out.append( (p, p["id"] == special_player["id"]) )
            return out

        return [(p, False) for p in top]

    lines = []
    # команда A
    for p, bold_flag in pick_two_with_special(a):
        ru_surname = resolve_ru_surname(p.get("first",""), p.get("last",""), p.get("id",""))
        lines.append(fmt_stat_line_ru(p, ru_surname, bold_flag))
    if lines: lines.append("")  # разделение между командами
    # команда B
    for p, bold_flag in pick_two_with_special(b):
        ru_surname = resolve_ru_surname(p.get("first",""), p.get("last",""), p.get("id",""))
        lines.append(fmt_stat_line_ru(p, ru_surname, bold_flag))

    return head + "\n".join([l for l in lines if l.strip()])

# ---------- POST ----------
def build_post() -> str:
    # выберем первый день с матчами из кандидатов
    chosen_day = None
    games = []
    for d in pick_candidate_days():
        games = fetch_scoreboard(d)
        if games:
            chosen_day = d
            break
    if not chosen_day:
        chosen_day = pick_report_date()

    title = f"НБА • {ru_date(chosen_day)} • {len(games)} {ru_plural(len(games), ('матч','матча','матчей'))}\n"
    title += "Результаты надёжно спрятаны 👇\n"
    title += "–––––––––––––––––––––––\n\n"

    if not games:
        return (title.rstrip())

    blocks = []
    for i, g in enumerate(games, 1):
        try:
            blk = build_game_block(g)
            blocks.append(blk if blk.strip() else "— данные по матчу временно недоступны")
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
        # загрузим кэши
        loaded_map = _load_json(RU_MAP_PATH, {})
        loaded_pending = _load_json(RU_PENDING_PATH, [])
        if isinstance(loaded_map, dict):
            RU_MAP.clear(); RU_MAP.update(loaded_map)
        if isinstance(loaded_pending, list):
            RU_PENDING.clear(); RU_PENDING.extend(loaded_pending)

        text = build_post()
        tg_send(text)

        _save_json(RU_PENDING_PATH, RU_PENDING)
        _save_json(RU_MAP_PATH, RU_MAP)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
