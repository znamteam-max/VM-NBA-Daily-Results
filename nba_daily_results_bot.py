#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU)
• Один пост на весь день.
• В заголовке дата и число матчей.
• По каждому матчу:
  – Логотип-эмодзи рядом с названием и счётом (не фото).
  – Отметка овертайма: (ОТ), (2ОТ) и т.д.
  – Ниже 1–2 «выдающихся» игрока каждой команды:
      ≥30 очков ИЛИ дабл-дабл (PTS/REB/AST) ИЛИ ≥15 REB ИЛИ ≥12 AST
      ИЛИ ≥4 STL / ≥4 BLK.
    Показываем очки всегда; REB/AST только если ≥5; STL/BLK только если ≥4.
  – Для BKN всегда включаем Дёмина (если играл), для MIA — Голдина; выделяем жирным.
• Источники: ESPN API (scoreboard/boxscore) + sports.ru для русских фамилий.
"""

import os, sys, re, json, time, unicodedata
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# --------- ENV ----------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# --------- ESPN ----------
ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"  # scoreboard/boxscore

# --------- sports.ru ----------
SPORTS_RU   = "https://www.sports.ru"
SRU_PERSON  = SPORTS_RU + "/basketball/person/"
SRU_PLAYER  = SPORTS_RU + "/basketball/player/"
SRU_SEARCH  = SPORTS_RU + "/search/?q="

# --------- CACHE ----------
RU_MAP_PATH     = "ru_map_nba.json"      # { athleteId: "Фамилия" }
RU_PENDING_PATH = "ru_pending_nba.json"  # [{ id, first, last }]

RU_MAP: dict[str, str] = {}
RU_PENDING: list[dict] = []
_session_pending_ids: set[str] = set()

# --------- RUS DATES ----------
RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def log(*a): print(*a, file=sys.stderr)

# --------- HTTP ----------
def make_session():
    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.6,
              status_forcelist=[429,500,502,503,504],
              allowed_methods=["GET","POST"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({
        "User-Agent": "NBA-DailyResultsBot/2.0 (+espn; sports.ru resolver)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
    })
    return s
S = make_session()

def _get_json(url: str) -> dict:
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        log("HTTP", r.status_code, url[:160])
        return {}
    try:
        return r.json()
    except Exception:
        return {}

# --------- DATE PICK ----------
def pick_report_date() -> date:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return (now_et.date() - timedelta(days=1)) if now_et.hour < 8 else now_et.date()

def pick_candidate_days() -> list[date]:
    base = pick_report_date()
    return [base, base - timedelta(days=1), base - timedelta(days=2)]

# --------- TEAM NAMES + EMOJI (компактные «логотипы») ----------
TEAM_RU = {
    "ATL":"Атланта","BOS":"Бостон","BKN":"Бруклин","NY":"Нью-Йорк","NYK":"Нью-Йорк","PHI":"Филадельфия",
    "TOR":"Торонто","CHI":"Чикаго","CLE":"Кливленд","DET":"Детройт","IND":"Индиана","MIL":"Милуоки",
    "DEN":"Денвер","MIN":"Миннесота","OKC":"Оклахома-Сити","POR":"Портленд","UTA":"Юта","UTAH":"Юта",
    "GS":"Голден Стэйт","GSW":"Голден Стэйт","LAC":"Клипперс","LAL":"Лейкерс","PHX":"Финикс","SAC":"Сакраменто",
    "MIA":"Майами","ORL":"Орландо","DAL":"Даллас","HOU":"Хьюстон","MEM":"Мемфис","NO":"Новый Орлеан",
    "NOP":"Новый Орлеан","SA":"Сан-Антонио","SAS":"Сан-Антонио","WSH":"Вашингтон","WAS":"Вашингтон",
}
# Эмодзи-замены «логотипов» (компактные, не фото). При желании их можно заменить на кастомные из набора got_ball_team.
TEAM_EMOJI = {
    "ATL":"🦅","BOS":"☘️","BKN":"🕸️","NY":"🗽","NYK":"🗽","PHI":"🔔",
    "TOR":"🦖","CHI":"🐂","CLE":"🛡️","DET":"🔧","IND":"💫","MIL":"🦌",
    "DEN":"⛏️","MIN":"🐺","OKC":"⚡","POR":"🧭","UTA":"🎷","UTAH":"🎷",
    "GS":"🗡️","GSW":"🗡️","LAC":"✂️","LAL":"⭐","PHX":"☀️","SAC":"👑",
    "MIA":"🔥","ORL":"✨","DAL":"🐎","HOU":"🚀","MEM":"🐻","NO":"🪶",
    "NOP":"🪶","SA":"🪙","SAS":"🪙","WSH":"🧙","WAS":"🧙",
}
def team_ru(abbr: str) -> str:
    return TEAM_RU.get((abbr or "").upper(), (abbr or ""))
def team_emoji(abbr: str) -> str:
    return TEAM_EMOJI.get((abbr or "").upper(), "🏀")

# --------- CACHE I/O ----------
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

# --------- sports.ru resolver ----------
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
    "Demin":"Дёмин","Goldin":"Голдин",
}

def _queue_pending(pid: str, first: str, last: str):
    if not pid or pid in _session_pending_ids or pid in RU_MAP: return
    for it in RU_PENDING:
        if it.get("id") == pid: return
    RU_PENDING.append({"id": pid, "first": first, "last": last})
    _session_pending_ids.add(pid)

def resolve_ru_surname(first: str, last: str, athlete_id: str) -> str:
    if athlete_id and athlete_id in RU_MAP:
        return RU_MAP[athlete_id]
    last_clean = last.strip()
    if last_clean in {"Jr.","Jr","III","II"}:
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

    if last_clean in EXCEPT_LAST: ru = EXCEPT_LAST[last_clean]
    elif last in EXCEPT_LAST:    ru = EXCEPT_LAST[last]
    else:                        ru = last or first

    if athlete_id: _queue_pending(athlete_id, first, last)
    return ru

# --------- ESPN helpers ----------
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
            if not (completed or state in {"post","final"}):
                continue
            comp = (ev.get("competitions") or [])[0]
            competitors = comp.get("competitors") or []

            status_comp = (comp.get("status") or {}).get("type") or {}
            short = (status_comp.get("shortDetail") or t.get("shortDetail") or "").lower()
            ot_label = ""
            if "ot" in short:
                m = re.search(r'(\d+)\s*ot', short) or re.search(r'(\d)ot', short)
                ot_label = f" ({int(m.group(1))}ОТ)" if m else " (ОТ)"

            game = {"eventId": ev.get("id"), "competitors": [], "ot": ot_label}
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
    return _get_json(f"{ESPN_BASE}/boxscore?event={event_id}") or {}

def _to_int(x, default=0) -> int:
    if x is None: return default
    if isinstance(x, (int, float)): return int(x)
    s = str(x).strip()
    m = re.search(r"-?\d+", s)
    return int(m.group(0)) if m else default

def parse_players_from_box(box: dict) -> dict:
    """
    teamId -> [{"id","first","last","pts","reb","ast","stl","blk"}]
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

# --------- highlights & format ----------
def _flame(pts:int, reb:int, ast:int, stl:int, blk:int) -> str:
    dd = sum(v>=10 for v in (pts,reb,ast))
    td = dd >= 3
    if pts >= 35 or td or (pts>=30 and dd>=2):
        return " 🔥"
    return ""

def fmt_stat_line_ru(p: dict, ru_surname: str, bold: bool=False) -> str:
    pts, reb, ast, stl, blk = p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]
    name = f"<b>{ru_surname}</b>" if bold else ru_surname
    parts = [f"{name}: {pts} {ru_plural(pts, ('очко','очка','очков'))}"]
    if reb >= 5: parts.append(f"{reb} {ru_plural(reb, ('подбор','подбора','подборов'))}")
    if ast >= 5: parts.append(f"{ast} {ru_plural(ast, ('передача','передачи','передач'))}")
    if stl >= 4: parts.append(f"{stl} {ru_plural(stl, ('перехват','перехвата','перехватов'))}")
    if blk >= 4: parts.append(f"{blk} {ru_plural(blk, ('блок-шот','блок-шота','блок-шотов'))}")
    return ", ".join(parts) + _flame(pts,reb,ast,stl,blk)

def is_highlight(p: dict) -> bool:
    pts, reb, ast, stl, blk = p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]
    dd = sum(v>=10 for v in (pts,reb,ast))
    if pts >= 30: return True
    if dd >= 2:  return True
    if reb >= 15 or ast >= 12: return True
    if stl >= 4 or blk >= 4:   return True
    return False

def select_highlights(players: list[dict], abbr: str) -> list[tuple[dict,bool]]:
    """
    Возвращает 1–2 игрока.
    • Если есть «выдающиеся» — берём их (до 2).
    • Если нет — хотя бы одного лучшего по (PTS, REB+AST, STL+BLK).
    • Для BKN добавляем Дёмина (если в бокскоре), для MIA — Голдина. Выделяем жирным.
    """
    if not players: return []
    want_special = "demin" if abbr=="BKN" else ("goldin" if abbr=="MIA" else None)

    def key_score(p):
        return (p.get("pts",0), p.get("reb",0)+p.get("ast",0), p.get("stl",0)+p.get("blk",0))

    players_sorted = sorted(players, key=key_score, reverse=True)
    picks = [p for p in players_sorted if is_highlight(p)][:2]
    if not picks:
        picks = [players_sorted[0]]

    spec = None
    if want_special:
        spec = next((p for p in players_sorted if p.get("last","").strip().lower().endswith(want_special)), None)
        if spec and all(spec["id"] != x["id"] for x in picks):
            if len(picks) == 2:
                picks[1] = spec
            else:
                picks.append(spec)

    out = []
    for p in picks:
        out.append((p, bool(spec and p["id"] == spec["id"])))
    return out

# --------- GAME → text block ----------
SEP = "–––––––––––––––––––––––"

def build_game_block(game: dict) -> str:
    comp = game["competitors"]
    if len(comp) != 2: return ""
    a, b = comp[0], comp[1]

    def line(c, add_ot: bool):
        emo = team_emoji(c["abbr"])
        s   = f"<b>{c['score']}</b>" if c["winner"] else f"{c['score']}"
        rec = f" ({c['record']})" if c["record"] else ""
        ot  = game["ot"] if add_ot and game.get("ot") else ""
        return f"{emo} {team_ru(c['abbr'])}: {s}{rec}{ot}"

    head = line(a, add_ot=False) + "\n" + line(b, add_ot=True) + "\n"

    # игроки
    box = fetch_boxscore(game["eventId"])
    players_by_team = parse_players_from_box(box)

    lines = []
    for c in (a, b):
        arr = players_by_team.get(c["teamId"], [])
        for p, bold in select_highlights(arr, c["abbr"]):
            ru = resolve_ru_surname(p.get("first",""), p.get("last",""), p.get("id",""))
            lines.append(fmt_stat_line_ru(p, ru, bold))
        if lines and not lines[-1].endswith("\n\n"):
            lines.append("")  # пустая строка между командами

    return head + ("\n".join(l for l in lines if l.strip())).strip()

# --------- POST (одним сообщением, с безопасным делением на части < 4096)
def build_post_text() -> str:
    chosen_day = None
    games = []
    for d in pick_candidate_days():
        games = fetch_scoreboard(d)
        if games:
            chosen_day = d
            break
    if not chosen_day:
        chosen_day = pick_report_date()

    title = f"НБА • {ru_date(chosen_day)} • {len(games)} {ru_plural(len(games), ('матч','матча','матчей'))}\n" \
            "Результаты надёжно спрятаны 👇\n" \
            f"{SEP}\n\n"

    blocks = []
    for i, g in enumerate(games, 1):
        blk = build_game_block(g)
        blocks.append(blk.strip())
        if i < len(games):
            blocks.append(f"\n{SEP}\n")

    return (title + "\n".join(blocks)).strip()

# --------- TELEGRAM (одним сообщением; при переполнении — разбиваем по разделителям)
def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    MAX = 3800  # запас под HTML и эмодзи
    parts = []
    t = text

    # режем по разделителю блоков
    while len(t) > MAX:
        cut = t.rfind(SEP, 0, MAX)
        if cut == -1:
            cut = t.rfind("\n\n", 0, MAX)
        if cut == -1:
            cut = MAX
        parts.append(t[:cut].rstrip())
        t = t[cut:].lstrip()
    parts.append(t)

    # отправляем как одну «цепочку»: первое сообщение — основная часть, остальные — продолжение
    first = True
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
        first = False

# --------- MAIN ----------
if __name__ == "__main__":
    try:
        loaded_map = _load_json(RU_MAP_PATH, {})
        loaded_pending = _load_json(RU_PENDING_PATH, [])
        if isinstance(loaded_map, dict):
            RU_MAP.clear(); RU_MAP.update(loaded_map)
        if isinstance(loaded_pending, list):
            RU_PENDING.clear(); RU_PENDING.extend(loaded_pending)

        text = build_post_text()
        tg_send(text)

        _save_json(RU_PENDING_PATH, RU_PENDING)
        _save_json(RU_MAP_PATH, RU_MAP)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
