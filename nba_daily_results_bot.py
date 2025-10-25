#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU), с логотипами, овертаймами и выдающимися игроками.

Источники:
• Матчи/бокскор: ESPN (public) — https://site.api.espn.com/apis/site/v2/sports/basketball/nba/...
• Русские фамилии: sports.ru (профиль/поиск) + кэш ru_map_nba.json / ru_pending_nba.json
• Логотипы: ESPN CDN — https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/{code}.png

Параметры окружения:
• TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID — куда отправлять
• NBA_LOGOS_MODE = "photo" (по умолчанию) | "text" — отправлять фото с логотипами или текст
"""

import os, sys, re, json, time, unicodedata, io
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# --- optional: Pillow для коллажа логотипов ---
try:
    from PIL import Image
    PIL_OK = True
except Exception:
    PIL_OK = False

# ---------- ENV ----------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()
LOGOS_MODE = (os.getenv("NBA_LOGOS_MODE", "photo") or "photo").lower()  # "photo" | "text"

# ---------- ESPN ----------
ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"  # scoreboard/boxscore

# ---------- SPORTS.RU ----------
SPORTS_RU   = "https://www.sports.ru"
SRU_PERSON  = SPORTS_RU + "/basketball/person/"
SRU_PLAYER  = SPORTS_RU + "/basketball/player/"
SRU_SEARCH  = SPORTS_RU + "/search/?q="

# ---------- CACHE ----------
RU_MAP_PATH     = "ru_map_nba.json"      # { athleteId: "Фамилия" }
RU_PENDING_PATH = "ru_pending_nba.json"  # [{ id, first, last }]

RU_MAP: dict[str, str] = {}
RU_PENDING: list[dict] = []
_session_pending_ids: set[str] = set()

# ---------- RUS DATES ----------
RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
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
    s.headers.update({"User-Agent": "NBA-DailyResultsBot/1.4 (+espn; sports.ru resolver)", "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6"})
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

# ---------- DATE PICK ----------
def pick_report_date() -> date:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return (now_et.date() - timedelta(days=1)) if now_et.hour < 8 else now_et.date()

def pick_candidate_days() -> list[date]:
    base = pick_report_date()
    return [base, base - timedelta(days=1), base - timedelta(days=2)]

# ---------- TEAMS / RUS NAMES ----------
TEAM_RU = {
    # Включаем все варианты аббревиатур ESPN, которые встречаются на табло
    "ATL": ("Атланта"),
    "BOS": ("Бостон"),
    "BKN": ("Бруклин"),
    "NY":  ("Нью-Йорк"),
    "NYK": ("Нью-Йорк"),
    "PHI": ("Филадельфия"),
    "TOR": ("Торонто"),
    "CHI": ("Чикаго"),
    "CLE": ("Кливленд"),
    "DET": ("Детройт"),
    "IND": ("Индиана"),
    "MIL": ("Милуоки"),
    "DEN": ("Денвер"),
    "MIN": ("Миннесота"),
    "OKC": ("Оклахома-Сити"),
    "POR": ("Портленд"),
    "UTA": ("Юта"),
    "UTAH":("Юта"),
    "GS":  ("Голден Стэйт"),
    "GSW": ("Голден Стэйт"),
    "LAC": ("Клипперс"),
    "LAL": ("Лейкерс"),
    "PHX": ("Финикс"),
    "SAC": ("Сакраменто"),
    "MIA": ("Майами"),
    "ORL": ("Орландо"),
    "DAL": ("Даллас"),
    "HOU": ("Хьюстон"),
    "MEM": ("Мемфис"),
    "NO":  ("Новый Орлеан"),
    "NOP": ("Новый Орлеан"),
    "SA":  ("Сан-Антонио"),
    "SAS": ("Сан-Антонио"),
    "WSH": ("Вашингтон"),
    "WAS": ("Вашингтон"),
}
def team_ru_name(abbr: str) -> str:
    return TEAM_RU.get((abbr or "").upper(), (abbr or ""))

# ЛОГО-коды ESPN CDN (см. /i/teamlogos/nba/500/{code}.png)
LOGO_CODE = {
    "ATL":"atl","BOS":"bos","BKN":"bkn","NY":"ny","NYK":"ny","PHI":"phi","TOR":"tor",
    "CHI":"chi","CLE":"cle","DET":"det","IND":"ind","MIL":"mil",
    "DEN":"den","MIN":"min","OKC":"okc","POR":"por","UTA":"utah","UTAH":"utah",
    "GS":"gsw","GSW":"gsw","LAC":"lac","LAL":"lal","PHX":"phx","SAC":"sac",
    "MIA":"mia","ORL":"orl","DAL":"dal","HOU":"hou","MEM":"mem",
    "NO":"no","NOP":"no","SA":"sa","SAS":"sa","WSH":"wsh","WAS":"wsh",
}
def logo_url(abbr: str) -> str | None:
    code = LOGO_CODE.get((abbr or "").upper())
    if not code: return None
    return f"https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/{code}.png"

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

# ---------- sports.ru resolver ----------
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

# ---------- ESPN helpers ----------
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
            # определим ОТ
            ot_label = ""
            if "ot" in short:
                m = re.search(r"(\d+)\s*ot", short)
                if m:
                    n = int(m.group(1))
                    ot_label = f" ({n}ОТ)"
                else:
                    ot_label = " (ОТ)"

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

# ---------- highlights & format ----------
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
    if not players: return []
    # спец-игроки
    want_special = "demin" if abbr=="BKN" else ("goldin" if abbr=="MIA" else None)
    # сортировка важности
    def score(p):
        pts, reb, ast, stl, blk = p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]
        dd = sum(v>=10 for v in (pts,reb,ast))
        td = dd >= 3
        return (is_highlight(p), pts, reb+ast, stl+blk, td, reb, ast)
    players_sorted = sorted(players, key=score, reverse=True)
    # базовый выбор
    picks = [p for p in players_sorted if is_highlight(p)][:2]
    if not picks:
        picks = [players_sorted[0]]  # минимум один
    # добавить спец-игрока, если нужно
    if want_special:
        spec = next((p for p in players_sorted if p["last"].strip().lower().endswith(want_special)), None)
        if spec and all(spec["id"] != x["id"] for x in picks):
            if len(picks) == 2:
                picks[1] = spec
            else:
                picks.append(spec)
    # пометить жирным только спец-игрока
    out = []
    for p in picks:
        bold = bool(want_special and p is spec)
        out.append((p, bold))
    return out

# ---------- LOGO collage ----------
def fetch_logo(abbr: str) -> Image.Image | None:
    if not PIL_OK: return None
    url = logo_url(abbr)
    if not url: return None
    try:
        r = S.get(url, timeout=20)
        if r.status_code != 200: return None
        img = Image.open(io.BytesIO(r.content)).convert("RGBA")
        return img
    except Exception:
        return None

def make_pair_banner(abbr_left: str, abbr_right: str) -> bytes | None:
    if not PIL_OK: return None
    L = fetch_logo(abbr_left)
    R = fetch_logo(abbr_right)
    if not L or not R: return None
    # размеры
    H = 320
    BG = (255,255,255,0)  # прозрачный
    def fit(im: Image.Image, h=H):
        w = int(im.width * (h / im.height))
        return im.resize((w,h), Image.LANCZOS)
    Lr = fit(L); Rr = fit(R)
    gap = 40
    W = Lr.width + gap + Rr.width
    canvas = Image.new("RGBA", (W, H), BG)
    canvas.paste(Lr, (0, 0), Lr)
    canvas.paste(Rr, (Lr.width + gap, 0), Rr)
    out = io.BytesIO()
    canvas.save(out, format="PNG")
    return out.getvalue()

# ---------- GAME block → caption + (optional) photo ----------
def build_game_render(game: dict) -> dict:
    a, b = game["competitors"][0], game["competitors"][1]
    name_a, name_b = team_ru_name(a["abbr"]), team_ru_name(b["abbr"])

    def line(c, add_ot=False):
        s = f"<b>{c['score']}</b>" if c["winner"] else f"{c['score']}"
        rec = f" ({c['record']})" if c["record"] else ""
        ot = game["ot"] if add_ot and game.get("ot") else ""
        return f"{team_ru_name(c['abbr'])}: {s}{rec}{ot}"

    caption = line(a, add_ot=False) + "\n" + line(b, add_ot=True) + "\n\n"

    # игроки
    box = fetch_boxscore(game["eventId"])
    players_by_team = parse_players_from_box(box)

    lines = []
    for c in (a, b):
        arr = players_by_team.get(c["teamId"], [])
        picks = select_highlights(arr, c["abbr"])
        for p, bold in picks:
            ru = resolve_ru_surname(p.get("first",""), p.get("last",""), p.get("id",""))
            lines.append(fmt_stat_line_ru(p, ru, bold))
        if picks:
            lines.append("")  # пустая строка между командами

    caption += "\n".join(l for l in lines if l.strip())

    # рендер фото с логотипами
    photo_bytes = None
    if LOGOS_MODE == "photo" and PIL_OK:
        # чтобы слева вверхняя строка соответствовала левой картинке — ставим победителя жирным в подписи, а баннер — (a vs b)
        photo_bytes = make_pair_banner(a["abbr"], b["abbr"])
    return {"mode": ("photo" if (photo_bytes and LOGOS_MODE == "photo") else "text"),
            "caption": caption.strip(), "photo": photo_bytes}

# ---------- POST ----------
def build_post() -> tuple[str, list[dict]]:
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
            "–––––––––––––––––––––––"

    renders = []
    for g in games:
        try:
            renders.append(build_game_render(g))
        except Exception as e:
            log("[game render error]", e)
            renders.append({"mode":"text","caption":"— данные по матчу временно недоступны","photo":None})
    return title, renders

# ---------- TELEGRAM ----------
def tg_send_text(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    resp = S.post(url, json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=25)
    if resp.status_code != 200:
        raise RuntimeError(f"Telegram error {resp.status_code}: {resp.text}")
    time.sleep(0.25)

def tg_send_photo(caption: str, photo_bytes: bytes):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    files = {"photo": ("logos.png", photo_bytes, "image/png")}
    data = {"chat_id": CHAT_ID, "caption": caption, "parse_mode": "HTML"}
    resp = S.post(url, data=data, files=files, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Telegram error {resp.status_code}: {resp.text}")
    time.sleep(0.4)

def tg_send_post():
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    title, renders = build_post()
    tg_send_text(title)
    if not renders:
        return
    for i, r in enumerate(renders, 1):
        if r["mode"] == "photo" and r["photo"]:
            tg_send_photo(r["caption"], r["photo"])
        else:
            # текстовый запасной вариант + разделитель
            if i > 1:
                tg_send_text("–––––––––––––––––––––––")
            tg_send_text(r["caption"])

# ---------- MAIN ----------
if __name__ == "__main__":
    try:
        loaded_map = _load_json(RU_MAP_PATH, {})
        loaded_pending = _load_json(RU_PENDING_PATH, [])
        if isinstance(loaded_map, dict):
            RU_MAP.clear(); RU_MAP.update(loaded_map)
        if isinstance(loaded_pending, list):
            RU_PENDING.clear(); RU_PENDING.extend(loaded_pending)

        tg_send_post()

        _save_json(RU_PENDING_PATH, RU_PENDING)
        _save_json(RU_MAP_PATH, RU_MAP)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
