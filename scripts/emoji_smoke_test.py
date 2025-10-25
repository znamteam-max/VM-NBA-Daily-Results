#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Запускается отдельным workflow. Делает три вещи:
1) Парсит TEAM_EMOJI_JSON -> {ABBR: custom_emoji_id}
2) Проверяет все ID через getCustomEmojiStickers
3) Отправляет в чат одно или несколько сообщений:
   • "VALID": только распознанные (и, если задано, из нужного набора) лого через <tg-emoji>
   • "UNKNOWN": список аббревиатур/ID, которые не распознаны (для информации)
"""

import os, json, re, sys, textwrap, urllib.request, urllib.error

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT  = os.getenv("TELEGRAM_CHAT_ID", "").strip()
SET_NAME_FILTER = (os.getenv("TEAM_EMOJI_SET_NAME") or "").strip()  # напр. "got_ball_team"
RAW_JSON = os.getenv("TEAM_EMOJI_JSON", "").strip()

if not (TOKEN and CHAT and RAW_JSON):
    print("Missing envs: need TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TEAM_EMOJI_JSON", file=sys.stderr)
    sys.exit(1)

def tg_call(method: str, payload: dict) -> dict:
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{TOKEN}/{method}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "ignore")
        print(f"[HTTPError] {method} {e.code} {e.reason}: {body}", file=sys.stderr)
        raise

def send_html(html: str):
    payload = {
        "chat_id": CHAT,
        "text": html,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    res = tg_call("sendMessage", payload)
    ok = res.get("ok")
    if not ok:
        print("sendMessage failed:", res, file=sys.stderr)

# 1) Парсим JSON секрета
try:
    mapping = json.loads(RAW_JSON)
except Exception as e:
    print("TEAM_EMOJI_JSON parse error:", e, file=sys.stderr)
    sys.exit(1)

# 2) Извлекаем числовые ID
abbr_to_id = {}
ids = []
for abbr, val in mapping.items():
    v = str(val).strip()
    m = re.fullmatch(r"(?:id:)?(\d{10,})", v)
    if not m:
        print(f"[WARN] value for {abbr} is not a numeric custom_emoji_id: {v}", file=sys.stderr)
        continue
    cid = m.group(1)
    abbr_to_id[abbr] = cid
    ids.append(cid)

# Убираем дубли ID, но сохраняем первичные соответствия аббревиатур
ids_unique = list(dict.fromkeys(ids))
if not ids_unique:
    print("No numeric custom_emoji_ids found in secret", file=sys.stderr)
    sys.exit(1)

# 3) Проверяем все ID разом
resp = tg_call("getCustomEmojiStickers", {"custom_emoji_ids": ids_unique})
if not resp.get("ok"):
    print("getCustomEmojiStickers failed:", resp, file=sys.stderr)
    sys.exit(1)

stickers = resp.get("result") or []
id_to_sticker = {st.get("custom_emoji_id"): st for st in stickers}

recognized = {}
unknown = {}   # abbr -> id (не распознаны)

for abbr, cid in sorted(abbr_to_id.items()):
    st = id_to_sticker.get(cid)
    if not st:
        unknown[abbr] = cid
        continue
    # Фильтр по имени набора (если задан)
    if SET_NAME_FILTER:
        if st.get("set_name") != SET_NAME_FILTER:
            # если фильтр задан, а set_name не совпал — считаем «unknown для отправки»,
            # но логируем отдельно как "wrong set"
            unknown[abbr] = f"{cid} (set={st.get('set_name')})"
            continue
    # Дополнительно убеждаемся в типе
    if st.get("type") not in (None, "custom_emoji", "regular"):
        # В новых версиях Bot API поле type может отсутствовать для кастом эмодзи.
        pass
    recognized[abbr] = cid

print(f"[INFO] Recognized {len(recognized)} of {len(abbr_to_id)} (IDs unique: {len(ids_unique)})")
if unknown:
    print("[INFO] Unknown/filtered IDs:")
    for a, cid in unknown.items():
        print(f"  {a}: {cid}")

# 4) Готовим текст только из корректных логотипов
# Формат строки: "ATL: <tg-emoji emoji-id="...">X</tg-emoji>"
lines = []
for abbr in sorted(recognized.keys()):
    cid = recognized[abbr]
    lines.append(f"{abbr}: <tg-emoji emoji-id=\"{cid}\">🏀</tg-emoji>")

if not lines:
    send_html("❌ Проверка логотипов: нет валидных ID (ничего не отправлено).")
    sys.exit(0)

# Разбиваем на блоки, чтобы точно не превысить лимит 4096 символов
blocks = []
cur = []
cur_len = 0
for line in lines:
    # учтём перевод строки
    add_len = len(line) + 1
    if cur_len + add_len > 3500 and cur:
        blocks.append("\n".join(cur))
        cur = []
        cur_len = 0
    cur.append(line)
    cur_len += add_len
if cur:
    blocks.append("\n".join(cur))

# 5) Отправляем «валидные» (только распознанные) и опционально список «неизвестных»
send_html(f"✅ Проверка логотипов (valid {len(recognized)}/{len(abbr_to_id)}):")
for b in blocks:
    send_html(b)

if unknown:
    # просто информируем текстом; сами «битые» эмодзи не вставляем
    # чтобы сообщение не было огромным — обрежем до 30 строк
    unk_lines = [f"{a}: {cid}" for a,cid in sorted(unknown.items())]
    preview = "\n".join(unk_lines[:30])
    if len(unk_lines) > 30:
        preview += f"\n… и ещё {len(unk_lines)-30}"
    send_html("⚠️ Не распознаны или отфильтрованы по набору:\n" + "<pre>" + preview + "</pre>")
