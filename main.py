# main.py
# Bot de columnas – encabezado tolerante (con o sin emoji) y refuerzo de Actores top.
# Reemplaza COMPLETO este archivo.

import os
import re
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from collections import defaultdict, Counter

import pytz
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# =========================
# Config desde variables de entorno
# =========================
TOKEN = os.getenv("TELEGRAM_TOKEN")
SUMMARY_CHAT_ID = int(os.getenv("SUMMARY_CHAT_ID", "0"))
SOURCE_CHAT_ID = int(os.getenv("SOURCE_CHAT_ID", "0"))
ALERTS_CHAT_ID = int(os.getenv("ALERTS_CHAT_ID", "0"))
TZ_NAME = os.getenv("LOCAL_TZ", "America/Chihuahua")

# Horarios de corte para el resumen automático (6-8 am, envía 8:30)
RESUME_HORA = int(os.getenv("RESUME_HORA", "8"))
RESUME_MIN = int(os.getenv("RESUME_MIN", "30"))

# =========================
# Inicialización
# =========================
logging.basicConfig(level=logging.INFO)
bot = Bot(token=TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler()

tz = pytz.timezone(TZ_NAME)

# Memoria simple en proceso (día actual)
ENTRADAS: List[Dict] = []

# =========================
# Utilidades
# =========================

RE_URL = re.compile(r"(https?://\S+)", re.IGNORECASE)
RE_HEADER = re.compile(r"^\s*([🟢🟡🔴⚠️])?\s*(.+?)\s*$")

def hoy_range() -> Tuple[datetime, datetime]:
    now = datetime.now(tz)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end

def normaliza(s: str) -> str:
    # Mayúsculas, sin espacios extra
    return re.sub(r"\s+", " ", s.strip()).upper()

def pick_semaforo_from_body(texto: str) -> str:
    # Cuenta emojis en el cuerpo si no hay en encabezado
    counts = Counter(ch for ch in texto if ch in "🟢🟡🔴⚠️")
    if not counts:
        return "🟡"
    # si empatan, prioriza 🟡
    orden = ["🟢", "🟡", "🔴", "⚠️"]
    return max(orden, key=lambda e: (counts.get(e, 0)))

def parse_encabezado_y_medio(texto: str) -> Tuple[str, List[str], str]:
    """
    Detecta encabezado en la PRIMERA línea no vacía.
    Soporta con o sin emoji al inicio.
    actor = primer segmento
    medio = último segmento
    """
    lineas = [l for l in texto.splitlines() if l.strip()]
    if not lineas:
        return "🟡", [], ""

    header_line = lineas[0]
    m = RE_HEADER.match(header_line)
    if not m:
        # sin header claro -> semáforo desde cuerpo, sin actores
        sem = pick_semaforo_from_body(texto)
        return sem, [], ""

    emoji_hdr, resto = m.groups()
    tokens = [normaliza(t) for t in resto.split("/") if t.strip()]
    # Actor(es) = primer segmento (puede traer varios separados por coma o &)
    actores: List[str] = []
    if tokens:
        primer = tokens[0]
        # divide por coma o “ & ”
        for a in re.split(r"\s*,\s*|\s*&\s*", primer):
            if a:
                actores.append(a)

    medio = tokens[-1] if len(tokens) >= 1 else ""

    # Si no vino emoji al inicio, decide por el cuerpo:
    sem = emoji_hdr if emoji_hdr in ("🟢", "🟡", "🔴", "⚠️") else pick_semaforo_from_body(texto)
    return sem, actores, medio

def medio_desde_url(url: str) -> str:
    try:
        host = re.sub(r"^https?://", "", url, flags=re.IGNORECASE).split("/")[0]
        host = host.lower()
    except Exception:
        return ""
    # mapeos comunes
    if "vozenred" in host:
        return "VOZ EN RED"
    if "entrelineas" in host:
        return "ENTRELÍNEAS"
    if "omnia" in host:
        return "OMNIA"
    if "netnoticias" in host:
        return "NET NOTICIAS"
    if "elheraldodechihuahua" in host or "heraldodechihuahua" in host:
        return "EL HERALDO DE CHIHUAHUA"
    return host.upper()

def parse_mensaje(msg: str) -> Dict:
    """
    Devuelve dict con:
    - semaforo: 🟢/🟡/🔴/⚠️
    - actores: [..]
    - medio: str
    - url: str
    - ts: datetime aware
    """
    sem, actores, medio_hdr = parse_encabezado_y_medio(msg)
    url = ""
    m = RE_URL.search(msg)
    if m:
        url = m.group(1).strip()

    medio = medio_hdr if medio_hdr else (medio_desde_url(url) if url else "SIN MEDIO")

    return {
        "semaforo": sem,
        "actores": actores,
        "medio": medio,
        "url": url,
        "ts": datetime.now(tz),
    }

def dentro_de_hoy(ts: datetime) -> bool:
    start, end = hoy_range()
    return start <= ts < end

# =========================
# Render de resumen
# =========================

def render_resumen(items: List[Dict]) -> str:
    # Semáforo global
    c = Counter(it["semaforo"] for it in items)
    total = len(items)
    verdes = c.get("🟢", 0)
    amar = c.get("🟡", 0)
    rojos = c.get("🔴", 0)
    alrt = c.get("⚠️", 0)

    # Actores top: agrupa por actor individual
    by_actor: Dict[str, Counter] = defaultdict(Counter)
    for it in items:
        for act in it["actores"] or ["OTROS DE INTERES"]:
            by_actor[act][it["semaforo"]] += 1
            by_actor[act]["TOTAL"] += 1

    # Ordena por total desc
    actores_sorted = sorted(by_actor.items(), key=lambda kv: (-kv[1]["TOTAL"], kv[0]))

    # Medios con publicación (únicos, orden alfabético)
    medios = sorted({it["medio"] for it in items})

    now = datetime.now(tz)
    header = f"🔵 COLUMNAS / {now:%a %d %b %Y – %H:%M}".replace("Thu", "Thu").replace("Mon", "Mon")

    lines = [header, "", "🪫 Semáforo",
             f"🟢 Positivas: {verdes}",
             f"🟡 Neutras:   {amar}",
             f"🔴 Negativas: {rojos}",
             f"⚠️ Alertas:   {alrt}",
             f"Total entradas: {total}", ""]

    # Actores top
    lines.append("👥 Actores top")
    lines.append("-------------------------")
    if actores_sorted:
        for actor, cnt in actores_sorted[:10]:
            g = cnt.get("🟢", 0); y = cnt.get("🟡", 0); r = cnt.get("🔴", 0); w = cnt.get("⚠️", 0); t = cnt.get("TOTAL", 0)
            lines.append(f"{actor}")
            lines.append(f"| Total {t}   🟢{g} 🟡{y} 🔴{r} ⚠️{w}")
    else:
        lines.append("—")

    lines.append("")
    lines.append("📰 Medios con publicación")
    if medios:
        for m in medios:
            lines.append(f"- {m}")
    else:
        lines.append("—")

    return "\n".join(lines)

# =========================
# Handlers
# =========================

@dp.message(Command("resumen_hoy"))
async def cmd_resumen_hoy(message: Message):
    subset = [it for it in ENTRADAS if dentro_de_hoy(it["ts"])]
    texto = render_resumen(subset)
    await bot.send_message(SUMMARY_CHAT_ID or message.chat.id, texto, disable_web_page_preview=True)

@dp.message()
async def on_any_message(message: Message):
    # Solo procesa mensajes del grupo fuente (si se configuró) y que tengan link
    if SOURCE_CHAT_ID and message.chat.id != SOURCE_CHAT_ID:
        return

    txt = (message.text or message.caption or "").strip()
    if not txt:
        return

    if RE_URL.search(txt) is None:
        # si no trae link, la ignoramos (regla original)
        return

    item = parse_mensaje(txt)
    ENTRADAS.append(item)

    # ALERTA: si viene ⚠️ o 🔴, reenvía al canal de alertas
    if item["semaforo"] in ("⚠️", "🔴") and ALERTS_CHAT_ID:
        medio = item["medio"]
        acts = ", ".join(item["actores"]) if item["actores"] else "OTROS DE INTERES"
        aviso = f"🚨 {item['semaforo']} {acts} – {medio}\n{item['url']}"
        await bot.send_message(ALERTS_CHAT_ID, aviso, disable_web_page_preview=True)

# =========================
# Tarea programada 8:30 AM
# =========================

async def enviar_resumen_autom():
    subset = [it for it in ENTRADAS if dentro_de_hoy(it["ts"])]
    titulo = render_resumen(subset)
    await bot.send_message(SUMMARY_CHAT_ID or SOURCE_CHAT_ID or 0, titulo, disable_web_page_preview=True)

def programa_job():
    # 8:30 hora local
    now = datetime.now(tz)
    run_at = now.replace(hour=RESUME_HORA, minute=RESUME_MIN, second=0, microsecond=0)
    if run_at < now:
        run_at += timedelta(days=1)
    scheduler.add_job(enviar_resumen_autom, "date", run_date=run_at)
    # vuelve a programar cada día
    scheduler.add_job(enviar_resumen_autom, "cron", hour=RESUME_HORA, minute=RESUME_MIN, timezone=tz)

# =========================
# Main
# =========================

async def main():
    programa_job()
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
