# main.py
# aiogram v3.x

import os
import re
import asyncio
import logging
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, time, timedelta

import pytz
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# =========================
# Config desde variables de entorno
# =========================
TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
if not TOKEN:
    raise RuntimeError("Falta TELEGRAM_TOKEN")

# Chat origen (donde llegan las columnas)
SOURCE_CHAT_ID = int(os.getenv("SOURCE_CHAT_ID", "0") or "0")
# Chat de resumen
SUMMARY_CHAT_ID = int(os.getenv("SUMMARY_CHAT_ID", "0") or "0")
# Chat para alertas/negativas
ALERTS_CHAT_ID = int(os.getenv("ALERTS_CHAT_ID", "0") or "0")

# Zona horaria
TZ_NAME = os.getenv("TIMEZONE", "America/Chihuahua")
TZ = pytz.timezone(TZ_NAME)

# Ventana AM por defecto (06:00–08:00) y hora auto-resumen (08:30)
AM_START = time(6, 0)
AM_END = time(8, 0)
AUTO_SUMMARY_HOUR = 8
AUTO_SUMMARY_MIN = 30

# =========================
# Estado en memoria
# =========================
# Guardamos entradas de columnas: [{"dt": datetime, "color": "green|yellow|red|alert",
# "actors": [..], "medio": str, "url": str, "raw": str}]
ENTRADAS = []
# Para evitar duplicados por URL
VISTAS_URL = set()

# =========================
# Utilidades
# =========================
COLOR_MAP = {
    "🟢": "green", "🟡": "yellow", "🔴": "red", "⚠️": "alert",
    "🟠": "alert", "🟥": "red", "🟨": "yellow", "🟩": "green"
}

STOP_TOKENS = {
    "OTROS DE INTERES", "OTROS DE INTERÉS",
    "ESTATAL", "MUNICIPAL", "JUÁREZ", "JUAREZ",
    "CHIHUAHUA", "LOCAL", "NACIONAL", "EL", "LA", "DEL", "DE", "LOCALE"
}

# Alias de actores (normalizados sin acentos y en minúsculas)
ACTOR_ALIASES = {
    "marco bonilla": {
        "bonilla", "alcalde bonilla", "marco bonilla", "marco bonilla mendoza",
        "alcalde de chihuahua", "presidente municipal chihuahua", "alcalde marco bonilla"
    },
    "maru campos": {
        "maru campos", "maria eugenia campos", "maría eugenia campos", "gobernadora campos",
        "maru campos galvan", "maría eugenia campos galván", "gobernadora de chihuahua"
    },
    "cruz pérez cuéllar": {
        "cruz pérez cuéllar", "cruz perez cuellar", "perez cuellar", "alcalde de juarez",
        "alcalde de juárez", "cruz pérez"
    },
    "pan": {"pan", "accion nacional", "acción nacional", "albiazul"},
    "morena": {"morena", "movimiento de regeneracion nacional", "movimiento de regeneración nacional", "guinda"},
    "pri": {"pri", "partido revolucionario institucional", "tricolor"},
}

def norm_txt(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s.lower().strip()

def ahora_tz() -> datetime:
    return datetime.now(TZ)

def en_ventana_am(dt: datetime) -> bool:
    t = dt.timetz()
    return AM_START <= t.replace(tzinfo=None) <= AM_END

def extraer_url(text: str) -> str:
    m = re.search(r"(https?://\S+)", text, flags=re.IGNORECASE)
    return m.group(1) if m else ""

def parse_header(header: str):
    """
    Devuelve: color(str), actores[list], medio(str)
    Header típico: "🟡 BONILLA / PAN / ENTRELÍNEAS"
    """
    color = "yellow"
    for c in COLOR_MAP:
        if c in header:
            color = COLOR_MAP[c]
            header = header.replace(c, " ")
            break

    # Partes separadas por "/"
    partes = [p.strip() for p in header.split("/") if p.strip()]
    if not partes:
        return color, [], "SIN MEDIO"

    # Última parte la tomamos como medio
    medio = partes[-1].upper()

    # Resto: actores brutos (limpiamos tokens comunes)
    actores_brutos = []
    for tok in partes[:-1]:
        tokU = tok.upper()
        if tokU in STOP_TOKENS:
            continue
        actores_brutos.append(tok)

    # Normalización por alias
    actores_canon = []
    for ab in actores_brutos:
        nab = norm_txt(ab)
        canon = None
        for can_name, alias_set in ACTOR_ALIASES.items():
            if nab in alias_set or nab == norm_txt(can_name):
                canon = can_name
                break
        if canon is None:
            # Si no matchea alias, usamos el texto capitalizado sin acentos
            canon = ab.title()
        if canon not in actores_canon:
            actores_canon.append(canon)

    return color, actores_canon, medio

def registrar_entrada(texto: str):
    # Buscamos encabezado en la primera línea con slashes o color
    lines = [l.strip() for l in texto.splitlines() if l.strip()]
    header = lines[0] if lines else ""
    color, actores, medio = parse_header(header)

    url = extraer_url(texto)
    if url and url in VISTAS_URL:
        return
    if url:
        VISTAS_URL.add(url)

    ENTRADAS.append({
        "dt": ahora_tz(),
        "color": color,
        "actors": actores,
        "medio": medio,
        "url": url,
        "raw": texto
    })

def build_resumen(subset, titulo_prefix="COLUMNAS"):
    # Conteos de semáforo
    c = Counter(e["color"] for e in subset)
    total = len(subset)

    # Actores
    actor_tot = Counter()
    actor_sem = defaultdict(lambda: Counter())
    for e in subset:
        for a in e["actors"]:
            actor_tot[a] += 1
            actor_sem[a][e["color"]] += 1

    # Medios
    medios = sorted({e["medio"] for e in subset if e["medio"]})

    # Armado texto
    now = ahora_tz()
    header = f"🔵 {titulo_prefix} / {now.strftime('%a %d %b %Y – %H:%M')}\n\n"
    semaforo = (
        "🪫 Semáforo\n"
        f"🟢 Positivas: {c.get('green',0)}\n"
        f"🟡 Neutras:   {c.get('yellow',0)}\n"
        f"🔴 Negativas: {c.get('red',0)}\n"
        f"⚠️ Alertas:   {c.get('alert',0)}\n"
        f"Total entradas: {total}\n"
    )

    actores_txt = "👥 Actores top\n-------------------------\n"
    if actor_tot:
        # top 10
        for a, n in actor_tot.most_common(10):
            row = f"{a.upper()}\n| Total {n}   "
            row += f"🟢{actor_sem[a].get('green',0)} "
            row += f"🟡{actor_sem[a].get('yellow',0)} "
            row += f"🔴{actor_sem[a].get('red',0)} "
            row += f"⚠️{actor_sem[a].get('alert',0)}\n"
            actores_txt += row
    else:
        actores_txt += "(sin actores detectados)\n"

    medios_txt = "📰 Medios con publicación\n"
    if medios:
        for m in medios:
            medios_txt += f"- {m}\n"
    else:
        medios_txt += "- (sin medios)\n"

    return header + semaforo + "\n" + actores_txt + "\n" + medios_txt

# =========================
# Bot y handlers
# =========================
router = Router()

@router.message(CommandStart())
async def cmd_start(m: Message):
    await m.answer("Hola 👋 Listo para resumir columnas. Usa /resumen_hoy o /resumen_am.")

@router.message(Command("resumen_hoy"))
async def cmd_resumen_hoy(m: Message):
    # Filtra por HOY (si no hay en la ventana, usa todo HOY)
    today = ahora_tz().date()
    hoy = [e for e in ENTRADAS if e["dt"].date() == today]
    if not hoy:
        await m.answer("No tengo columnas registradas para hoy.")
        return

    subset = hoy  # sin ventana
    txt = build_resumen(subset, "COLUMNAS")
    await m.answer(txt)

@router.message(Command("resumen_am"))
async def cmd_resumen_am(m: Message):
    today = ahora_tz().date()
    hoy = [e for e in ENTRADAS if e["dt"].date() == today]
    subset = []
    for e in hoy:
        t = e["dt"].astimezone(TZ).timetz().replace(tzinfo=None)
        if AM_START <= t <= AM_END:
            subset.append(e)

    titulo = "COLUMNAS AM"
    if not subset:
        await m.answer(f"🔵 {titulo} / Hoy no se recibieron columnas entre {AM_START.strftime('%H:%M')}–{AM_END.strftime('%H:%M')}.")
        return

    txt = build_resumen(subset, titulo)
    await m.answer(txt)

@router.message()
async def on_message(m: Message):
    # Solo guardamos lo que venga del chat origen
    if SOURCE_CHAT_ID and m.chat.id != SOURCE_CHAT_ID:
        return

    if not (m.text or m.caption):
        return

    contenido = (m.text or m.caption or "").strip()
    # Debe tener formato de cabecera para considerarse columna
    if "/" not in contenido and not any(c in contenido for c in COLOR_MAP.keys()):
        return

    registrar_entrada(contenido)

    # Si es negativa/alerta, avisa al grupo de alertas
    try:
        color, _, _ = parse_header(contenido.splitlines()[0])
        if color in ("red", "alert") and ALERTS_CHAT_ID:
            snippet = contenido.splitlines()[0][:200]
            await m.bot.send_message(
                ALERTS_CHAT_ID,
                f"🚨 Nueva {'NEGATIVA' if color=='red' else 'ALERTA'} detectada:\n{snippet}"
            )
    except Exception:
        pass

# =========================
# Programación automática 08:30
# =========================
async def enviar_resumen_autom(bot: Bot):
    today = ahora_tz().date()
    hoy = [e for e in ENTRADAS if e["dt"].date() == today]
    subset = []
    for e in hoy:
        t = e["dt"].astimezone(TZ).timetz().replace(tzinfo=None)
        if AM_START <= t <= AM_END:
            subset.append(e)

    titulo = "COLUMNAS AM"
    if not subset:
        await bot.send_message(
            SUMMARY_CHAT_ID,
            f"🔵 {titulo} / Hoy no se recibieron columnas entre {AM_START.strftime('%H:%M')}–{AM_END.strftime('%H:%M')}."
        )
        return

    await bot.send_message(SUMMARY_CHAT_ID, build_resumen(subset, titulo))

def programar_cron(scheduler: AsyncIOScheduler, bot: Bot):
    # Ejecuta todos los días a la hora local indicada
    scheduler.add_job(
        enviar_resumen_autom,
        "cron",
        hour=AUTO_SUMMARY_HOUR,
        minute=AUTO_SUMMARY_MIN,
        args=[bot],
        timezone=TZ_NAME,
        id="enviar_resumen_autom",
        replace_existing=True
    )

# =========================
# Arranque
# =========================
async def main():
    logging.basicConfig(level=logging.INFO)
    bot = Bot(token=TOKEN, parse_mode="HTML")
    dp = Dispatcher()
    dp.include_router(router)  # <<< IMPORTANTE: incluir router para que escuche

    scheduler = AsyncIOScheduler(timezone=TZ)
    scheduler.start()
    programar_cron(scheduler, bot)

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
