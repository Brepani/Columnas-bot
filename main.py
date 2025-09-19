# main.py
import os
import re
import asyncio
import logging
from datetime import datetime, time
from typing import Dict, List, Optional
import pytz

from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# =========================
# Config desde variables de entorno
# =========================
TOKEN = os.getenv("TELEGRAM_TOKEN")
SOURCE_CHAT_ID = int(os.getenv("SOURCE_CHAT_ID", "0"))
SUMMARY_CHAT_ID = int(os.getenv("SUMMARY_CHAT_ID", "0"))
ALERTS_CHAT_ID = int(os.getenv("ALERTS_CHAT_ID", "0"))
TZ = os.getenv("TIMEZONE", "America/Chihuahua")
RANGO_INICIO = os.getenv("RANGO_INICIO", "06:00")
RANGO_FIN = os.getenv("RANGO_FIN", "08:00")
RESUMEN_HORA = os.getenv("RESUMEN_HORA", "08:30")

bot = Bot(token=TOKEN)
dp = Dispatcher()
mx_tz = pytz.timezone(TZ)

ENTRADAS: List[Dict] = []

# =========================
# Catálogos y normalización
# =========================
EMOJI_SENT = {"🟢": "pos", "🟡": "neu", "🔴": "neg", "⚠️": "alert"}
EMOJI_ORDER = ["🟢", "🟡", "🔴", "⚠️"]

MEDIOS_CONOCIDOS = {
    "ENTRELÍNEAS", "ENTRELINEAS", "OMNIA", "VOZ EN RED", "LA PARADOJA",
    "EL DIARIO DE CHIHUAHUA", "EL HERALDO DE CHIHUAHUA", "NET NOTICIAS",
    "EL BORDO", "LA JIRIBILLA", "OJOS DE HIERRO", "TIEMPO"
}

DOMAIN_TO_MEDIO = {
    "omnia.com.mx": "OMNIA",
    "entrelineas.com.mx": "ENTRELÍNEAS",
    "vozenred.com.mx": "VOZ EN RED",
    "eldiariodechihuahua.mx": "EL DIARIO DE CHIHUAHUA",
    "elheraldodechihuahua.com.mx": "EL HERALDO DE CHIHUAHUA",
    "netnoticias.mx": "NET NOTICIAS",
    "elbordo.com.mx": "EL BORDO",
    "lajiribilla.com.mx": "LA JIRIBILLA",
    "tiempo.com.mx": "TIEMPO"
}

STOP_TOKENS = {
    "OTROS DE INTERES","OTROS DE INTERÉS","OTROS","DE","INTERES","INTERÉS",
    "LOCAL","ESTATAL","MUNICIPAL","ESTADO","CIUDAD","CHIHUAHUA","JUÁREZ","JUAREZ",
    "ALCALDE","PRESIDENTE","GOBERNADOR","GOBERNADORA","GABINETE","SEGURIDAD",
    "EDICIÓN","EDICION","COLUMNA","COLUMNAS","OPINIÓN","OPINION","SECCIÓN","SECCION",
    "POLÍTICA","POLITICA","EDITORIAL","GPS"
}

def norm(s: str) -> str:
    s = (s or "").upper().strip()
    s = (s.replace("Á","A").replace("É","E").replace("Í","I")
           .replace("Ó","O").replace("Ú","U").replace("Ü","U")
           .replace("Ñ","N"))
    s = re.sub(r"\s+", " ", s)
    return s

def ahora_tz() -> datetime:
    return datetime.now(mx_tz)

# =========================
# Helpers de encabezado
# =========================
def extraer_encabezado(lineas: List[str]) -> Optional[str]:
    for ln in lineas[:5]:
        t = ln.strip()
        if not t:
            continue
        if any(t.startswith(e) for e in EMOJI_ORDER) and t.count("/") >= 2:
            return t
    return None

def detectar_sentimiento(enc: str) -> str:
    for e in EMOJI_ORDER:
        if e in enc:
            return EMOJI_SENT[e]
    return "neu"

def split_slashes(enc: str) -> List[str]:
    return [p.strip() for p in enc.split("/") if p.strip()]

def _medio_por_dominio(texto: str) -> Optional[str]:
    urls = re.findall(r"https?://([^/\s]+)/?", texto, flags=re.IGNORECASE)
    if not urls:
        return None
    host = urls[-1].lower()
    host = re.sub(r"^www\.", "", host)
    for dom, medio in DOMAIN_TO_MEDIO.items():
        if host.endswith(dom):
            return medio
    return None

def decidir_medio(parts: List[str], texto_completo: str) -> str:
    if parts:
        candidato = norm(parts[-1])
        candidato = re.sub(r"[^\w\s-]", "", candidato)
        if candidato in {norm(m) for m in MEDIOS_CONOCIDOS}:
            for m in MEDIOS_CONOCIDOS:
                if norm(m) == candidato:
                    return m
    medio_link = _medio_por_dominio(texto_completo)
    if medio_link:
        return medio_link
    return "SIN MEDIO"

def decidir_actores(parts: List[str]) -> List[str]:
    actores_raw = parts[:-1] if len(parts) >= 2 else parts
    actores: List[str] = []
    for a in actores_raw:
        a_n = norm(re.sub(r"[^\w\s-]", "", a))
        if not a_n or a_n in STOP_TOKENS or len(a_n) <= 2:
            continue
        actores.append(a_n)
    vistos, res = set(), []
    for a in actores:
        if a not in vistos:
            vistos.add(a)
            res.append(a)
    return res

def extraer_link(texto: str) -> Optional[str]:
    m = re.findall(r"https?://\S+", texto)
    return m[-1] if m else None

# =========================
# Parseo de mensaje
# =========================
def parsear_columna(msg: Message) -> Optional[Dict]:
    if not (msg.text or msg.caption):
        return None
    full = msg.text or msg.caption or ""
    lineas = full.splitlines()
    enc = extraer_encabezado(lineas)
    if not enc:
        return None

    sent = detectar_sentimiento(enc)
    parts = split_slashes(enc)
    medio = decidir_medio(parts, full)
    actores = decidir_actores(parts)

    return {
        "fecha": datetime.fromtimestamp(msg.date, tz=mx_tz),
        "sent": sent,
        "medio": medio,
        "actores": actores,
        "link": extraer_link(full) or "",
        "raw_header": enc
    }

# =========================
# Handlers
# =========================
@dp.message()
async def on_message(m: Message):
    if SOURCE_CHAT_ID and m.chat.id != SOURCE_CHAT_ID:
        return
    col = parsear_columna(m)
    if not col:
        return
    ENTRADAS.append(col)

    if ALERTS_CHAT_ID and col["sent"] in ("neg", "alert"):
        resumen = armar_alerta(col)
        await bot.send_message(ALERTS_CHAT_ID, resumen, disable_web_page_preview=True)

def armar_alerta(col: Dict) -> str:
    icono = {"pos":"🟢","neu":"🟡","neg":"🔴","alert":"⚠️"}[col["sent"]]
    act = ", ".join(col["actores"]) if col["actores"] else "SIN ACTOR"
    medio = col["medio"]
    link = col["link"]
    partes = [
        f"{icono} ALERTA",
        f"Actores: {act}",
        f"Medio: {medio}"
    ]
    if link:
        partes.append(f"Link: {link}")
    return "\n".join(partes)

# =========================
# Resumen
# =========================
def build_resumen(entradas: List[Dict], titulo: str) -> str:
    c_pos = sum(1 for e in entradas if e["sent"] == "pos")
    c_neu = sum(1 for e in entradas if e["sent"] == "neu")
    c_neg = sum(1 for e in entradas if e["sent"] == "neg")
    c_ale = sum(1 for e in entradas if e["sent"] == "alert")

    actores_map: Dict[str, Dict[str,int]] = {}
    for e in entradas:
        for a in e["actores"]:
            actores_map.setdefault(a, {"pos":0,"neu":0,"neg":0,"alert":0,"tot":0})
            actores_map[a][e["sent"]] += 1
            actores_map[a]["tot"] += 1

    actores_top = sorted(actores_map.items(), key=lambda x: (-x[1]["tot"], x[0]))[:10]
    medios = sorted({e["medio"] for e in entradas})

    ahora = ahora_tz()
    head = [
        f"🔵 {titulo} / {ahora.strftime('%a %d %b %Y – %H:%M')}",
        "",
        "🪫 Semáforo",
        f"🟢 Positivas: {c_pos}",
        f"🟡 Neutras:   {c_neu}",
        f"🔴 Negativas: {c_neg}",
        f"⚠️ Alertas:   {c_ale}",
        f"Total entradas: {len(entradas)}",
        ""
    ]

    actores_lines = ["👥 Actores top", "-------------------------"]
    if actores_top:
        for nombre, cnt in actores_top:
            linea = f"{nombre}\n| Total {cnt['tot']}   🟢{cnt['pos']} 🟡{cnt['neu']} 🔴{cnt['neg']} ⚠️{cnt['alert']}"
            actores_lines.append(linea)
    else:
        actores_lines.append("— sin actores detectados —")

    medios_lines = ["", "📰 Medios con publicación"]
    if medios:
        for m in medios:
            medios_lines.append(f"- {m}")
    else:
        medios_lines.append("— sin medios —")

    cuerpo = "\n".join(head + actores_lines + medios_lines)
    return cuerpo

async def enviar_resumen(subset: List[Dict], titulo: str):
    if not subset:
        msg = "🔵 COLUMNAS AM / Hoy no se recibieron columnas en el periodo solicitado."
        await bot.send_message(SUMMARY_CHAT_ID, msg, disable_web_page_preview=True)
        return
    texto = build_resumen(subset, "COLUMNAS")
    await bot.send_message(SUMMARY_CHAT_ID, texto, disable_web_page_preview=True)

@dp.message(Command("resumen_hoy"))
async def cmd_resumen_hoy(m: Message):
    now = ahora_tz()
    hoy = now.date()
    h1 = datetime.combine(hoy, time.fromisoformat(RANGO_INICIO)).astimezone(mx_tz)
    h2 = datetime.combine(hoy, time.fromisoformat(RANGO_FIN)).astimezone(mx_tz)

    subset = [e for e in ENTRADAS if h1 <= e["fecha"] <= h2]
    await enviar_resumen(subset, "COLUMNAS")

# =========================
# Scheduler 8:30 AM
# =========================
def programar_scheduler():
    scheduler = AsyncIOScheduler(timezone=mx_tz)
    hh, mm = RESUMEN_HORA.split(":")
    scheduler.add_job(job_resumen_automatico, "cron", hour=int(hh), minute=int(mm))
    scheduler.start()

async def job_resumen_automatico():
    now = ahora_tz()
    hoy = now.date()
    h1 = datetime.combine(hoy, time.fromisoformat(RANGO_INICIO)).astimezone(mx_tz)
    h2 = datetime.combine(hoy, time.fromisoformat(RANGO_FIN)).astimezone(mx_tz)
    subset = [e for e in ENTRADAS if h1 <= e["fecha"] <= h2]
    await enviar_resumen(subset, "COLUMNAS")

# =========================
# Main
# =========================
async def main():
    logging.basicConfig(level=logging.INFO)
    programar_scheduler()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
