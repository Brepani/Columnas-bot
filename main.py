import os
import re
import logging
import asyncio
from typing import Dict, List, Tuple, Set
from datetime import datetime, timedelta
import hashlib

import pytz
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# =========================
# Config por variables de entorno
# =========================
TOKEN = os.getenv("TELEGRAM_TOKEN")
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")
ALERTS_CHAT_ID = os.getenv("ALERTS_CHAT_ID")
SOURCE_CHAT_ID = os.getenv("SOURCE_CHAT_ID")  # chat del que se leen las columnas

TIMEZONE = "America/Chihuahua"
tz = pytz.timezone(TIMEZONE)

def ahora_tz():
    """Devuelve la hora actual con la zona configurada."""
    return datetime.now(tz)

# Validación de variables
missing = [k for k, v in {
    "TELEGRAM_TOKEN": TOKEN,
    "SUMMARY_CHAT_ID": SUMMARY_CHAT_ID,
    "ALERTS_CHAT_ID": ALERTS_CHAT_ID,
    "SOURCE_CHAT_ID": SOURCE_CHAT_ID,
}.items() if not v]
if missing:
    raise RuntimeError("Faltan variables de entorno: " + ", ".join(missing))

# =========================
# Bot & Dispatcher
# =========================
bot = Bot(token=TOKEN)
dp = Dispatcher()

# =========================
# Memoria en ejecución
# =========================
# Guardaremos UN ITEM por COLUMNA (un link = una columna)
# Campos por item:
#   col_id, fecha, color, actores (list), alcance, medio, cuerpo, link
all_items: List[Dict] = []

# =========================
# Utilidades de texto / parsing
# =========================
ICON_SET = ("🟢", "🟡", "🔴", "⚠️")
ICON_WEIGHT = {"⚠️": 4, "🔴": 3, "🟡": 2, "🟢": 1}

URL_RE = re.compile(r"(https?://[^\s]+)")
WS = re.compile(r"\s+")

def limpiar(s: str) -> str:
    return WS.sub(" ", (s or "").strip(" \n\r\t-—·*"))

def detectar_color(texto: str) -> str:
    """Si hay 🔴 o ⚠️ en el mensaje, respétalo. Si no, intenta detectar 🟢/🟡. Default 🟡."""
    if "🔴" in texto:
        return "🔴"
    if "⚠️" in texto or "⚠" in texto:
        return "⚠️"
    if "🟢" in texto:
        return "🟢"
    if "🟡" in texto:
        return "🟡"
    return "🟡"

def normalizar_token(tok: str) -> str:
    return limpiar(tok.upper())

def hash_columna(link: str) -> str:
    return hashlib.sha1(link.encode("utf-8")).hexdigest()

def extraer_header_y_cuerpo(texto: str) -> Tuple[str, str]:
    """Primera línea se toma como encabezado de formato:
       CLASIFICACIÓN / ACTOR(ES) / ALCANCE / MEDIO
       El resto es el cuerpo."""
    lineas = texto.strip().splitlines()
    if not lineas:
        return "", ""
    header = limpiar(lineas[0])
    cuerpo = limpiar("\n".join(lineas[1:]))
    return header, cuerpo

def parse_header(header: str) -> Tuple[List[str], str, str]:
    """
    Devuelve (actores[], alcance, medio)
    Header esperado con slashes: A / B / C / D
    Donde C = alcance, D = medio. B puede contener varios actores separados por '/'.
    Si no hay suficientes partes, se hace lo mejor posible.
    """
    # Quitar emojis al inicio de header si los hubiera
    header_sin_icons = header
    for ic in ICON_SET:
        header_sin_icons = header_sin_icons.replace(ic, "")
    partes = [normalizar_token(p) for p in header_sin_icons.split("/") if limpiar(p)]
    actores: List[str] = []
    alcance, medio = "", ""

    if len(partes) >= 4:
        # [clasificación] / [actores] / [alcance] / [medio]
        actores = [normalizar_token(x) for x in partes[1:-2]]  # si vinieran varios con /
        # En muchos ejemplos solo hay un bloque de actores en el segundo segmento
        if not actores:
            actores = [normalizar_token(partes[1])]
        alcance = partes[-2]
        medio = partes[-1]
    elif len(partes) == 3:
        # [clasificación] / [actores] / [medio]  (tratamos 2do como actores, 3ro como medio)
        actores = [normalizar_token(partes[1])]
        medio = partes[2]
    elif len(partes) == 2:
        actores = [normalizar_token(partes[1])]
    else:
        # Sólo clasif; no hay actores
        actores = []

    # Filtro de vacíos
    actores = [a for a in actores if a]
    return actores, alcance, medio

def parse_message(texto: str) -> List[Dict]:
    """
    Parsea un mensaje completo de Telegram (una columna por link).
    - Header en la primera línea con slashes.
    - El último URL del mensaje se toma como link de la columna.
    - color: detectado por emojis en el mensaje (🔴, ⚠️, 🟢, 🟡)
    """
    if not texto:
        return []

    header, cuerpo = extraer_header_y_cuerpo(texto)
    if not header:
        return []

    actores, alcance, medio = parse_header(header)

    # Buscar link (usamos el ÚLTIMO por seguridad)
    urls = URL_RE.findall(texto)
    link = urls[-1] if urls else ""

    if not link:
        # Sin link no registramos columna (regla de negocio del proyecto)
        return []

    color = detectar_color(texto)
    col_id = hash_columna(link)
    item = {
        "col_id": col_id,
        "fecha": ahora_tz(),
        "color": color,
        "actors": actores or ["OTROS DE INTERES"],
        "alcance": alcance,
        "medio": medio or "SIN MEDIO",
        "cuerpo": cuerpo,
        "link": link,
    }
    return [item]

def dentro_rango(dt: datetime, start: datetime, end: datetime) -> bool:
    return (dt >= start) and (dt <= end)

def filtrar_por_rango(items: List[Dict], start: datetime, end: datetime) -> List[Dict]:
    return [it for it in items if dentro_rango(it["fecha"], start, end)]

def armar_alerta(it: Dict) -> str:
    """Mensaje corto de alerta para el canal de alertas (sin Markdown)."""
    frase = (it.get("cuerpo") or "").replace("\n", " ").strip()
    if len(frase) > 200:
        frase = frase[:200] + "…"
    actors_str = ", ".join(it["actors"])
    alcance = f" ({it['alcance']})" if it['alcance'] else ""
    alerta = (
        f"[ALERTA {it['color']}] {actors_str}\n"
        f"Medio: {it['medio']}{alcance}\n\n"
        f"Frase clave:\n\"{frase}\"\n"
    )
    if it.get("link"):
        alerta += f"\n🔗 Abrir nota: {it['link']}"
    return alerta

# =========================
# Construcción de resumen
# =========================
def contar_semaforo(items: List[Dict]) -> Tuple[int, int, int, int]:
    verdes = sum(1 for x in items if x["color"] == "🟢")
    amar = sum(1 for x in items if x["color"] == "🟡")
    rojas = sum(1 for x in items if x["color"] == "🔴")
    alert = sum(1 for x in items if x["color"] == "⚠️")
    return verdes, amar, rojas, alert

def top_actores(items: List[Dict], limite: int = 5) -> List[Tuple[str, Dict]]:
    acc: Dict[str, Dict[str, int]] = {}
    for it in items:
        for a in it["actors"]:
            d = acc.setdefault(a, {"🟢": 0, "🟡": 0, "🔴": 0, "⚠️": 0, "total": 0})
            d[it["color"]] += 1
            d["total"] += 1
    orden = sorted(acc.items(), key=lambda kv: (kv[1]["total"], kv[1]["🔴"], kv[1]["⚠️"]), reverse=True)
    return orden[:limite]

def medios_con_publicacion(items: List[Dict]) -> List[str]:
    vistos: Set[str] = set()
    out: List[str] = []
    for it in items:
        m = it["medio"]
        if m and m not in vistos:
            vistos.add(m)
            out.append(m)
    return out

def extraer_temas(items: List[Dict], max_temas: int = 5) -> List[str]:
    """
    Heurística simple de 'temas': toma oraciones destacables del cuerpo,
    priorizando las más largas que contengan palabras clave básicas.
    """
    textos = []
    for it in items:
        body = (it.get("cuerpo") or "").replace("\n", " ")
        # dividir en oraciones por puntos
        frases = [limpiar(x) for x in re.split(r"[\.!?]+", body) if limpiar(x)]
        # agregar algunas candidatas por columna
        for f in frases[:3]:  # limitamos para no sobrecargar
            textos.append(f)

    # Scoring básico por longitud y presencia de palabras “políticas”
    claves = ("ALCALDE", "GOBERNADORA", "CONGRESO", "PAN", "MORENA", "PRI", "JUÁREZ", "CHIHUAHUA", "ELECCIÓN", "ENCUESTA")
    scored = []
    for t in textos:
        score = len(t)
        if any(k in t.upper() for k in claves):
            score += 80
        scored.append((score, t))

    scored.sort(reverse=True)
    # Evitar duplicados muy similares
    vistos = set()
    temas = []
    for _, t in scored:
        base = t[:80]
        if base in vistos:
            continue
        vistos.add(base)
        temas.append(t)
        if len(temas) >= max_temas:
            break
    return temas

def render_actores_top(top: List[Tuple[str, Dict]]) -> str:
    if not top:
        return "—"
    lines = ["👥 Actores top", "-------------------------"]
    for actor, data in top:
        lines.append(f"{actor}\n| Total {data['total']}   🟢{data['🟢']} 🟡{data['🟡']} 🔴{data['🔴']} ⚠️{data['⚠️']}")
    return "\n".join(lines)

def generar_resumen(items: List[Dict], titulo: str) -> str:
    if not items:
        return f"🔵 COLUMNAS AM / {titulo}\nHoy no se recibieron columnas en el periodo solicitado."

    v, a, r, w = contar_semaforo(items)
    total = len(items)

    # Semáforo
    lines = [
        f"🔵 COLUMNAS / {titulo}",
        "",
        "🪫 Semáforo",
        f"🟢 Positivas: {v}",
        f"🟡 Neutras:   {a}",
        f"🔴 Negativas: {r}",
        f"⚠️ Alertas:   {w}",
        f"Total entradas: {total}",
        "",
    ]

    # Actores
    lines.append(render_actores_top(top_actores(items, 6)))
    lines.append("")

    # Medios
    medios = medios_con_publicacion(items)
    if medios:
        lines.append("📰 Medios con publicación")
        for m in medios:
            lines.append(f"- {m}")
        lines.append("")

    # Temas
    temas = extraer_temas(items, 5)
    if temas:
        lines.append("📌 Temas principales")
        for t in temas:
            lines.append(f"- {t}")
        lines.append("")

    return "\n".join(lines).strip()

# =========================
# Handlers
# =========================
@dp.message(Command("resumen_hoy"))
async def cmd_resumen_hoy(message: Message):
    now = ahora_tz()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    subset = filtrar_por_rango(all_items, start, now)
    titulo = now.strftime("%a %d %b %Y – %H:%M")
    await bot.send_message(SUMMARY_CHAT_ID, generar_resumen(subset, titulo), disable_web_page_preview=True)

@dp.message(Command("links"))
async def cmd_links(message: Message):
    now = ahora_tz()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    subset = filtrar_por_rango(all_items, start, now)

    vistos: Set[str] = set()
    lines: List[str] = []
    for it in subset:
        link = it.get("link")
        if not link:
            continue
        if it["col_id"] in vistos:
            continue
        vistos.add(it["col_id"])
        lines.append(f"{it['color']} {it['medio']} – {', '.join(it['actors'])}\n{link}")

    if not lines:
        await bot.send_message(SUMMARY_CHAT_ID, "🔗 Hoy no hay links registrados.", disable_web_page_preview=True)
        return

    await bot.send_message(SUMMARY_CHAT_ID, "🔗 Links de hoy\n\n" + "\n\n".join(lines), disable_web_page_preview=True)

@dp.message()
async def recibir_columnas(message: Message):
    try:
        # Solo aceptar del chat fuente configurado
        if SOURCE_CHAT_ID and str(message.chat.id) != str(SOURCE_CHAT_ID):
            return

        texto = message.text or ""
        if "/" not in texto:
            return

        items = parse_message(texto)
        if not items:
            return

        # Evitar duplicados por link
        existentes = {it["col_id"] for it in all_items}
        nuevos = [it for it in items if it["col_id"] not in existentes]
        if not nuevos:
            return

        all_items.extend(nuevos)

        # Alertas inmediatas (🔴 o ⚠️)
        for it in nuevos:
            if it["color"] in ("🔴", "⚠️") and ALERTS_CHAT_ID:
                try:
                    await bot.send_message(ALERTS_CHAT_ID, armar_alerta(it), disable_web_page_preview=True)
                except Exception as e:
                    logging.error(f"Error enviando alerta: {e}")

    except Exception as e:
        logging.error(f"Error en recibir_columnas: {e}")

# =========================
# Tarea programada 08:30 (toma 06–08)
# =========================
async def enviar_resumen_autom():
    now = ahora_tz()
    start = now.replace(hour=6, minute=0, second=0, microsecond=0)
    end   = now.replace(hour=8, minute=0, second=0, microsecond=0)
    subset = filtrar_por_rango(all_items, start, end)
    titulo = now.strftime("%a %d %b %Y – 08:30")
    await bot.send_message(SUMMARY_CHAT_ID, generar_resumen(subset, titulo), disable_web_page_preview=True)

# =========================
# Main
# =========================
async def main():
    logging.basicConfig(level=logging.INFO)
    scheduler = AsyncIOScheduler(timezone=tz)
    scheduler.add_job(enviar_resumen_autom, "cron", hour=8, minute=30)
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
