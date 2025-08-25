import os
import logging
import asyncio
from typing import Dict, List, Tuple
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime
import pytz
import re

# =========================
# Config por .env
# =========================
TOKEN = os.getenv("TELEGRAM_TOKEN")
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")
ALERTS_CHAT_ID = os.getenv("ALERTS_CHAT_ID")
SOURCE_CHAT_ID = os.getenv("SOURCE_CHAT_ID")

TIMEZONE = "America/Chihuahua"
tz = pytz.timezone(TIMEZONE)

# Validación temprana (para que el log diga qué falta)
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

# Guardamos TODAS las entradas del día (una por párrafo)
all_items: List[Dict] = []

# =========================
# Utilidades
# =========================
ICON_SET = ("🟢", "🟡", "🔴", "⚠️")
# Severidad: mayor es más grave
ICON_WEIGHT = {"⚠️": 4, "🔴": 3, "🟡": 2, "🟢": 1}

def ahora_tz() -> datetime:
    return datetime.now(tz)

def choose_severity(found_icons: List[str]) -> str:
    if not found_icons:
        return "🟡"
    return max(found_icons, key=lambda ic: ICON_WEIGHT.get(ic, 0))

def split_paragraphs(lines: List[str]) -> List[str]:
    """Separa en párrafos por líneas en blanco; conserva texto."""
    paras, buf = [], []
    for ln in lines:
        if ln.strip() == "":
            if buf:
                paras.append("\n".join(buf).strip())
                buf = []
        else:
            buf.append(ln)
    if buf:
        paras.append("\n".join(buf).strip())
    return paras

def clean_leading_icons(text: str) -> Tuple[str, List[str]]:
    """Extrae íconos al inicio y dentro del párrafo, devuelve cuerpo limpio + lista de íconos encontrados."""
    # Íconos al inicio (p.ej. '🟡⚠ Texto...')
    start_icons = []
    i = 0
    while i < len(text):
        ch = text[i]
        # considerar emojis compuestos (dos bytes visuales), usamos coincidencia simple por pertenencia
        if text[i:i+2] in ICON_SET:
            start_icons.append(text[i:i+2])
            i += 2
            continue
        if ch.isspace() or ch in "-–—•*":
            i += 1
            continue
        break
    body = text[i:].strip()
    # También detecta íconos en el resto del texto (por si vienen después)
    inner = [m.group(0) for m in re.finditer(r"(🟢|🟡|🔴|⚠️)", body)]
    icons = start_icons + inner
    return body, list(dict.fromkeys(icons))  # únicos en orden

def parse_header(header: str) -> Dict | None:
    """
    Soporta:
    - '🟡 BONILLA / JUÁREZ / EL DIARIO'
    - 'PAN / MORENA / ALCALDE BONILLA / ENTRELÍNEAS'
    Retorna dict con actors (list), alcance (str|''), medio (str)
    y si viene color en el header, lo ignora (se toma del párrafo).
    """
    h = header.strip()
    # Quitar color inicial si viene pegado al header
    if h[:2] in ICON_SET:
        h = h[2:].strip()
    parts = [p.strip() for p in h.split("/") if p.strip()]
    if len(parts) < 2:
        return None
    if len(parts) == 3:
        actor, alcance, medio = parts
        actors = [actor]
        return {"actors": actors, "alcance": alcance, "medio": medio}
    else:
        medio = parts[-1]
        actors = parts[:-1]
        return {"actors": actors, "alcance": "", "medio": medio}

def parse_message(text: str) -> List[Dict]:
    """
    Devuelve una lista de items (uno por párrafo detectado).
    Cada item: {color, actors[], alcance, medio, cuerpo, link, ts}
    """
    try:
        lines = [ln for ln in (text or "").split("\n")]
        # Buscar link (última línea http)
        link = ""
        for ln in reversed(lines):
            if ln.strip().startswith("http"):
                link = ln.strip()
                break
        # Header = primera línea no vacía
        first_nonempty_idx = next((i for i, ln in enumerate(lines) if ln.strip()), None)
        if first_nonempty_idx is None:
            return []
        header = lines[first_nonempty_idx].strip()
        meta = parse_header(header)
        if not meta:
            return []  # sin header válido, ignorar

        # Rango del cuerpo (entre después del header y antes del link si aplica)
        end_idx = len(lines)
        if link:
            # toma hasta la línea del link exclusiva
            end_idx = next((i for i, ln in enumerate(lines) if ln.strip() == link), len(lines))
        body_lines = lines[first_nonempty_idx + 1:end_idx]

        # Ignorar una posible línea de "sección" (ENTRELÍNEAS, CAJA NEGRA) si está sola
        if body_lines and body_lines[0].strip() and body_lines[0].strip().upper() == body_lines[0].strip() and len(body_lines) >= 1:
            # Mantenerla en el cuerpo; no la quitamos para no perder contexto
            pass

        paragraphs = split_paragraphs(body_lines)
        items: List[Dict] = []
        for p in paragraphs:
            if not p.strip():
                continue
            cuerpo, icons = clean_leading_icons(p)
            color = choose_severity([ic for ic in icons if ic in ICON_SET])
            items.append({
                "color": color,
                "actors": meta["actors"],
                "alcance": meta["alcance"],
                "medio": meta["medio"],
                "cuerpo": cuerpo,
                "link": link,
                "ts": ahora_tz(),
            })
        # Si no se detectó ningún párrafo (todo vacío), crear una entrada neutra con el header
        if not items:
            items.append({
                "color": "🟡",
                "actors": meta["actors"],
                "alcance": meta["alcance"],
                "medio": meta["medio"],
                "cuerpo": "",
                "link": link,
                "ts": ahora_tz(),
            })
        return items
    except Exception as e:
        logging.error(f"Error parseando mensaje: {e}")
        return []

def filtrar_por_rango(cols: List[Dict], start: datetime, end: datetime) -> List[Dict]:
    return [c for c in cols if start <= c["ts"] < end]

def generar_resumen(cols: List[Dict], titulo_hora: str) -> str:
    if not cols:
        return "🔵 COLUMNAS / Hoy no se recibieron columnas en el periodo solicitado."

    total = len(cols)
    colores = {"🟢": 0, "🟡": 0, "🔴": 0, "⚠️": 0}
    actores: Dict[str, Dict[str, int]] = {}

    for it in cols:
        colores[it["color"]] = colores.get(it["color"], 0) + 1
        for a in it["actors"]:
            if a not in actores:
                actores[a] = {"🟢": 0, "🟡": 0, "🔴": 0, "⚠️": 0, "total": 0}
            actores[a][it["color"]] += 1
            actores[a]["total"] += 1

    out = f"## 🔵 COLUMNAS / {titulo_hora}\n\n"

    out += "### 🚦 Semáforo\n"
    out += f"🟢 Positivas: {colores.get('🟢', 0)}\n"
    out += f"🟡 Neutras:   {colores.get('🟡', 0)}\n"
    out += f"🔴 Negativas: {colores.get('🔴', 0)}\n"
    out += f"⚠️ Alertas:   {colores.get('⚠️', 0)}\n"
    out += f"**Total entradas: {total}**\n\n"

    out += "### 👥 Actores top\n```\n"
    for actor, data in sorted(actores.items(), key=lambda x: x[1]["total"], reverse=True):
        out += (f"{actor:<22} "
                f"🟢{data['🟢']} 🟡{data['🟡']} 🔴{data['🔴']} ⚠️{data['⚠️']} "
                f"| Total {data['total']}\n")
    out += "```\n\n"

    out += "### 🔗 Links\n"
    seen = set()
    for it in cols:
        link = it.get("link", "")
        if link and link not in seen:
            seen.add(link)
            medio_fmt = it['medio'].replace("*", "")
            actors_str = ", ".join(it["actors"])
            alcance = f" ({it['alcance']})" if it['alcance'] else ""
            out += f"{it['color']} *{medio_fmt}*{alcance} – {actors_str}: [Abrir]({link})\n"
    return out

def armar_alerta(it: Dict) -> str:
    frase = (it.get("cuerpo") or "").replace("\n", " ").strip()
    if len(frase) > 160:
        frase = frase[:160] + "…"
    actors_str = ", ".join(it["actors"])
    alcance = f" ({it['alcance']})" if it['alcance'] else ""
    alerta = (
        f"[ALERTA {it['color']}] {actors_str}\n"
        f"Medio: {it['medio']}{alcance}\n\n"
        f"Frase clave:\n\"{frase}\"\n\n"
    )
    if it.get("link"):
        alerta += f"🔗 [Abrir nota]({it['link']})"
    return alerta

# =========================
# Handlers
# =========================
@dp.message(Command("resumen_hoy"))
async def cmd_resumen_hoy(message: Message):
    """Resumen de TODO lo recibido HOY (00:00 → ahora)."""
    now = ahora_tz()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    subset = filtrar_por_rango(all_items, start, now)
    titulo = now.strftime("%a %d %b %Y – %H:%M")
    await bot.send_message(SUMMARY_CHAT_ID, generar_resumen(subset, titulo), parse_mode="Markdown")

@dp.message(Command("links"))
async def cmd_links(message: Message):
    """Lista solo los links de HOY (únicos)."""
    now = ahora_tz()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    subset = filtrar_por_rango(all_items, start, now)
    links = []
    seen = set()
    for it in subset:
        if it.get("link") and it["link"] not in seen:
            seen.add(it["link"])
            links.append(f"{it['color']} {it['medio']} – {', '.join(it['actors'])}: {it['link']}")
    if not links:
        await bot.send_message(SUMMARY_CHAT_ID, "🔗 Hoy no hay links registrados.")
        return
    await bot.send_message(SUMMARY_CHAT_ID, "### 🔗 Links de hoy\n" + "\n".join(links), parse_mode="Markdown")

@dp.message()
async def recibir_columnas(message: Message):
    """Ingesta de columnas desde el grupo origen."""
    try:
        if SOURCE_CHAT_ID and str(message.chat.id) != str(SOURCE_CHAT_ID):
            return

        texto = message.text or ""
        if "/" not in texto:
            return  # sin encabezado con '/', ignorar

        items = parse_message(texto)
        if not items:
            return

        # Guardar TODO (para /resumen_hoy)
        all_items.extend(items)

        # Enviar alertas de inmediato por cada párrafo rojo/alerta
        for it in items:
            if it["color"] in ("🔴", "⚠️") and ALERTS_CHAT_ID:
                try:
                    await bot.send_message(ALERTS_CHAT_ID, armar_alerta(it), parse_mode="Markdown")
                except Exception as e:
                    logging.error(f"Error enviando alerta: {e}")

    except Exception as e:
        logging.error(f"Error en recibir_columnas: {e}")

# =========================
# Tarea programada 08:30 (solo 06:00–08:00)
# =========================
async def enviar_resumen_autom():
    now = ahora_tz()
    start = now.replace(hour=6, minute=0, second=0, microsecond=0)
    end   = now.replace(hour=8, minute=0, second=0, microsecond=0)
    subset = filtrar_por_rango(all_items, start, end)
    titulo = now.strftime("%a %d %b %Y – 08:30")
    await bot.send_message(SUMMARY_CHAT_ID, generar_resumen(subset, titulo), parse_mode="Markdown")

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
