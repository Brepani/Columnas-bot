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
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")   # Chat donde sale el resumen
ALERTS_CHAT_ID = os.getenv("ALERTS_CHAT_ID")     # Chat donde se mandan alertas 🔴/⚠️
SOURCE_CHAT_ID = os.getenv("SOURCE_CHAT_ID")     # Chat del que se leen las columnas

TIMEZONE = "America/Chihuahua"
tz = pytz.timezone(TIMEZONE)

def ahora_tz() -> datetime:
    """Hora actual con timezone configurada."""
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
# Un item por COLUMNA (un link = una columna)
# Campos: col_id, fecha, color, actors (list), alcance, medio, cuerpo, link
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
    """Respeta color si viene en el mensaje; default 🟡."""
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
    """Primera línea = encabezado con slashes; resto = cuerpo."""
    lineas = texto.strip().splitlines()
    if not lineas:
        return "", ""
    header = limpiar(lineas[0])
    cuerpo = limpiar("\n".join(lineas[1:]))
    return header, cuerpo

# -------- Alcance & header robusto --------
ALCANCE_CATALOG = {
    "ESTATAL", "LOCAL", "NACIONAL",
    "JUAREZ", "JUÁREZ", "CHIHUAHUA",
    "MUNICIPAL", "ESTADO", "CD JUAREZ", "CD. JUAREZ", "CD. JUÁREZ",
    "ZONA CENTRO", "REGIONAL"
}

def parse_header(header: str) -> Tuple[List[str], str, str]:
    """
    Formatos tolerados:
      - CLASIF / ACTOR(ES) / ALCANCE / MEDIO
      - CLASIF / ACTOR(ES) / MEDIO
    Reglas:
      • Último token = MEDIO
      • Penúltimo token es ALCANCE si ∈ ALCANCE_CATALOG
      • Lo que queda entre CLASIF y (ALCANCE|MEDIO) = actores
    """
    clean = header
    for ic in ICON_SET:
        clean = clean.replace(ic, "")
    tokens = [limpiar(p) for p in clean.split("/") if limpiar(p)]

    if len(tokens) < 2:
        return [], "", ""

    medio = normalizar_token(tokens[-1])
    alcance = ""
    core = tokens[1:-1]  # entre CLASIF y MEDIO

    if core:
        penult = normalizar_token(core[-1])
        if penult in ALCANCE_CATALOG:
            alcance = penult
            core = core[:-1]

    actores = [normalizar_token(x) for x in core] or ["OTROS DE INTERES"]
    return actores, alcance, medio

def parse_message(texto: str) -> List[Dict]:
    """
    Parsea un mensaje completo de Telegram (una columna por link):
      - Header en 1a línea (con '/')
      - Último URL del mensaje = link de la columna
      - color: por emojis (🔴 ⚠️ 🟢 🟡)
    """
    if not texto:
        return []
    header, cuerpo = extraer_header_y_cuerpo(texto)
    if not header:
        return []

    actores, alcance, medio = parse_header(header)
    urls = URL_RE.findall(texto)
    link = urls[-1] if urls else ""
    if not link:
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
        "cuerpo": (cuerpo or ""),
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

def top_actores(items: List[Dict], limite: int = 6) -> List[Tuple[str, Dict]]:
    acc: Dict[str, Dict[str, int]] = {}
    for it in items:
        for a in it["actors"]:
            d = acc.setdefault(a, {"🟢": 0, "🟡": 0, "🔴": 0, "⚠️": 0, "total": 0})
            d[it["color"]] += 1
            d["total"] += 1
    orden = sorted(acc.items(), key=lambda kv: (kv[1]["total"], kv[1]["🔴"], kv[1]["⚠️"]), reverse=True)
    return orden[:limite]

def render_actores_top(top: List[Tuple[str, Dict]]) -> str:
    if not top:
        return "—"
    lines = ["👥 Actores top", "-------------------------"]
    for actor, data in top:
        lines.append(f"{actor}\n| Total {data['total']}   🟢{data['🟢']} 🟡{data['🟡']} 🔴{data['🔴']} ⚠️{data['⚠️']}")
    return "\n".join(lines)

def medios_con_publicacion(items: List[Dict]) -> List[str]:
    vistos: Set[str] = set()
    out: List[str] = []
    for it in items:
        m = it["medio"]
        if m and m not in vistos:
            vistos.add(m)
            out.append(m)
    return out

# -------- Helpers para temas (n-grams) --------
STOP_ES = {
    "LA","EL","LOS","LAS","DE","DEL","AL","A","Y","O","U","EN","POR","PARA","CON",
    "SE","QUE","SU","SUS","UN","UNA","UNOS","UNAS","LO","LES","YA","NO","SI","SÍ",
    "MAS","MÁS","COMO","ES","SON","SER","FUE","HAN","HAY","ESTE","ESTA","ESTOS","ESTAS",
    "ESE","ESA","AQUEL","AQUELLA","ANTE","BAJO","CABE","HACIA","HASTA","TRAS","ENTRE",
    "SOBRE","MUY","TAMBIÉN","TAMBIEN","PERO","NI","SINO","LE","DEBE","DEBEN","DEBEMOS"
}

def _tokenizar(texto: str) -> List[str]:
    t = re.sub(r"[^A-ZÁÉÍÓÚÜÑ0-9 ]", " ", (texto or "").upper())
    words = [w for w in t.split() if len(w) >= 3 and w not in STOP_ES]
    return words

def _ngrams(words: List[str], n: int) -> List[str]:
    if len(words) < n:
        return []
    return [" ".join(words[i:i+n]) for i in range(len(words)-n+1)]

def extraer_temas(items: List[Dict], max_temas: int = 5) -> List[str]:
    """
    Nuevo extractor de TEMAS:
      • Une cuerpos + encabezados breves
      • Genera bigramas y trigramas sin stopwords
      • Cuenta frecuencia (por columna)
      • Devuelve 'Tema (menciones) — medios: ...'
    """
    corpus: List[Tuple[str, str]] = []  # (texto, medio)
    for it in items:
        cuerpo = (it.get("cuerpo") or "").replace("\n", " ")
        breve = " ".join(it.get("actors", [])) + " " + it.get("alcance", "")
        txt = limpiar(breve + " " + cuerpo)
        corpus.append((txt, it.get("medio", "SIN MEDIO")))

    from collections import Counter, defaultdict
    cnt = Counter()
    tema_en_medio = defaultdict(set)

    for txt, medio in corpus:
        words = _tokenizar(txt)
        grams = _ngrams(words, 2) + _ngrams(words, 3)
        grams = [g for g in grams if not all(w in STOP_ES for w in g.split())]
        for g in set(grams):  # únicos por documento
            cnt[g] += 1
            tema_en_medio[g].add(medio)

    if not cnt:
        return []

    top = sorted(cnt.items(), key=lambda kv: (kv[1], len(kv[0])), reverse=True)[: max_temas * 3]

    elegidos: List[str] = []
    base_seen: Set[str] = set()
    for g, _ in top:
        base = g[:50]
        if base in base_seen:
            continue
        base_seen.add(base)
        elegidos.append(g)
        if len(elegidos) >= max_temas:
            break

    salida = []
    for g in elegidos:
        menciones = cnt[g]
        medios = ", ".join(sorted(list(tema_en_medio[g]))[:4])
        salida.append(f"{g.title()} (menciones: {menciones}) — medios: {medios}")
    return salida

def generar_resumen(items: List[Dict], titulo: str) -> str:
    if not items:
        return f"🔵 COLUMNAS AM / {titulo}\nHoy no se recibieron columnas en el periodo solicitado."

    v, a, r, w = contar_semaforo(items)
    total = len(items)

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
        # Solo del chat fuente
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
