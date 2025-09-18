import os
import re
import logging
import asyncio
from typing import Dict, List, Tuple, Set
from datetime import datetime
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
    return datetime.now(tz)

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
# Memoria
# =========================
all_items: List[Dict] = []

# =========================
# Utilidades & parsing
# =========================
ICON_SET = ("🟢", "🟡", "🔴", "⚠️")
URL_RE = re.compile(r"(https?://[^\s]+)")
WS = re.compile(r"\s+")

def limpiar(s: str) -> str:
    return WS.sub(" ", (s or "").strip(" \n\r\t-—·*"))

def detectar_color(texto: str) -> str:
    if "🔴" in texto: return "🔴"
    if "⚠️" in texto or "⚠" in texto: return "⚠️"
    if "🟢" in texto: return "🟢"
    if "🟡" in texto: return "🟡"
    return "🟡"

def normalizar_token(tok: str) -> str:
    return limpiar(tok.upper())

def hash_columna(link: str) -> str:
    return hashlib.sha1(link.encode("utf-8")).hexdigest()

def extraer_header_y_cuerpo(texto: str) -> Tuple[str, str]:
    lineas = texto.strip().splitlines()
    if not lineas: return "", ""
    header = limpiar(lineas[0])
    cuerpo = limpiar("\n".join(lineas[1:]))
    return header, cuerpo

# -------- Canonización de actores --------
# Accentos fuera para el matching
def _sin_acentos(s: str) -> str:
    return (s
            .replace("Á","A").replace("É","E").replace("Í","I")
            .replace("Ó","O").replace("Ú","U").replace("Ü","U")
            .replace("Ñ","N"))

# Palabras de rol a eliminar al inicio
ROLES = {
    "ALCALDE","ALCALDESA","PRESIDENTE MUNICIPAL","PRESIDENTA MUNICIPAL",
    "GOBERNADOR","GOBERNADORA","DIPUTADO","DIPUTADA","SENADOR","SENADORA",
    "FISCAL","SECRETARIO","SECRETARIA","REGIDOR","REGIDORA"
}

def quitar_rol(s: str) -> str:
    t = normalizar_token(s)
    for r in sorted(ROLES, key=len, reverse=True):
        pref = r + " "
        if t.startswith(pref):
            t = t[len(pref):]
            break
    return t

# Alias (ejemplos más comunes; puedes ampliarlo cuando quieras)
ALIASES = {
    "MARCO BONILLA":"MARCO BONILLA",
    "BONILLA":"MARCO BONILLA",
    "ALCALDE BONILLA":"MARCO BONILLA",
    "MARCO BONILLA MENDOZA":"MARCO BONILLA",
    "ALCALDE MARCO BONILLA":"MARCO BONILLA",
    "ALCALDE MARCO":"MARCO BONILLA",
    "ALCALDE DE CHIHUAHUA":"MARCO BONILLA",

    "CRUZ PEREZ CUELLAR":"CRUZ PÉREZ CUÉLLAR",
    "CRUZ PÉREZ CUÉLLAR":"CRUZ PÉREZ CUÉLLAR",
    "PEREZ CUELLAR":"CRUZ PÉREZ CUÉLLAR",
    "CRUZ PEREZ":"CRUZ PÉREZ CUÉLLAR",
    "CRUZ PÉREZ":"CRUZ PÉREZ CUÉLLAR",

    "MARU CAMPOS":"MARU CAMPOS",
    "MARIA EUGENIA CAMPOS":"MARU CAMPOS",
    "MARÍA EUGENIA CAMPOS":"MARU CAMPOS",

    "CESAR JAUREGUI MORENO":"CÉSAR JÁUREGUI MORENO",
    "CÉSAR JÁUREGUI MORENO":"CÉSAR JÁUREGUI MORENO",
    "CESAR JAUREGUI":"CÉSAR JÁUREGUI MORENO",
    "JAUREGUI":"CÉSAR JÁUREGUI MORENO",
}

# Mapa sin acentos -> canon
ALIASES_NORM = { _sin_acentos(k):v for k,v in ALIASES.items() }
ACTORES_CANON_SET = set(ALIASES.values())

def canon_actor(s: str) -> str:
    if not s: return ""
    t = quitar_rol(s)
    t = normalizar_token(t)
    key = _sin_acentos(t)
    if key in ALIASES_NORM:
        return ALIASES_NORM[key]
    return t  # si no hay alias, deja el texto limpio

def partir_actores_chunk(chunk: str) -> List[str]:
    # separa por coma, slash, ' y ', ' e ', guiones
    tmp = re.split(r"[,/]| y | e | - ", chunk, flags=re.IGNORECASE)
    return [a for a in map(limpiar, tmp) if a]

ALCANCE_CATALOG = {
    "ESTATAL","LOCAL","NACIONAL",
    "JUAREZ","JUÁREZ","CHIHUAHUA",
    "MUNICIPAL","ESTADO","CD JUAREZ","CD. JUAREZ","CD. JUÁREZ",
    "ZONA CENTRO","REGIONAL"
}

def parse_header(header: str) -> Tuple[List[str], str, str]:
    clean = header
    for ic in ICON_SET:
        clean = clean.replace(ic, "")
    tokens = [limpiar(p) for p in clean.split("/") if limpiar(p)]
    if len(tokens) < 2:
        return [], "", ""

    medio = normalizar_token(tokens[-1])
    alcance = ""
    core = tokens[1:-1]

    if core:
        penult = normalizar_token(core[-1])
        if penult in ALCANCE_CATALOG:
            alcance = penult
            core = core[:-1]

    # core puede tener varios actores en un mismo chunk: separa y canoniza
    actores: List[str] = []
    for ch in core:
        for a in partir_actores_chunk(ch):
            ca = canon_actor(a)
            if ca and ca not in actores:
                actores.append(ca)

    if not actores:
        actores = ["OTROS DE INTERES"]

    return actores, alcance, medio

def parse_message(texto: str) -> List[Dict]:
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
    return [{
        "col_id": col_id,
        "fecha": ahora_tz(),
        "color": color,
        "actors": actores,
        "alcance": alcance,
        "medio": medio or "SIN MEDIO",
        "cuerpo": (cuerpo or ""),
        "link": link,
    }]

def dentro_rango(dt: datetime, start: datetime, end: datetime) -> bool:
    return (dt >= start) and (dt <= end)

def filtrar_por_rango(items: List[Dict], start: datetime, end: datetime) -> List[Dict]:
    return [it for it in items if dentro_rango(it["fecha"], start, end)]

def armar_alerta(it: Dict) -> str:
    frase = (it.get("cuerpo") or "").replace("\n", " ").strip()
    if len(frase) > 200: frase = frase[:200] + "…"
    actors_str = ", ".join(it["actors"])
    alcance = f" ({it['alcance']})" if it['alcance'] else ""
    out = (
        f"[ALERTA {it['color']}] {actors_str}\n"
        f"Medio: {it['medio']}{alcance}\n\n"
        f"Frase clave:\n\"{frase}\"\n"
    )
    if it.get("link"):
        out += f"\n🔗 Abrir nota: {it['link']}"
    return out

# =========================
# Resumen
# =========================
def contar_semaforo(items: List[Dict]) -> Tuple[int, int, int, int]:
    v = sum(1 for x in items if x["color"] == "🟢")
    a = sum(1 for x in items if x["color"] == "🟡")
    r = sum(1 for x in items if x["color"] == "🔴")
    w = sum(1 for x in items if x["color"] == "⚠️")
    return v, a, r, w

def top_actores(items: List[Dict], limite: int = 6) -> List[Tuple[str, Dict]]:
    acc: Dict[str, Dict[str, int]] = {}
    for it in items:
        for a in it["actors"]:
            d = acc.setdefault(a, {"🟢":0,"🟡":0,"🔴":0,"⚠️":0,"total":0})
            d[it["color"]] += 1
            d["total"] += 1
    orden = sorted(acc.items(), key=lambda kv: (kv[1]["total"], kv[1]["🔴"], kv[1]["⚠️"]), reverse=True)
    return orden[:limite]

def render_actores_top(top: List[Tuple[str, Dict]]) -> str:
    if not top: return "—"
    lines = ["👥 Actores top","-------------------------"]
    for actor, data in top:
        lines.append(f"{actor}\n| Total {data['total']}   🟢{data['🟢']} 🟡{data['🟡']} 🔴{data['🔴']} ⚠️{data['⚠️']}")
    return "\n".join(lines)

def medios_con_publicacion(items: List[Dict]) -> List[str]:
    vistos: Set[str] = set()
    out: List[str] = []
    for it in items:
        m = it["medio"]
        if m and m not in vistos:
            vistos.add(m); out.append(m)
    return out

# -------- Temas (n-grams) con filtros --------
STOP_ES = {
    "LA","EL","LOS","LAS","DE","DEL","AL","A","Y","O","U","EN","POR","PARA","CON",
    "SE","QUE","SU","SUS","UN","UNA","UNOS","UNAS","LO","LES","YA","NO","SI","SÍ",
    "MAS","MÁS","COMO","ES","SON","SER","FUE","HAN","HAY","ESTE","ESTA","ESTOS","ESTAS",
    "ESE","ESA","AQUEL","AQUELLA","ANTE","BAJO","CABE","HACIA","HASTA","TRAS","ENTRE",
    "SOBRE","MUY","TAMBIÉN","TAMBIEN","PERO","NI","SINO","LE","DEBE","DEBEN","DEBEMOS",
    # basura típica de medios/links
    "HTTPS","HTTP","WWW","COM","MX","NOTICIAS","ENTRELÍNEAS","ENTRELINEAS",
    "OMNIA","PARADOJA","HERALDO","RED","VOZ","NOTA","COLUMNA","OPINIÓN","OPINION",
    "OTROS","INTERES","INTERÉS","SIN","MEDIO"
}
ROLES_BAN = { _sin_acentos(r) for r in ROLES }  # evita que salgan como temas

def _tokenizar(texto: str) -> List[str]:
    t = re.sub(r"[^A-ZÁÉÍÓÚÜÑ0-9 ]", " ", (texto or "").upper())
    w = [w for w in t.split() if len(w) >= 3]
    out = []
    for x in w:
        sx = _sin_acentos(x)
        if sx in STOP_ES or sx in ROLES_BAN:
            continue
        out.append(x)
    return out

def _ngrams(words: List[str], n: int) -> List[str]:
    if len(words) < n: return []
    return [" ".join(words[i:i+n]) for i in range(len(words)-n+1)]

def extraer_temas(items: List[Dict], max_temas: int = 5) -> List[str]:
    from collections import Counter, defaultdict
    cnt = Counter()
    tema_en_medio = defaultdict(set)

    actores_can_sin_acento = { _sin_acentos(a) for a in ACTORES_CANON_SET }

    for it in items:
        cuerpo = (it.get("cuerpo") or "").replace("\n", " ")
        base = limpiar(cuerpo)
        words = _tokenizar(base)
        grams = _ngrams(words, 3) + _ngrams(words, 2)

        # filtra n-grams que contengan actores (para que temas no sean nombres)
        grams_filtrados = []
        for g in grams:
            g_sin = _sin_acentos(g)
            if any(a in g_sin for a in actores_can_sin_acento):
                continue
            if g_sin in STOP_ES:  # por si algún bigrama cae completo en stop
                continue
            grams_filtrados.append(g)

        for g in set(grams_filtrados):  # únicos por documento
            cnt[g] += 1
            tema_en_medio[g].add(it.get("medio","SIN MEDIO"))

    if not cnt: return []

    # prioriza por frecuencia y por longitud (más específico)
    top = sorted(cnt.items(), key=lambda kv: (kv[1], len(kv[0])), reverse=True)

    elegidos: List[str] = []
    bases: Set[str] = set()
    for g, _ in top:
        base = _sin_acentos(g)
        # evita duplicados cercanos
        if any(base.startswith(b) or b.startswith(base) for b in bases):
            continue
        bases.add(base)
        elegidos.append(g)
        if len(elegidos) >= max_temas:
            break

    salida = []
    for g in elegidos:
        menc = cnt[g]
        medios = ", ".join(sorted(list(tema_en_medio[g]))[:4])
        salida.append(f"{g.title()} (menciones: {menc}) — medios: {medios}")
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
        render_actores_top(top_actores(items, 6)),
        ""
    ]

    medios = medios_con_publicacion(items)
    if medios:
        lines.append("📰 Medios con publicación")
        for m in medios:
            lines.append(f"- {m}")
        lines.append("")

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
        if not link: continue
        if it["col_id"] in vistos: continue
        vistos.add(it["col_id"])
        lines.append(f"{it['color']} {', '.join(it['actors'])} – {it['medio']}\n{link}")

    if not lines:
        await bot.send_message(SUMMARY_CHAT_ID, "🔗 Hoy no hay links registrados.", disable_web_page_preview=True)
        return

    await bot.send_message(SUMMARY_CHAT_ID, "🔗 Links de hoy\n\n" + "\n\n".join(lines), disable_web_page_preview=True)

@dp.message()
async def recibir_columnas(message: Message):
    try:
        if SOURCE_CHAT_ID and str(message.chat.id) != str(SOURCE_CHAT_ID):
            return

        texto = message.text or ""
        if "/" not in texto:
            return

        items = parse_message(texto)
        if not items:
            return

        existentes = {it["col_id"] for it in all_items}
        nuevos = [it for it in items if it["col_id"] not in existentes]
        if not nuevos:
            return

        all_items.extend(nuevos)

        for it in nuevos:
            if it["color"] in ("🔴", "⚠️") and ALERTS_CHAT_ID:
                try:
                    await bot.send_message(ALERTS_CHAT_ID, armar_alerta(it), disable_web_page_preview=True)
                except Exception as e:
                    logging.error(f"Error enviando alerta: {e}")

    except Exception as e:
        logging.error(f"Error en recibir_columnas: {e}")

# =========================
# Tarea programada 08:30 (rango 06–08)
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
