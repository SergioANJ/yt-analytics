"""
llm/hybrid_agent.py
====================
Agente híbrido que combina:
  1. RAG sobre PDFs de la cuenta (contexto estratégico / reportes)
  2. Consulta SQL directa a la BD (métricas en tiempo real)
  3. GPT para generar la respuesta final

Flujo por pregunta:
  usuario → agente → [SQL si es sobre métricas] + [RAG si hay PDFs]
         → GPT (contexto combinado) → respuesta

El agente decide automáticamente si ejecutar SQL basándose en keywords
detectados en la pregunta. Si la BD no tiene datos relevantes, solo
usa el contexto de PDFs (y viceversa).
"""

import os
import re
import logging
from typing import Optional, Callable

import pandas as pd
from sqlalchemy import Engine, text
from langchain_community.vectorstores import FAISS
from openai import OpenAI

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import LLM_MODEL, LLM_MAX_TOKENS, LLM_TEMPERATURE
from llm.rag_pipeline import buscar_contexto

log = logging.getLogger(__name__)

# ── Palabras clave que activan la consulta SQL ─────────────
SQL_KEYWORDS = {
    "views", "vistas", "revenue", "ingresos", "ingreso", "watch time",
    "watchtime", "cpm", "likes", "suscriptores", "subscriptores",
    "shorts", "videos", "lives", "dispositivo", "móvil", "computador",
    "tablet", "tv", "tráfico", "búsqueda", "búsquedas", "canal", "canales",
    "métrica", "métricas", "rendimiento", "datos", "cuánto", "cuántos",
    "cuántas", "total", "promedio", "mes", "año", "trimestre", "período",
}


def _necesita_sql(pregunta: str) -> bool:
    """Detecta si la pregunta requiere datos de la BD."""
    p_lower = pregunta.lower()
    return any(kw in p_lower for kw in SQL_KEYWORDS)


def _extraer_rango_fechas(pregunta: str) -> tuple:
    """
    Detecta año, mes o trimestre mencionados en la pregunta y
    devuelve (fecha_inicio, fecha_fin) como strings 'YYYY-MM-DD'.
    Si no detecta nada, devuelve el mes actual completo.
    """
    import re
    from datetime import date
    from dateutil.relativedelta import relativedelta

    p = pregunta.lower()
    hoy = date.today()

    MESES = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
        "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
        "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
    }
    TRIMESTRES = {
        "primer trimestre": (1, 3), "q1": (1, 3),
        "segundo trimestre": (4, 6), "q2": (4, 6),
        "tercer trimestre": (7, 9), "q3": (7, 9),
        "cuarto trimestre": (10, 12), "q4": (10, 12),
    }

    # Detectar año
    anios = re.findall(r"\b(202[0-9])\b", p)
    anio = int(anios[0]) if anios else hoy.year

    # Detectar trimestre
    for key, (m_ini, m_fin) in TRIMESTRES.items():
        if key in p:
            ini = date(anio, m_ini, 1)
            fin = date(anio, m_fin, 1) + relativedelta(months=1) - relativedelta(days=1)
            return ini.isoformat(), fin.isoformat()

    # Detectar mes específico
    for nombre, num in MESES.items():
        if nombre in p:
            ini = date(anio, num, 1)
            fin = ini + relativedelta(months=1) - relativedelta(days=1)
            return ini.isoformat(), fin.isoformat()

    # Detectar "año completo" sin mes
    if anios and not any(m in p for m in MESES):
        return f"{anio}-01-01", f"{anio}-12-31"

    # Sin fecha detectada → últimos 30 días
    ini = hoy - relativedelta(days=30)
    return ini.isoformat(), hoy.isoformat()


def _ejecutar_sql_seguro(engine: Engine, id_cuenta: int,
                          pregunta: str) -> str:
    """
    Genera y ejecuta consultas SQL según la pregunta del usuario.
    Detecta el rango de fechas mencionado y consulta ese período exacto.
    """
    p = pregunta.lower()
    resultados = []
    fecha_ini, fecha_fin = _extraer_rango_fechas(pregunta)

    try:
        # ── Total consolidado del período (una sola fila) ─────────
        df_total = pd.read_sql("""
            SELECT
                SUM(h.views_total)                         AS views_total,
                ROUND(SUM(h.revenue_total)::numeric, 2)    AS revenue_total,
                ROUND(SUM(h.watch_time_total)::numeric, 0) AS watch_time_h,
                ROUND(AVG(h.cpm_promedio)::numeric, 2)     AS cpm_promedio,
                SUM(h.likes_total)                         AS likes_total,
                SUM(h.suscriptores_total)                  AS suscriptores_total
            FROM hechos_metricas h
            JOIN dim_subcuenta s ON h.id_subcuenta = s.id_subcuenta
            JOIN dim_fecha     f ON h.id_fecha     = f.id_fecha
            WHERE s.id_cuenta = %s
              AND f.fecha BETWEEN %s AND %s
        """, con=engine, params=(id_cuenta, fecha_ini, fecha_fin))

        if not df_total.empty and df_total["views_total"].iloc[0]:
            fila = df_total.iloc[0]
            resultados.append(
                f"📊 TOTALES CONSOLIDADOS del período {fecha_ini} → {fecha_fin}:\n"
                f"  • Views totales:      {int(fila['views_total']):,}\n"
                f"  • Revenue total:      ${float(fila['revenue_total']):,.2f}\n"
                f"  • Watch Time (horas): {int(fila['watch_time_h']):,}\n"
                f"  • CPM promedio:       ${float(fila['cpm_promedio']):,.2f}\n"
                f"  • Likes totales:      {int(fila['likes_total']):,}\n"
                f"  • Suscriptores netos: {int(fila['suscriptores_total']):,}"
            )
        else:
            resultados.append(
                f"ℹ️ No hay datos para el período {fecha_ini} → {fecha_fin}."
            )
            return "\n\n".join(resultados)

        # ── Desglose por subcuenta ────────────────────────────────
        df_subs = pd.read_sql("""
            SELECT
                s.nombre_subcuenta,
                SUM(h.views_total)                         AS views,
                ROUND(SUM(h.revenue_total)::numeric, 2)    AS revenue,
                ROUND(SUM(h.watch_time_total)::numeric, 0) AS watch_time_h
            FROM hechos_metricas h
            JOIN dim_subcuenta s ON h.id_subcuenta = s.id_subcuenta
            JOIN dim_fecha     f ON h.id_fecha     = f.id_fecha
            WHERE s.id_cuenta = %s
              AND f.fecha BETWEEN %s AND %s
            GROUP BY s.nombre_subcuenta
            ORDER BY views DESC
        """, con=engine, params=(id_cuenta, fecha_ini, fecha_fin))

        if not df_subs.empty and len(df_subs) > 1:
            resultados.append(
                f"\n📺 Desglose por subcuenta (estos valores ya están incluidos en el total):\n"
                + df_subs.to_string(index=False)
            )

        # ── Desglose mensual si pregunta por año completo ─────────
        if re.search(r"\b202[0-9]\b", p) and not any(
            m in p for m in ["enero","febrero","marzo","abril","mayo","junio",
                              "julio","agosto","septiembre","octubre","noviembre","diciembre",
                              "trimestre","q1","q2","q3","q4"]
        ):
            df_mensual = pd.read_sql("""
                SELECT
                    f.mes,
                    f.nombre_mes,
                    SUM(h.views_total)                      AS views,
                    ROUND(SUM(h.revenue_total)::numeric, 2) AS revenue
                FROM hechos_metricas h
                JOIN dim_subcuenta s ON h.id_subcuenta = s.id_subcuenta
                JOIN dim_fecha     f ON h.id_fecha     = f.id_fecha
                WHERE s.id_cuenta = %s
                  AND f.fecha BETWEEN %s AND %s
                GROUP BY f.mes, f.nombre_mes
                ORDER BY f.mes
            """, con=engine, params=(id_cuenta, fecha_ini, fecha_fin))
            if not df_mensual.empty:
                resultados.append(
                    f"\n📅 Desglose mensual (estos valores ya están incluidos en el total):\n"
                    + df_mensual.to_string(index=False)
                )

        # ── Top videos ────────────────────────────────────────────
        if any(kw in p for kw in ["video", "contenido", "top", "popular", "visto", "título"]):
            df_videos = pd.read_sql("""
                SELECT title, SUM(views) AS views, geo_group
                FROM vw_top_videos
                WHERE id_cuenta = %s
                  AND fecha_inicio <= %s AND fecha_fin >= %s
                GROUP BY title, geo_group
                ORDER BY views DESC
                LIMIT 10
            """, con=engine, params=(id_cuenta, fecha_fin, fecha_ini))
            if not df_videos.empty:
                resultados.append("\n🎬 Top videos:\n" + df_videos.to_string(index=False))

        # ── Términos de búsqueda ──────────────────────────────────
        if any(kw in p for kw in ["búsqueda", "buscan", "busca", "término", "search"]):
            df_terms = pd.read_sql("""
                SELECT search_term, SUM(views) AS views, geo_group
                FROM vw_search_terms
                WHERE id_cuenta = %s
                  AND fecha_inicio <= %s AND fecha_fin >= %s
                GROUP BY search_term, geo_group
                ORDER BY views DESC
                LIMIT 15
            """, con=engine, params=(id_cuenta, fecha_fin, fecha_ini))
            if not df_terms.empty:
                resultados.append("\n🔍 Términos de búsqueda:\n" + df_terms.to_string(index=False))

        # ── Demografía ────────────────────────────────────────────
        if any(kw in p for kw in ["edad", "género", "genero", "demograf", "hombre", "mujer"]):
            df_demo = pd.read_sql("""
                SELECT age_group, gender, ROUND(SUM(viewer_pct)::numeric, 2) AS pct
                FROM vw_demograficos
                WHERE id_cuenta = %s AND geo_group = 'WW'
                  AND fecha_inicio <= %s AND fecha_fin >= %s
                GROUP BY age_group, gender
                ORDER BY age_group, gender
            """, con=engine, params=(id_cuenta, fecha_fin, fecha_ini))
            if not df_demo.empty:
                resultados.append("\n👥 Demografía WW:\n" + df_demo.to_string(index=False))

        # ── Proyecciones vs real ──────────────────────────────────
        if any(kw in p for kw in ["proyecc", "meta", "objetivo", "cumplimiento"]):
            anios = re.findall(r"\b(202[0-9])\b", p)
            anio_proj = int(anios[0]) if anios else date.today().year
            df_proj = pd.read_sql("""
                SELECT
                    p.mes,
                    p.views_proyectadas,
                    COALESCE(SUM(h.views_total), 0) AS views_real,
                    ROUND(COALESCE(SUM(h.views_total),0)
                          / NULLIF(p.views_proyectadas,0) * 100, 1) AS pct_cumplimiento
                FROM proyecciones_mensuales p
                LEFT JOIN vw_metricas h
                    ON h.id_cuenta = p.id_cuenta
                   AND EXTRACT(MONTH FROM h.fecha) = p.mes
                   AND EXTRACT(YEAR  FROM h.fecha) = p.anio
                WHERE p.id_cuenta = %s AND p.anio = %s
                GROUP BY p.mes, p.views_proyectadas
                ORDER BY p.mes
            """, con=engine, params=(id_cuenta, anio_proj))
            if not df_proj.empty:
                resultados.append(
                    f"\n📆 Proyecciones vs Real {anio_proj}:\n"
                    + df_proj.to_string(index=False)
                )

    except Exception as e:
        log.warning(f"Error ejecutando SQL para el agente: {e}")

    return "\n\n".join(resultados)


# ── System prompt del agente ──────────────────────────────────────────────────
SYSTEM_PROMPT = """Eres un analista de datos experto en YouTube Analytics para medios de comunicación.
Tu rol es ayudar a los equipos de contenido a entender sus métricas y tomar decisiones estratégicas.

Tienes acceso a dos fuentes de información:
1. DATOS DE BASE DE DATOS: métricas reales de YouTube Analytics. Tienes datos históricos
   completos — puedes responder preguntas sobre cualquier mes, trimestre o año disponible
   en la BD, sin límite de fechas. Si la pregunta menciona un período específico, los datos
   ya vienen filtrados por ese período exacto.
2. CONTEXTO DE DOCUMENTOS: reportes, estrategias y materiales internos de la cuenta.

Reglas:
- Responde siempre en español
- Sé preciso con los números: usa separadores de miles (,) y 2 decimales para revenue
- Si los datos están disponibles en el contexto, cítalos explícitamente con los números exactos
- NUNCA digas que no tienes acceso a datos de una fecha si los datos vienen en el contexto
- Si el contexto dice "No hay datos para el período X", entonces sí puedes indicar que
  ese período no está cargado en la BD
- Ofrece interpretaciones y recomendaciones accionables cuando sea relevante
- Mantén un tono profesional pero accesible
- Máximo 200 palabras por respuesta salvo que el usuario pida un análisis detallado
"""


def crear_agente_hibrido(
    id_cuenta: int,
    engine: Engine,
    vectorstore: Optional[FAISS],
) -> Callable[[str], str]:
    """
    Retorna una función callable que acepta una pregunta y devuelve la respuesta.

    Args:
        id_cuenta:    ID de la cuenta del usuario logueado.
        engine:       SQLAlchemy engine conectado a la BD.
        vectorstore:  Índice FAISS con los PDFs (puede ser None).

    Returns:
        Función agente: pregunta (str) → respuesta (str)
    """
    client = OpenAI()   # usa OPENAI_API_KEY del entorno automáticamente

    def agente(pregunta: str) -> str:
        contexto_sql = ""
        contexto_pdf = ""

        # 1. Consulta SQL si la pregunta es sobre métricas
        if _necesita_sql(pregunta):
            contexto_sql = _ejecutar_sql_seguro(engine, id_cuenta, pregunta)

        # 2. Búsqueda semántica en PDFs
        if vectorstore is not None:
            contexto_pdf = buscar_contexto(vectorstore, pregunta, k=4)

        # 3. Construir el mensaje de usuario con el contexto
        partes = [f"Pregunta del usuario: {pregunta}"]

        if contexto_sql:
            partes.append(f"\n--- DATOS DE BASE DE DATOS ---\n{contexto_sql}")
        if contexto_pdf:
            partes.append(f"\n--- CONTEXTO DE DOCUMENTOS INTERNOS ---\n{contexto_pdf}")
        if not contexto_sql and not contexto_pdf:
            partes.append("\n(No se encontraron datos específicos. Responde con tu conocimiento general.)")
        
        # TEMPORAL — para diagnóstico, quítalo después
        print("=== CONTEXTO SQL ===")
        print(contexto_sql)
        print("=== FIN CONTEXTO ===")
        
        mensaje_usuario = "\n".join(partes)

        # 4. Llamada a la API de OpenAI
        try:
            response = client.chat.completions.create(
                model=LLM_MODEL,
                max_tokens=LLM_MAX_TOKENS,
                temperature=LLM_TEMPERATURE,
                messages=[
                    {"role": "system",  "content": SYSTEM_PROMPT},
                    {"role": "user",    "content": mensaje_usuario},
                ],
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            log.error(f"Error llamando a OpenAI: {e}")
            return (
                "⚠️ No pude generar una respuesta en este momento. "
                f"Detalle técnico: {str(e)}"
            )

    return agente