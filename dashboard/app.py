"""
dashboard/app.py  —  Punto de entrada del dashboard Streamlit
=============================================================
Ejecutar con:
  streamlit run dashboard/app.py
"""

import streamlit as st
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.settings import CUENTAS_CON_USPR
from dashboard import queries as Q
from dashboard import components as C
from dotenv import load_dotenv
import os

load_dotenv() # Esto lee el archivo .env
# ============================================================
# CONFIG
# ============================================================
st.set_page_config(
    page_title="YouTube Analytics Dashboard",
    page_icon="assets/logo.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# CSS MÍNIMO
# ============================================================
st.markdown("""
<style>
    [data-testid="stMetric"] {
        background-color: #f7f8fa;
        border-radius: 8px;
        padding: 12px 16px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    }
    .block-container { padding-top: 1.5rem; }
</style>
""", unsafe_allow_html=True)

# ============================================================
# LOGIN
# ============================================================
st.sidebar.header("🔐 Iniciar sesión")

if "usuario" not in st.session_state:
    nombre_input = st.sidebar.text_input("Usuario")
    pass_input   = st.sidebar.text_input("Contraseña", type="password")
    if st.sidebar.button("Entrar", use_container_width=True):
        usuario = Q.verificar_usuario(nombre_input, pass_input)
        if usuario is not None:
            st.session_state["usuario"]      = usuario
            st.session_state["nombre_cuenta"] = Q.get_nombre_cuenta(usuario["id_cuenta"])
            st.rerun()
        else:
            st.sidebar.error("Usuario o contraseña incorrectos")
else:
    st.sidebar.success(f"✅ {st.session_state['usuario']['nombre_usuario']}")
    if st.sidebar.button("Cerrar sesión", use_container_width=True):
        st.session_state.clear()
        st.rerun()

if "usuario" not in st.session_state:
    st.title("📊 YouTube Analytics Dashboard")
    st.info("🔒 Inicia sesión desde el panel lateral para acceder a tu dashboard.")
    
    st.markdown("<br><br><br><br><br>", unsafe_allow_html=True)
    try:
        col1, col2, col3 = st.columns([1, 2, 1]) # Crea 3 columnas (la del medio es más ancha)
        with col2:
            st.image("assets/logo_Betrmedia.png")
    except Exception as e:
        st.error(f"No se pudo cargar el video: {e}")
    st.stop()

# ============================================================
# VARIABLES GLOBALES DE SESIÓN
# ============================================================
id_cuenta     = int(st.session_state["usuario"]["id_cuenta"])
nombre_cuenta = st.session_state["nombre_cuenta"]
es_uspr       = nombre_cuenta in CUENTAS_CON_USPR

# ============================================================
# SIDEBAR — FILTROS
# ============================================================
subcuentas = Q.get_subcuentas(id_cuenta)

if not subcuentas.empty:
    st.sidebar.markdown("### 🧩 Subcuentas")
    sel_nombres = st.sidebar.multiselect(
        "Selecciona:",
        options=subcuentas["nombre_subcuenta"].tolist(),
        default=subcuentas["nombre_subcuenta"].tolist()
    )
    ids_sub = subcuentas[
        subcuentas["nombre_subcuenta"].isin(sel_nombres)
    ]["id_subcuenta"].tolist()
else:
    ids_sub = []

st.sidebar.markdown("### 📅 Rango de fechas")
try:
    fmin, fmax = Q.get_fecha_range(id_cuenta)
except Exception:
    st.sidebar.warning("Aún no hay datos cargados para esta cuenta.")
    fmin, fmax = None, None

if fmin and fmax:
    rango = st.sidebar.date_input("Período:", [fmin, fmax],
                                   min_value=fmin, max_value=fmax)
else:
    from datetime import date
    rango = st.sidebar.date_input("Período:")

if len(rango) != 2:
    st.info("⚠️ Selecciona fecha de inicio y fecha fin para continuar.")
    st.stop()

f0, f1 = rango

# ============================================================
# TÍTULO
# ============================================================
st.title(f"📈 Dashboard — {nombre_cuenta.title()}")
st.caption(f"Período: {f0} → {f1}")

# ============================================================
# CARGA DE DATOS DIARIOS
# ============================================================
df = Q.get_daily(id_cuenta, ids_sub, f0, f1)

if df.empty:
    st.warning("⚠️ Sin datos para el rango y subcuentas seleccionados.")
    st.stop()

# ============================================================
# PANEL ESTÁNDAR (todas las cuentas) O CON TABS (US+PR)
# ============================================================

if not es_uspr:
    # ──────────────────────────────────────────────────────
    # PANEL ESTÁNDAR — igual para todas las cuentas sin US+PR
    # ──────────────────────────────────────────────────────
    C.kpis(df)
    st.markdown("---")
    C.evolucion(df)
    st.markdown("---")
    C.tipo_contenido(df)
    st.markdown("---")
    C.fuentes_trafico(df)
    st.markdown("---")
    C.dispositivos(df)
    st.markdown("---")

    df_demo = Q.get_demograficos(id_cuenta, ids_sub, f0, f1, geo="WW")
    C.demografia(df_demo)
    st.markdown("---")

    df_paises = Q.get_paises(id_cuenta, ids_sub, f0, f1)
    C.geografia_paises(df_paises)
    st.markdown("---")

    df_terms = Q.get_search_terms(id_cuenta, ids_sub, f0, f1, geo="WW")
    C.search_terms(df_terms, "🔍 Términos de búsqueda")
    st.markdown("---")

    df_top = Q.get_top_videos(id_cuenta, ids_sub, f0, f1, geo="WW")
    C.top_videos(df_top, "🎬 Top Videos del período")
    st.markdown("---")

    C.progreso_anual(id_cuenta, ids_sub,
                     Q.get_proyecciones, Q.get_metricas_anuales)

else:
    # ──────────────────────────────────────────────────────
    # PANEL TELEMUNDO — tabs WW / US+PR
    # ──────────────────────────────────────────────────────
    tab_ww, tab_uspr = st.tabs(["🌍 Mundial (WW)", "🇺🇸 EE.UU. + Puerto Rico"])

    with tab_ww:
        C.kpis(df)
        st.markdown("---")
        C.evolucion(df, key_suffix="_ww")
        st.markdown("---")
        C.tipo_contenido(df, key_suffix="_ww")
        st.markdown("---")
        C.fuentes_trafico(df, key_suffix="_ww")
        st.markdown("---")
        C.dispositivos(df, key_suffix="_ww")
        st.markdown("---")

        df_demo_ww = Q.get_demograficos(id_cuenta, ids_sub, f0, f1, geo="WW")
        C.demografia(df_demo_ww, key_suffix="_ww")
        st.markdown("---")

        df_paises = Q.get_paises(id_cuenta, ids_sub, f0, f1)
        C.geografia_paises(df_paises)
        st.markdown("---")

        df_terms_ww = Q.get_search_terms(id_cuenta, ids_sub, f0, f1, geo="WW")
        C.search_terms(df_terms_ww, "🔍 Términos de búsqueda — WW")
        st.markdown("---")

        df_top_ww = Q.get_top_videos(id_cuenta, ids_sub, f0, f1, geo="WW")
        C.top_videos(df_top_ww, "🎬 Top Videos — WW")

    with tab_uspr:
        df_uspr = Q.get_uspr_periodo(id_cuenta, ids_sub, f0, f1)

        if df_uspr.empty or df_uspr.iloc[0].get("views_total") is None:
            st.info("Sin datos US+PR para el período seleccionado.")
        else:
            row_uspr = df_uspr.iloc[0].fillna(0)

            st.markdown("#### 🇺🇸 KPIs — EE.UU. + Puerto Rico")
            C.kpis_uspr(row_uspr)
            st.markdown("---")
            C.fuentes_trafico_uspr(row_uspr)
            st.markdown("---")
            C.dispositivos_uspr(row_uspr)
            st.markdown("---")

        df_demo_uspr = Q.get_demograficos(id_cuenta, ids_sub, f0, f1, geo="US_PR")
        C.demografia(df_demo_uspr, key_suffix="_uspr")
        st.markdown("---")

        df_estados = Q.get_estados(id_cuenta, ids_sub, f0, f1)
        C.geografia_estados(df_estados)
        st.markdown("---")

        df_terms_uspr = Q.get_search_terms(id_cuenta, ids_sub, f0, f1, geo="US_PR")
        C.search_terms(df_terms_uspr, "🔍 Términos de búsqueda — US+PR")
        st.markdown("---")

        df_top_uspr = Q.get_top_videos(id_cuenta, ids_sub, f0, f1, geo="US_PR")
        C.top_videos(df_top_uspr, "🎬 Top Videos — US+PR")

    st.markdown("---")
    C.progreso_anual(id_cuenta, ids_sub,
                     Q.get_proyecciones, Q.get_metricas_anuales)

# ============================================================
# 💬  CHATBOT — MÓDULO OPCIONAL
# ============================================================

import streamlit as st  # ya importado, solo para claridad del bloque

try:
    from config.settings import LLM_ENABLED
except ImportError:
    LLM_ENABLED = False

if LLM_ENABLED:
    st.markdown("---")
    st.subheader("💬 Asistente inteligente de métricas")

    # Inicializar chatbot una sola vez por sesión
    if "chatbot_listo" not in st.session_state:
        st.session_state["chatbot_listo"] = False
        st.session_state["agente"]        = None
        st.session_state["chat_history"]  = []

    if not st.session_state["chatbot_listo"]:
        with st.spinner("Cargando asistente..."):
            try:
                from llm.pdf_loader   import cargar_pdf_por_cuenta
                from llm.rag_pipeline import crear_vectorstore
                from llm.hybrid_agent import crear_agente_hibrido
                from db.connection    import engine as db_engine

                pdfs        = cargar_pdf_por_cuenta(id_cuenta)
                vectorstore = crear_vectorstore(id_cuenta, pdfs) if pdfs else None
                agente      = crear_agente_hibrido(id_cuenta, db_engine, vectorstore)

                st.session_state["agente"]        = agente
                st.session_state["chatbot_listo"] = True

                if pdfs:
                    st.success(f"✅ Asistente listo — {len(pdfs)} páginas de documentos cargadas.")
                else:
                    st.info("ℹ️ Asistente listo — sin documentos PDF para esta cuenta. "
                            "Responderá basándose en los datos de la BD.")

            except FileNotFoundError:
                st.info("📁 No hay documentos configurados para esta cuenta. "
                        "El asistente responderá solo con datos de la base de datos.")
                try:
                    from llm.hybrid_agent import crear_agente_hibrido
                    from db.connection    import engine as db_engine
                    agente = crear_agente_hibrido(id_cuenta, db_engine, None)
                    st.session_state["agente"]        = agente
                    st.session_state["chatbot_listo"] = True
                except Exception as e:
                    st.error(f"❌ No se pudo inicializar el asistente: {e}")

            except Exception as e:
                st.error(f"❌ Error al inicializar el asistente: {e}")

    # Mostrar historial de chat
    if st.session_state["chatbot_listo"] and st.session_state["agente"]:
        for msg in st.session_state["chat_history"]:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

        prompt = st.chat_input("Pregunta sobre tus métricas, contenido o estrategia...")
        if prompt:
            st.chat_message("user").markdown(prompt)
            st.session_state["chat_history"].append({"role": "user", "content": prompt})

            with st.chat_message("assistant"):
                with st.spinner("Analizando..."):
                    respuesta = st.session_state["agente"](prompt)
                st.markdown(respuesta)

            st.session_state["chat_history"].append({"role": "assistant", "content": respuesta})

        # Botón para limpiar historial
        if st.session_state["chat_history"]:
            if st.button("🗑️ Limpiar conversación"):
                st.session_state["chat_history"] = []
                st.rerun()