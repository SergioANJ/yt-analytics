"""
config/settings.py
==================
Configuración central. En producción (Railway) todas las credenciales
se leen desde variables de entorno — NUNCA van hardcodeadas aquí.
"""
 
import os
from datetime import date
 
# ── Base de datos ──────────────────────────────────────────
# Railway inyecta DATABASE_URL automáticamente al conectar un plugin PostgreSQL
# Formato: postgresql://usuario:password@host:port/nombre_db
DB_NAME     = os.getenv("PGDATABASE", "yt_analytics")
DB_USER     = os.getenv("PGUSER",     "soporte")
DB_PASSWORD = os.getenv("PGPASSWORD", "soporte")
DB_HOST     = os.getenv("PGHOST",     "localhost")
DB_PORT     = os.getenv("PGPORT",     "5433")
 
# ── Tokens y credenciales OAuth ────────────────────────────
TOKENS_DIR         = os.getenv("TOKENS_DIR", "tokens")
CLIENT_SECRET_FILE = os.getenv("CLIENT_SECRET_FILE", "client_secret_personal.json")
 
TOKENS_GRUPOS = {
    "sony":      "Tokens_solo_Sony",
    "telemundo": "Tokens_solo_Telemundo",
    "restantes": "Tokens_canales_restantes",
}
 
# ── Extracción ─────────────────────────────────────────────
DEFAULT_START = os.getenv("DEFAULT_START", "2020-01-01")
DEFAULT_END   = date.today().strftime("%Y-%m-%d")
 
# ── Cuentas con métricas WW + US+PR ────────────────────────
CUENTAS_CON_USPR = {"TELEMUNDO"}
 
# ── Scopes de la API de YouTube ────────────────────────────
YT_SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
]
 
# ── Mapeo fuentes de tráfico ────────────────────────────────
TRAFFIC_SOURCE_MAP = {
    "YT_SEARCH":     "Búsqueda YouTube",
    "RELATED_VIDEO": "Videos sugeridos",
    "EXT_URL":       "Externos",
    "SUBSCRIBER":    "Browse / Suscriptores",
    "PLAYLIST":      "Playlists",
    "SHORTS":        "Shorts feed",
    "NO_LINK_OTHER": "Directo/Desconocido",
}
 
# ── Chatbot / LLM ──────────────────────────────────────────
LLM_ENABLED     = os.getenv("LLM_ENABLED", "true").lower() == "true"
LLM_MODEL       = os.getenv("LLM_MODEL", "gpt-4o-mini")
LLM_PDFS_DIR    = os.getenv("LLM_PDFS_DIR", "pdfs")
LLM_MAX_TOKENS  = int(os.getenv("LLM_MAX_TOKENS", "800"))
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.2"))
 
# ── Mapeo dispositivos ──────────────────────────────────────
DEVICE_MAP = {
    "MOBILE":  "mobile",
    "TV":      "tv",
    "DESKTOP": "computer",
    "TABLET":  "tablet",
}