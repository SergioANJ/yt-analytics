"""
db/connection.py
================
Módulo de conexión a PostgreSQL.
Provee un engine SQLAlchemy reutilizable.

Carga automáticamente .env.local (desarrollo) o .env.production
según la variable de entorno ENV. En Railway no se usa ningún .env
porque las variables se inyectan automáticamente.
"""

from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
import sys, os

# ── Cargar variables de entorno según el entorno activo ────
# EN RAILWAY: ENV no existe → no carga ningún .env (Railway inyecta las vars)
# EN LOCAL:   ENV=local (default) → carga .env.local
# EN LOCAL apuntando a prod: ENV=production → carga .env.production
_ENV = os.getenv("ENV", "local")
_env_file = f".env.{_ENV}"

if os.path.exists(_env_file):
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_file, override=True)
    except ImportError:
        pass  # python-dotenv no instalado — las vars deben venir del sistema

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT


def get_engine():
    """Retorna un engine SQLAlchemy con pool de conexiones."""
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, poolclass=QueuePool, pool_size=5, max_overflow=10)


# Engine singleton para reutilizar en todo el proyecto
engine = get_engine()