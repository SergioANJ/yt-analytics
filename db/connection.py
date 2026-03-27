"""
db/connection.py
================
Módulo de conexión a PostgreSQL.
La carga del .env ocurre en config/settings.py antes de llegar aquí.
"""

from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT


def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, poolclass=QueuePool, pool_size=5, max_overflow=10)


engine = get_engine()