"""
pipeline/run.py
===============
Punto de entrada para ejecutar el pipeline desde consola.

Ejemplos:
  python -m pipeline.run
  python -m pipeline.run --start 2025-01-01 --end 2025-06-30
  python -m pipeline.run --subcuenta "Telemundo Deportes"
  python -m pipeline.run --grupo telemundo
  python -m pipeline.run --grupo sony --start 2025-01-01 --end 2025-03-31
  python -m pipeline.run --grupo Tokens_canales_restantes
"""

import argparse
import logging
from datetime import date

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.settings import DEFAULT_START, DEFAULT_END, TOKENS_DIR, TOKENS_GRUPOS
from pipeline.extractor import run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline.log", encoding="utf-8"),
    ],
)

def resolver_tokens_dir(grupo: str | None) -> str:
    """
    Dado --grupo, devuelve la ruta absoluta de la carpeta de tokens a usar.
 
    Prioridad:
      1. Sin --grupo → usa TOKENS_DIR directamente (busca en raíz y subcarpetas)
      2. --grupo es un alias definido en TOKENS_GRUPOS → usa esa subcarpeta
      3. --grupo es el nombre exacto de una subcarpeta → la usa directamente
      4. No encontrado → error con lista de opciones válidas
    """
    if grupo is None:
        return TOKENS_DIR
 
    # Intentar alias (ej: "sony" → "Tokens_solo_Sony")
    carpeta = TOKENS_GRUPOS.get(grupo.lower())
 
    # Si no hay alias, intentar como nombre de carpeta directo
    if carpeta is None:
        carpeta = grupo
 
    ruta = os.path.join(TOKENS_DIR, carpeta)
    if not os.path.isdir(ruta):
        opciones = "\n  ".join(
            f"--grupo {alias:15} → {TOKENS_DIR}/{nombre}"
            for alias, nombre in TOKENS_GRUPOS.items()
        )
        raise SystemExit(
            f"❌ Carpeta de tokens no encontrada: '{ruta}'\n\n"
            f"Grupos disponibles:\n  {opciones}\n\n"
            f"O pasa el nombre exacto de una subcarpeta dentro de '{TOKENS_DIR}'."
        )
    return ruta
 
 
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline YouTube Analytics → PostgreSQL")
    parser.add_argument("--start",     default=DEFAULT_START, help="Fecha inicio YYYY-MM-DD")
    parser.add_argument("--end",       default=DEFAULT_END,   help="Fecha fin   YYYY-MM-DD")
    parser.add_argument("--subcuenta", default=None,          help="Procesar solo esta subcuenta")
    parser.add_argument("--grupo",     default=None,
                        help="Subcarpeta de tokens a usar. "
                             "Ej: --grupo sony | --grupo telemundo | --grupo restantes. "
                             "Sin este argumento se procesan TODOS los tokens de todas las subcarpetas.")
    args = parser.parse_args()
 
    tokens_dir = resolver_tokens_dir(args.grupo)
 
    run(
        start=args.start,
        end=args.end,
        only_subcuenta=args.subcuenta,
        tokens_dir=tokens_dir,
    )