"""
llm/pdf_loader.py
=================
Carga PDFs asociados a una cuenta.

Estructura de carpetas esperada:
  pdfs/
    {id_cuenta}/
      reporte_q1.pdf
      estrategia_2025.pdf
      ...

Si la carpeta no existe o está vacía, retorna lista vacía
y el chatbot simplemente no tendrá contexto de documentos.
"""

import os
from typing import List

from langchain_community.document_loaders import PyPDFLoader
from langchain_core.documents import Document

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import LLM_PDFS_DIR


def cargar_pdf_por_cuenta(id_cuenta: int) -> List[Document]:
    """
    Carga todos los PDFs de la carpeta pdfs/{id_cuenta}/.

    Returns:
        Lista de Document (LangChain). Vacía si no hay PDFs.

    Raises:
        FileNotFoundError: si la carpeta raíz de PDFs no existe en absoluto.
    """
    pdfs_root = os.path.abspath(LLM_PDFS_DIR)

    if not os.path.isdir(pdfs_root):
        raise FileNotFoundError(
            f"Carpeta de PDFs no encontrada: '{pdfs_root}'. "
            "Crea la carpeta o establece LLM_PDFS_DIR en config/settings.py."
        )

    cuenta_dir = os.path.join(pdfs_root, str(id_cuenta))
    if not os.path.isdir(cuenta_dir):
        return []   # Esta cuenta no tiene PDFs todavía — no es un error

    pdf_files = [f for f in os.listdir(cuenta_dir) if f.lower().endswith(".pdf")]
    if not pdf_files:
        return []

    documentos: List[Document] = []
    for fname in sorted(pdf_files):
        path = os.path.join(cuenta_dir, fname)
        try:
            loader = PyPDFLoader(path)
            pages  = loader.load()
            # Añadir metadato de origen para el agente
            for page in pages:
                page.metadata["source_file"] = fname
                page.metadata["id_cuenta"]   = id_cuenta
            documentos.extend(pages)
        except Exception as e:
            # Un PDF corrupto no debe romper toda la carga
            import logging
            logging.getLogger(__name__).warning(f"No se pudo cargar '{fname}': {e}")

    return documentos
