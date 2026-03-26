"""
llm/rag_pipeline.py
===================
Construye y cachea el vectorstore FAISS a partir de los documentos PDF
de una cuenta.

El vectorstore se guarda en memoria durante la sesión de Streamlit
usando st.cache_resource, por lo que no se reconstruye en cada rerun.
"""

import os
import logging
from typing import List, Optional

from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

log = logging.getLogger(__name__)


def crear_vectorstore(
    id_cuenta: int,
    documentos: List[Document],
    chunk_size: int = 1000,
    chunk_overlap: int = 150,
) -> Optional[FAISS]:
    """
    Divide los documentos en chunks y construye un índice FAISS.

    Args:
        id_cuenta:      ID de la cuenta (usado solo para logging).
        documentos:     Lista de Document cargados por pdf_loader.
        chunk_size:     Tamaño de cada chunk en caracteres.
        chunk_overlap:  Solapamiento entre chunks consecutivos.

    Returns:
        Vectorstore FAISS listo para búsqueda, o None si no hay documentos.
    """
    if not documentos:
        log.info(f"[cuenta {id_cuenta}] Sin documentos — vectorstore no creado.")
        return None

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        separators=["\n\n", "\n", ".", " ", ""],
    )
    chunks = splitter.split_documents(documentos)
    log.info(f"[cuenta {id_cuenta}] {len(documentos)} páginas → {len(chunks)} chunks")

    try:
        embeddings   = OpenAIEmbeddings()   # usa OPENAI_API_KEY del entorno
        vectorstore  = FAISS.from_documents(chunks, embeddings)
        log.info(f"[cuenta {id_cuenta}] Vectorstore FAISS creado.")
        return vectorstore
    except Exception as e:
        log.error(f"[cuenta {id_cuenta}] Error creando vectorstore: {e}")
        return None


def buscar_contexto(vectorstore: FAISS, pregunta: str, k: int = 4) -> str:
    """
    Recupera los k chunks más relevantes y los devuelve como string.

    Args:
        vectorstore: Índice FAISS ya construido.
        pregunta:    Pregunta del usuario.
        k:           Número de chunks a recuperar.

    Returns:
        Texto concatenado de los chunks más relevantes.
    """
    if vectorstore is None:
        return ""
    try:
        docs = vectorstore.similarity_search(pregunta, k=k)
        return "\n\n---\n\n".join(d.page_content for d in docs)
    except Exception as e:
        log.warning(f"Error en búsqueda semántica: {e}")
        return ""
