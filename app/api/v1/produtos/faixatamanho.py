from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = ["IDFAIXA","DESCRFAIXA"]


@router.get("/{idfaixatamanho}")
async def faixa_tamanho(idfaixatamanho: str):
    """
    Esta chamada retorna uma lista de faixas de tamanho de produtos para utilizar na busca no catalogo de produtos.

    """
    logging.info(f"Endpoint '/faixatamanho/{idfaixatamanho}' accessed successfully.")

    df = await execute_query("ciss_db2", "consultas/faixatamanho.sql", {":IDFAIXATAMANHO": idfaixatamanho})

    return await process_results(df, columns)