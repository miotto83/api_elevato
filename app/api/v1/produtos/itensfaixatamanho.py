from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = ["IDFAIXA", "IDFAIXADETALHE", "IDTAMANHO", "DESCRFAIXA", "IDPRODUTO"]


@router.get("/{idfaixatamanho}")
async def itens_faixa_tamanho(idfaixatamanho: str):
    """
    Esta chamada retorna uma lista de itens de uma determinada faixa de tamanho para utilizar na busca no catalogo de produtos.

    """
    logging.info(f"Endpoint '/itensfaixatamanho/{idfaixatamanho}' accessed successfully.")

    df = await execute_query("ciss_db2", "consultas/itensfaixatamanho.sql", {":IDFAIXATAMANHO": idfaixatamanho})

    return await process_results(df, columns)