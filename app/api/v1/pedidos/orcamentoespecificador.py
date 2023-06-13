from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = ["IDORCAMENTO", "IDCLIFOR", "NOME", "VALTOTLIQUIDO", "DTMOVIMENTO"]


@router.get("/{idespecificador}")
async def orcamentocabecalho_busca(idespecificador: str):
    """
    Esta chamada retorna uma lista com dados de um cabeçalho de orçamento filtrando pelo idespecificador.

    Exemplo para filtrar:
    idespecificador = 1188771
    """
    logging.info(f"Endpoint '/orcamentoespecificador/{idespecificador}' accessed successfully.")

    df = await execute_query("consultas/orcamentoespecificador.sql", {":IDESPECIFICADOR": idespecificador})

    return await process_results(df, columns, return_with_data=True)
