from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = ["IDORCAMENTO", "IDCLIFOR", "NOME", "VALTOTLIQUIDO", "DTMOVIMENTO", "FLAGCANCELADO"]


@router.get("/{idvendedor}")
async def orcamentocabecalho_busca(idvendedor: str):
    """
    Esta chamada retorna uma lista com dados de um cabeçalho de orçamento filtrando pelo idvendedor.

    Exemplo para filtrar:
    idvendedor = 1137478
    """
    logging.info(f"Endpoint '/orcamentocabecalho/{idvendedor}' accessed successfully.")

    df = await execute_query("consultas/orcamentocabecalho.sql", {":IDVENDEDOR": idvendedor})

    return await process_results(df, columns, return_with_data=True)
