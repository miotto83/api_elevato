from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = ["QTDDISPONIVEL"]


@router.get("/{idproduto}")
async def estoque_busca(idproduto: str):
    """
    Esta chamada retorna a quantidade disponível de estoque nos CDs de um determinado produto.

    Exemplo para filtrar:
    idproduto = 1059453
    """
    logging.info(f"Endpoint '/estoque/{idproduto}' accessed successfully.")

    df = await execute_query("consultas/estoque.sql", {":IDPRODUTO": idproduto})

    return await process_results(df, columns)
