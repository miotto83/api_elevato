from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = [
    "IDSUBPRODUTO",
    "IDSUBPRODUTO_P2",
    "NUM_SALES",
    "VALOR_P1",
    "VALOR_P2",
    "MARGEM_PER",
    "MARGEM_VALOR",
    "DESCR_P1",
    "GRUPO_P1",
    "SUBGRUPO_P1",
    "DESCR_P2",
    "GRUPO_P2",
    "SUBGRUPO_P2",
]


@router.get("/{idsubproduto}")
async def produto_busca(idsubproduto: str):
    """
    Esta chamada retorna uma lista dos 20 produtos mais vendidos junto com o produto passado no filtro.
    Exemplo para filtrar:
    idsubproduto = 1059453
    """
    logging.info(f"Endpoint '/crosssel/{idsubproduto}' accessed successfully.")

    df = await execute_query("consultas/crosssell.sql", {":IDSUBPRODUTO": idsubproduto})

    return await process_results(df, columns, return_with_data=True)
