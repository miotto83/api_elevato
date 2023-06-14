from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = [
    "IDPRODUTO",
    "IDSUBPRODUTO",
    "DESCRLOTE",
    "IDLOCALESTOQUE",
    "DESCRLOCALESTOQUE",
    "QTDSALDODISPONIVEL",
]


@router.get("/{idsubproduto}")
async def orcamentocabecalho_busca(idsubproduto: str):
    """
    Esta chamada retorna uma lista com os dados de Lote de um determinado produto e sua quantidade disponível.

    Exemplo para filtrar:
    idsubproduto = 1059453

    """
    logging.info(f"Endpoint '/buscaestoquelote/{idsubproduto}' accessed successfully.")

    replace_dict = {":IDSUBPRODUTO": idsubproduto}

    df = await execute_query("ciss_db2", "consultas/buscaestoquelote.sql", replace_dict)

    return await process_results(df, columns, return_with_data=True)
