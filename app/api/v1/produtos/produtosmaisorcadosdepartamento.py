from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = ["IDPRODUTO", "QTDPRODUTO", "VALTOTLIQUIDO", "QTDORCAMENTO"]


@router.get("/{iddepartamento}")
async def orcamentocabecalho_busca(iddepartamento: str):
    """
    Esta chamada retorna uma lista com os 20 itens mais orçados em um departamento nos últimos 30 dias

    Exemplo para filtrar:
    iddepartamento = 4

    """
    logging.info(f"Endpoint '/produtosmaisorcadosdepartamento/{iddepartamento}' accessed successfully.")

    replace_dict = {":IDDEPARTAMENTO": iddepartamento}

    df = await execute_query("ciss_db2", "consultas/produtosmaisorcadosdepartamento.sql", replace_dict)

    return await process_results(df, columns, return_with_data=True)
