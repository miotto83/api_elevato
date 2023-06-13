from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = ["IDPRODUTO", "QTDPRODUTO", "VALTOTLIQUIDO", "QTDORCAMENTO"]


@router.get("/")
async def orcamentocabecalho_busca():
    """
    Esta chamada retorna uma lista com os 20 itens mais orçados nos últimos 30 dias em todo o Grupo Elevato.

    Não é aplicado nenhum filtro.

    """
    logging.info(f"Endpoint '/produtosmaisorcados/' accessed successfully.")

    df = await execute_query("consultas/produtosmaisorcados.sql", {})

    return await process_results(df, columns, return_with_data=True)
