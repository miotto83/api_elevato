from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = [
    "IDEMPRESA",
    "IDORCAMENTOORIGEM",
    "NOMEFANTASIA",
    "IDCLIFOR",
    "NOME",
    "DESCRCIDADE",
    "UF",
    "IDORCAMENTO",
    "VALOR_VENDA",
    "VALFRETE_VENDA",
]


@router.get("/{idvendedor}")
async def pedidocabecalho_busca(idvendedor: str):
    """
    Esta chamada retorna uma lista com dados de um cabeçalho de pedido efetivado filtrando pelo idvendedor.

    Exemplo para filtrar:
    idvendedor = 1137478
    """
    logging.info(f"Endpoint '/pedidocabecalho/{idvendedor}' accessed successfully.")

    df = await execute_query("ciss_db2", "consultas/pedidocabecalho.sql", {":IDVENDEDOR": idvendedor})

    return await process_results(df, columns, return_with_data=True)
