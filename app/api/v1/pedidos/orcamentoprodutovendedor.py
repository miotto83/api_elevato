from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = ["IDORCAMENTO", "IDVENDEDOR", "QTDPRODUTO", "VALTOTLIQUIDO", "VALUNITBRUTO", "VALFRETE"]


@router.get("/{idvendedor}&{idproduto}")
async def orcamentocabecalho_busca(idvendedor: str, idproduto: str):
    """
    Esta chamada retorna uma lista com dados de orcamento/itens de orcamento filtrando um item e um vendedor.

    Exemplo para filtrar:
    idvendedor = 1137478
    idproduto = 1059453
    """
    logging.info(f"Endpoint '/orcamentoprodutovendedor/{idvendedor}&{idproduto}' accessed successfully.")

    df = await execute_query(
        "consultas/orcamentoprodutovendedor.sql", {":IDVENDEDOR": idvendedor, ":IDPRODUTO": idproduto}
    )

    return await process_results(df, columns, return_with_data=True)
