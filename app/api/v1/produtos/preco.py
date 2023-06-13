from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()


@router.get("/{idproduto}&{idempresa}")
async def preco_busca(idproduto: str, idempresa: str):
    """
    Esta chamada retorna o preço de venda varejo e o preço de venda promocional de um determinado produto em uma empresa.
    Utilize o código 1059453 e a empresa 1 para simular o retorno.
    Em cada execução ele retorna um único registro. Os campos que retornam serão apresentados na simulação.
    """
    df_preco = await execute_query(
        "ciss_db2", "consultas/preco.sql", {":IDPRODUTO": idproduto, ":IDEMPRESA": idempresa}
    )

    colunas = ["VALPRECOVAREJO", "VALPROMVAREJO"]

    return await process_results(df_preco, colunas)
