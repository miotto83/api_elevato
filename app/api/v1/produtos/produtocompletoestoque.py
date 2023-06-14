from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

colunas = ["IDSUBPRODUTO"]


@router.get("/{idempresa}&{qtdmaior}")
async def produto_busca_estoque(idempresa: str, qtdmaior: str):
    """
    Esta chamada retorna uma lista com dados de cadastro de produto com informações de estoque nos CDs e preço de venda e promoção.
    O filtro de idempresa é para identificar o preço de venda e promoção.
    O filtro de quantidade maior é para retornar somente produtos com quantidade maior que a filtrada nos CDs (a quantidade disponível somada dos 2 CDs).
    Exemplo para filtrar:
    idempresa = 1
    qtdmaior = 100
    """
    df = await execute_query("ciss_db2", 
        "consultas/produtocompletoestoque.sql", {":IDEMPRESA": idempresa, ":QTDMAIOR": qtdmaior}
    )
    return await process_results(df, colunas, return_with_data=True)
