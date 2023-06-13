from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

colunas = [
    "IDSUBPRODUTO",
    "DESCRCOMPRODUTO",
    "PERCOMAVISTA",
    "VALPRECOVAREJO",
    "VALPROMVAREJO",
    "DESCRGRUPO",
    "DESCRSUBGRUPO",
    "DESCRSECAO",
    "ESTOQUEGRAVATAI",
    "ESTOQUESEVERO",
    "EMBALAGEMSAIDA",
    "DATAFINALPROMOCAO",
]


@router.get("/{idempresa}&{fabricante}&{qtdmaior}")
async def produto_busca_estoque(idempresa: str, fabricante: str, qtdmaior: float):
    """
    Esta chamada retorna uma lista com dados de cadastro de produto com informações de estoque nos CDs e preço de venda e promoção.
    O filtro de idempresa é para identificar o preço de venda e promoção.
    O filtro de quantidade maior é para retornar somente produtos com quantidade maior que a filtrada nos CDs (a quantidade disponível somada dos 2 CDs).
    O filtro fabricante é para filtrar a descrição do produto.
    Exemplo para filtrar:
    idempresa = 1
    fabricante = PISO PORTINARI
    qtdmaior = 100 (se for passada a quantidade -9999 ele retorna qualquer quantidade)
    """
    fabricante = fabricante.replace(" ", "%")
    df = await execute_query(
        "consultas/produtocompletoestoquedescricao.sql",
        {":IDEMPRESA": idempresa, ":FABRICANTE": fabricante, ":QTDMAIOR": str(qtdmaior)},
    )
    return await process_results(df, colunas, return_with_data=True)
