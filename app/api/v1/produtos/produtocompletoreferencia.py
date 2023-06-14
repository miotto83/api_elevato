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


@router.get("/{idempresa}&{fabricante}")
async def produto_busca_estoque(idempresa: str, fabricante: str):
    """
    Esta chamada retorna uma lista com dados de cadastro de produto com informações de estoque nos CDs e preço de venda e promoção.
    O filtro de idempresa é para identificar o preço de venda e promoção.
    O filtro fabricante é para filtrar a referência do produto.
    Exemplo para filtrar:
    idempresa = 1
    fabricante = 1990 (referência da Deca - chuveiros)
    """
    fabricante = fabricante.replace(" ", "%")
    df = await execute_query("ciss_db2", 
        "consultas/produtocompletoreferencia.sql", {":IDEMPRESA": idempresa, ":FABRICANTE": fabricante}
    )
    return await process_results(df, colunas, return_with_data=True)
