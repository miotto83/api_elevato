from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

colunas = [
    "IDSUBPRODUTO",
    "VALMULTIVENDAS",
    "PERCOMAVISTA",
    "NCM",
    "PESOBRUTO",
    "EMBALAGEMSAIDA",
    "LARGURA",
    "ALTURA",
    "COMPRIMENTO",
    "DESCRCOMPRODUTO",
    "FABRICANTE",
    "VALPRECOVAREJO",
    "VALPROMVAREJO",
    "ESTOQUESEVERO",
    "ESTOQUEGRAVATAI",
    "IDCODBARPROD",
    "REFERENCIA",
    "DTFIMPROMOCAOVAR",
    "MODELO",
]


@router.get("/{idempresa}&{idprodutos}")
async def produto_busca(idempresa: str, idprodutos: str):
    """
    Esta chamada retorna dados completos do cadastro de produto com estoque dos CDs e preço de venda na empresa passada como filtro.
    Ela retorna a informação de uma lista de produtos, que pode ser passada da seguinte forma:
    1059453,1081882,1056157
    Neste exemplo irá retornar dados dos 3 códigos listados.
    Para o exemplo utilize a empresa 1.
    Em cada execução ele retorna uma lista de registros (conforme a quantidade de códigos de produtos enviado).
    Os campos que retornam serão apresentados na simulação.
    """
    df = await execute_query(
        "consultas/produtocompleto.sql", {":IDEMPRESA": idempresa, ":IDPRODUTOS": idprodutos}
    )
    return await process_results(df, colunas)
