from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

colunas = ["IDSUBPRODUTO"]


@router.get("/{idempresa}&{idfabricante}")
async def produto_busca_fabricante(idempresa: str, idfabricante: str):
    """
    Esta chamada retorna apenas o IDSUBPRODUTO de um cadastro filtrando preço por uma empresa e pela descrição do produto.
    Para visualizar o retorno, basta colocar "PISO PORTINARI" no idfabricante e "1" no idempresa.
    Em cada execução, ele retorna uma lista de idsubprodutos.
    """
    df_produtocompletofabricante = await execute_query("ciss_db2", 
        "consultas/produtocompletofabricante.sql", {":IDEMPRESA": idempresa, ":IDFABRICANTE": idfabricante}
    )
    return await process_results(df_produtocompletofabricante, colunas, return_with_data=True)
