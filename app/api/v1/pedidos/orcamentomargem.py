from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = ['IDDOCUMENTO', 'NUMSEQUENCIA', 'PERCMARGEMCONTRIBUICAO', 'PERCMARGEMCONTRIBUICAOMEDIA', 'VALCUSTOREPOS']
 

@router.get("/{idorcamento}")
async def orcamentomargem(idorcamento: str):
    """"
    Esta chamada retorna uma lista dos itens ( por número de sequencia) com as informações de margem e custo de cada item de um determinado
    pedido/orçamento.

    Para Testar pode ser utilizado um código de orçamento: Ex.:
    3101530
    """
    logging.info(f"Endpoint '/orcamentomargem/{idorcamento}' accessed successfully.")

    df = await execute_query("ciss_db2", "consultas/orcamentomargem.sql", {":IDORCAMENTO": idorcamento})

    return await process_results(df, columns, return_with_data=True)
