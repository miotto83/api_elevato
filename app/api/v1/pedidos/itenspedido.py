from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = [
    "IDEMPRESA",
    "DATA",
    "IDVENDEDOR",
    "IDPRODUTO",
    "IDSUBPRODUTO",
    "MARCA",
    "DESCRPRODUTO",
    "NUMSEQUENCIA",
    "QTDPRODUTO",
    "VALORVENDALIQ",
    "VALORFRETEVENDA",
]


@router.get("/{idpedido}")
async def itens_pedido(idpedido: str):
    """
    Esta chamada retorna uma lista de itens de um determinado pedido já efetivado no CISS.

    Exemplo para filtrar:
    idpedido = 3083928
    """
    logging.info(f"Endpoint '/itenspedido/{idpedido}' accessed successfully.")

    df = await execute_query("ciss_db2", "consultas/itenspedido.sql", {":IDPEDIDO": idpedido})

    return await process_results(df, columns, return_with_data=True)
