from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

colunas = ["IDRECEBIMENTO","DESCRRECEBIMENTO"]


@router.get("/")
async def forma_pagamento():
    """
     Esta chamada retorna uma lista de formas de pagamento para ser utilizada nas vendas.
    """
    
    logging.info(f"Endpoint '/formapagamento/' accessed successfully.")

    df = await execute_query("ciss_db2", "consultas/formapagamento.sql",{})

    return await process_results(df, colunas)