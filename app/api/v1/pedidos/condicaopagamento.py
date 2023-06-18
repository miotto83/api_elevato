from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

colunas = ["IDCONDICAO","DIASINTERVALO", "QTDPAGAMENTOS", "IDXMARGEMCONTRIB", "DESCRICAOCONDICAO"]


@router.get("/{idformapagamento}")
async def condicao_pagamento(idformapagamento:str):
    """
     Esta chamada retorna uma lista das condições de pagamento de uma forma de pagamento para 
     ser utilizada no orçamento/pedido.
    """
    
    logging.info(f"Endpoint '/condicaopagamento/{idformapagamento}' accessed successfully.")

    df = await execute_query("ciss_db2", "consultas/condicaopagamento.sql",{":IDFORMAPAGAMENTO" : idformapagamento})

    return await process_results(df, colunas)