from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = ["VALORVENDADIA", "VALORMARGEMDIA", "VALORVENDAMES", "VALORMARGEMMES"]


@router.get("/{idvendedor}")
async def vendas_resumo(idvendedor: str):
    """
    Esta chamada retorna um resumo do valor de vendas e margem de um determinado vendedor no mês corrente.
    Exemplo para filtrar:
    idvendedor = 1137478
    """
    logging.info(f"Endpoint '/vendasresumo/{idvendedor}' accessed successfully.")

    df = await execute_query("ciss_db2", "consultas/vendasresumo.sql", {":IDVENDEDOR": idvendedor})

    return await process_results(df, columns)
