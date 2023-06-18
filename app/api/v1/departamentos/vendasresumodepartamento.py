from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = ["VALORVENDADIA","VALORMARGEMDIA","VALORVENDAMES","VALORMARGEMMES", "VALORFRETEMES"]


@router.get("/{iddepartamento}")
async def vendas_resumo_departamento(iddepartamento: str):
    """
    Esta chamada retorna um resumo do valor de vendas e margem de um determinado departamento no mês corrente.

    Exemplo para filtrar:
    idempresa = 2

    """
    logging.info(f"Endpoint '/vendasresumodepartamento/{iddepartamento}' accessed successfully.")

    df = await execute_query("ciss_db2", "consultas/vendasresumodepartamento.sql", {":IDDEPARTAMENTO": iddepartamento})

    return await process_results(df, columns)
