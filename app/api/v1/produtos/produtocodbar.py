from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = ["IDCODBARPROD"]


@router.get("/{idproduto}")
async def produto_busca(idproduto: str):
    logging.info(f"Endpoint '/produtocodbar/{idproduto}' accessed successfully.")

    df = await execute_query("ciss_db2", "consultas/produtocodbar.sql", {":IDPRODUTO": idproduto})

    return await process_results(df, columns)
