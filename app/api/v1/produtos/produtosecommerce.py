from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

colunas = [
    "IDSUBPRODUTO",
    "DESCRCOMPRODUTO",
    "MULTIVENDAS",
    "VALPROMVAREJO",
    "VALPRECOVAREJO",
    "DTFIMPROMOCAOVAR",
    "EMBALAGEMSAIDA",
    "FABRICANTE",
    "REFERENCIA"
]


@router.get("/")
async def produtos_ecommerce():
    """
    Esta chamada retorna uma lista de dados de produtos para integração do ecommerce.
    """
    
    logging.info(f"Endpoint '/produtosecommerce/' accessed successfully.")

    df = await execute_query("ciss_db2", "consultas/produtosecommerce.sql",{})

    return await process_results(df, colunas)
