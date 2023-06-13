from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = ["IDCLIFOR", "NOME", "IDCLUBE", "DESCRCLUBE"]


@router.get("/{cnpjcpf}")
async def especificadores_busca(cnpjcpf: str):
    """
    Esta chamada retorna dados de um especificador filtrando pelo CPF ou pelo número do telefone.

    Exemplo para filtrar:
    cnpjcpf = 56581580015 ou 51997038022
    """
    logging.info(f"Endpoint '/especificadores/{cnpjcpf}' accessed successfully.")

    df = await execute_query("consultas/especificadores.sql", {":CNPJCPF": cnpjcpf})

    return await process_results(df, columns)
