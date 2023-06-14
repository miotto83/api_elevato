from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = [
    "IDINDICADOR",
    "NOMEINDICADOR",
    "FONECELULAR",
    "QTDVENDAS",
    "IDVENDEDOR",
    "NOMEVENDEDOR",
    "ULTIMACOMPRA",
    "DIASSEMCOMPRAS",
    "STATUS",
    "VALOR_VENDA_INDICADOR",
    "VALFRETE_VENDA_INDICADOR",
    "VALOR_DEV_INDICADOR",
    "VALOR_CAN_INDICADOR",
    "VALORLIQUIDOVENDAINDICADOR",
]


@router.get("/{idvendedor}")
async def especificadorvendedor_busca(idvendedor: str):
    """
    Esta chamada retorna uma lista com dados de especificadores filtrando pelo vendedor (idvendedor).

    Exemplo para filtrar:
    idvendedor = 1137478
    """
    logging.info(f"Endpoint '/especificadorvendedor/{idvendedor}' accessed successfully.")

    df = await execute_query("ciss_db2", "consultas/especificadorvendedor.sql", {":IDVENDEDOR": idvendedor})

    return await process_results(df, columns, return_with_data=True)
