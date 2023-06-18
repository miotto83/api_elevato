from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

colunas = ["IDPERFIL","DESCRICAO","IDREGIAO","DESCRREGIAO","VALORFRETE"]


@router.get("/")
async def perfil_frete():
    """
    Esta chamada retorna uma lista de perfis de fretes  para utilizadar para o cálculo do valor de frete nas vendas.
    """
    
    logging.info(f"Endpoint '/perfilfrete/' accessed successfully.")

    df = await execute_query("ciss_db2", "consultas/perfilfrete.sql",{})

    return await process_results(df, colunas)