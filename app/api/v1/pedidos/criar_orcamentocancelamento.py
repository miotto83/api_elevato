from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging
from pydantic import BaseModel

router = APIRouter()

class OrcamentoPerdido(BaseModel):
    idorcamento: str



@router.post("/")
async def create_orcamento(orcamentocancelamento: OrcamentoPerdido):
    logging.info(
        f"Endpoint '/orcamentocancelamento/{orcamentocancelamento.idorcamento}' accessed successfully."
    )

    query = f"""SELECT
           STATUS 
        FROM
        TABLE(  BIELEVATO.CANCELA_ORCAMENTO_BUBBLE(
                                    '{orcamentocancelamento.idorcamento}'
                )
        ) AS TBL"""

    df = await execute_query(
        database="ciss_db2",
        query=query,
    )

    colunas = ["STATUS"]
    status = {key: value for key, value in df.iloc[-1,:].to_dict().items() if key in colunas}
    
    return status

