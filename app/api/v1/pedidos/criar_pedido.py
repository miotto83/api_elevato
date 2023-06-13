from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging
from pydantic import BaseModel

router = APIRouter()

class Pedido(BaseModel):
    idempresa: str
    idclifor: str


@router.post("/")
async def create_pedido(pedido: Pedido):
    logging.info(
        f"Endpoint '/pedido/{pedido.idempresa}&{pedido.idclifor}' accessed successfully."
    )

    query = f"""SELECT
           cast(idpedido as varchar(20)) as idpedido
        FROM
        TABLE(  BIELEVATO.SET_PEDIDO_BUBBLE(
                                    '{pedido.idempresa}',
                                    '{pedido.idclifor}'
                )
        ) AS TBL"""

    df = await execute_query(
        database="ciss_db2",
        query=query,
    )

    colunas = ["IDPEDIDO"]
    idpedidociss = {key: value for key, value in df.iloc[-1,:].to_dict().items() if key in colunas}
    
    return idpedidociss

