from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging
from pydantic import BaseModel

router = APIRouter()

class ItemPedido(BaseModel):
    idpedido: str
    idempresa: str
    idvendedor: str
    idproduto: str
    idsubproduto: str
    numsequencia: str
    idlote: str
    qtdproduto: float
    valunitbruto: float
    valtotliquido: float
    valdescontopro: float
    ambiente: str
    idlocalestoque: str


@router.post("/")
async def create_item_pedido(itempedido: ItemPedido):
    logging.info(
        f"Endpoint '/pedido/{itempedido.idempresa}&{itempedido.ambiente}&{itempedido.idpedido}&{itempedido.idvendedor}&{itempedido.idproduto}&{itempedido.idsubproduto}&{itempedido.qtdproduto}&{itempedido.valtotliquido}&{itempedido.idproduto}&{itempedido.numsequencia}&{itempedido.valdescontopro}&{itempedido.valunitbruto}' accessed successfully."
    )

    query = f"""SELECT
           cast(statusitenspedido as varchar(10)) as statusitenspedido
        FROM
        TABLE(  BIELEVATO.SET_ITEM_PEDIDO_BUBBLE_3(
                                    '{itempedido.idpedido}',
                                    '{itempedido.idempresa}',
                                    '{itempedido.idvendedor}',
                                    '{itempedido.idproduto}',
                                    '{itempedido.idsubproduto}',
                                    '{itempedido.numsequencia}',
                                    '{itempedido.qtdproduto}',
                                    '{itempedido.valunitbruto}',
                                    '{itempedido.valtotliquido}',
                                    '{itempedido.valdescontopro}',
                                    '{itempedido.ambiente}',
                                    '{itempedido.idlote}',
                                    '{itempedido.idlocalestoque}'
                )
        ) AS TBL"""

    df = await execute_query(
        database="ciss_db2",
        query=query,
    )

    colunas = ["STATUSITENSPEDIDO"]
    idstatusciss = {key: value for key, value in df.iloc[-1,:].to_dict().items() if key in colunas}
    
    return idstatusciss

