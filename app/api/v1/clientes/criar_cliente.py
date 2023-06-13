from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging
from pydantic import BaseModel

router = APIRouter()

class Cliente(BaseModel):
    cpfcnpj: str
    nome: str
    rua: str
    numero: int
    complemento: str
    bairro: str
    cep: str
    # idcliente:str
    fonecelular: str
    email: str
    idusuariocadastro: str
    obsgeral: str
    inscestadual: str
    dtnascimento: str
    nomefantasia: str


@router.post("/")
async def create_cliente(cliente: Cliente):
    logging.info(
        f"Endpoint '/clientes/{cliente.cpfcnpj}&{cliente.nome}&{cliente.cep}&{cliente.rua}&{cliente.bairro}&{cliente.complemento}&{cliente.email}&{cliente.fonecelular}&{cliente.dtnascimento}&{cliente.inscestadual}&{cliente.nomefantasia}&{cliente.numero}' accessed successfully."
    )

    query = f"""SELECT
           cast(idcliente as varchar(20)) as idcliente
        FROM
        TABLE(  DBA.UF_ADD_CLIENTE_BUBBLE(
                                    '{cliente.cpfcnpj}',
                                    '{cliente.nome}',
                                    '{cliente.rua}',
                                    '{cliente.numero}',
                                    '{cliente.complemento}',
                                    '{cliente.bairro}',
                                    '{cliente.cep}',
                                    '{cliente.fonecelular}',
                                    '{cliente.email}',
                                    '{cliente.idusuariocadastro}',
                                    '{cliente.obsgeral}',
                                    '{cliente.inscestadual}',
                                    '{cliente.dtnascimento}',
                                    '{cliente.nomefantasia}'

                )
        ) AS TBL"""

    df = await execute_query(
        database="ciss_db2",
        query=query,
    )

    colunas = ["IDCLIENTE"]
    idclienteciss = {key: value for key, value in df.iloc[-1,:].to_dict().items() if key in colunas}
    
    return idclienteciss