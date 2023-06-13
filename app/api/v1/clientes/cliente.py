from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

colunas = [
    "IDCLIFOR",
    "NOME",
    "NOMEFANTASIA",
    "CNPJCPF",
    "ENDERECO",
    "IDCEP",
    "NUMERO",
    "BAIRRO",
    "DESCRCIDADE",
    "DTNASCIMENTO",
    "COMPLEMENTO",
    "UF",
    "FONE2",
    "FONE1",
    "FONEFAX",
    "FONECELULAR",
    "EMAIL",
    "CODIGOIBGE",
    "GENERO",
    "IDVENDEDOR",
    "NOMEVENDEDOR",
]


@router.get("/{cpfcnpj}")
async def cliente_busca(cpfcnpj: str):
    """
    Esta chamada retorna dados de cadastro de cliente. Use o CPF 00881535923 para simular os dados que retornam.
    É passado um paramêtro chamado cpfcnpj que é utilizado para filtrar o cpfcnpj e número de telefone.
    Em cada execução ele retorna um único registro de cliente. Os campos que retornam serão apresentados na simulação.
    """
    df = await execute_query("ciss_db2", "consultas/cliente.sql", {":CNPJCPF": cpfcnpj})
    return await process_results(df, colunas)
