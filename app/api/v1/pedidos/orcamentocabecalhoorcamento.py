from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = [
    "IDORCAMENTO",
    "ENDERECOCOMPLETO",
    "IDCEP",
    "UF",
    "OBSERVACAO",
    "CNPJCPF",
    "BAIRRO",
    "NUMERO",
    "COMPLEMENTO",
    "FONECELULAR",
    "CLIENTE",
    "NOME",
    "IDVENDEDOR",
    "NOMEVENDEDOR",
    "EMAILVENDEDOR",
    "IDEMPRESA",
    "FONEEMPRESA",
    "ENDERECOEMPRESA",
    "EMAILEMPRESA",
    "NOMELOJA",
    "PERFILFRETE",
    "VALTOTLIQUIDO",
    "FLAGCANCELADO",
]


@router.get("/{idorcamento}")
async def orcamentocabecalho_busca(idorcamento: str):
    """
    Esta chamada retorna dados de cabeçalho de orçamento filtrando pelo idorcamento.

    Exemplo para filtrar:
    idorcamento = 3083769
    """
    logging.info(f"Endpoint '/orcamentocabecalhoorcamento/{idorcamento}' accessed successfully.")

    df = await execute_query("ciss_db2", "consultas/orcamentocabecalhoorcamento.sql", {":IDORCAMENTO": idorcamento})

    return await process_results(df, columns)
