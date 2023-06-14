from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = [
    "IDORCAMENTO",
    "IDPRODUTO",
    "IDSUBPRODUTO",
    "DESCRAMBIENTE",
    "NUMSEQUENCIA",
    "IDVENDEDOR",
    "VALMULTIVENDAS",
    "EMBALAGEMSAIDA",
    "DESCRCOMPRODUTO",
    "VALDESCONTOPRO",
    "PERDESCONTOPRO",
    "VALLUCRO",
    "PERMARGEMLUCRO",
    "TIPOENTREGA",
    "NUMSEQUENCIA",
    "IDLOCALRETIRADA",
    "DESCRLOCALRETIRADA",
    "IDLOCALESTOQUE",
    "IDLOTE",
    "QTDPRODUTO",
    "VALUNITBRUTO",
    "VALFRETE",
    "VALTOTLIQUIDO",
]


@router.get("/{idorcamento}")
async def orcamentocabecalho_busca(idorcamento: str):
    """
    Esta chamada retorna uma lista com dados de itens de um determinado orçamento. O filtro é pelo idorcamento.

    Exemplo para filtrar:
    idorcamento = 3081473 (este orçamento pode ter sido cancelado ou efetivado. Para testar, sugiro consultar um novo idorcamento no CISS.)
    """
    logging.info(f"Endpoint '/orcamentoitens/{idorcamento}' accessed successfully.")

    df = await execute_query("ciss_db2", "consultas/orcamentoitens.sql", {":IDORCAMENTO": idorcamento})

    return await process_results(df, columns, return_with_data=True)
