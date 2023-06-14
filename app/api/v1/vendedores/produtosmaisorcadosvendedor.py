from fastapi import APIRouter
from services.query_service import execute_query, process_results
import logging

router = APIRouter()

columns = ["IDPRODUTO", "QTDPRODUTO", "VALTOTLIQUIDO", "QTDORCAMENTO"]


@router.get("/{idvendedor}",
            summary="Produtos mais orçados de um vendedor",
            description="Esta chamada retorna uma lista com os 20 itens mais orçados de um vendedor nos últimos 30 dias. Exemplo para filtrar: idvendedor = 1137478"
            )
async def orcamentocabecalho_busca(idvendedor: str):
    
    logging.info(f"Endpoint '/produtosmaisorcadosvendedor/{idvendedor}' accessed successfully.")

    replace_dict = {":IDVENDEDOR": idvendedor}

    df = await execute_query("ciss_db2", "consultas/produtosmaisorcadosvendedor.sql", replace_dict)

    return await process_results(df, columns, return_with_data=True)
