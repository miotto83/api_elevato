from fastapi import APIRouter
from services import query_service

router = APIRouter()


@router.get("/{idvendedor}&{meses}",
            summary="Vendas do vendedor nos últimos x meses",
            description="""Esta chamada retorna uma lista com as vendas do vendedor nos últimos x meses mês a mês.
            meses = 0 retorna o mês atual.
            Exemplo para filtrar: idvendedor = 1137478 e meses = 6"""
            )
async def funcao(idvendedor: str, meses: str):
    df_cliente = await query_service.execute_query(
        "dw_postgres", "consultas/venda_x_meses.sql", {":IDVENDEDOR": idvendedor, ":MESES": meses}
    )
    return await query_service.process_results(df_cliente, return_with_data=True)
