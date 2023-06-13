from fastapi import APIRouter
from services import query_service

router = APIRouter()


@router.get("/{idvendedor}",
            summary="Vendas do vendedor nos últimos 15 meses",
            description="Esta chamada retorna uma lista com as vendas do vendedor nos últimos 15 meses mês a mês. Exemplo para filtrar: idvendedor = 1137478"
            )
async def funcao(idvendedor: str):
    df_cliente = await query_service.execute_query(
        "dw_postgres", "consultas/kpis/vendedores/venda_15_meses.sql", {":IDVENDEDOR": idvendedor}
    )
    return await query_service.process_results(df_cliente, return_with_data=True)
