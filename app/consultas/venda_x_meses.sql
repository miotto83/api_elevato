SELECT TO_CHAR(DATE_TRUNC('month', "DATA"), 'YYYY-MM') AS MES,
    SUM("VALORLIQUIDOVENDA" + "VALFRETE_VENDA") as VENDA,
    SUM("PERCMARGEMCONTRIBUICAO" / 100 * ("VALORLIQUIDOVENDA" + "VALFRETE_VENDA")) as MARGEM,
    SUM("PERCMARGEMCONTRIBUICAO" / 100 * ("VALORLIQUIDOVENDA" + "VALFRETE_VENDA")) / SUM("VALORLIQUIDOVENDA" + "VALFRETE_VENDA") as MARGEM_PERCENTUAL
FROM venda_consolidada
WHERE "IDVENDEDOR" = :IDVENDEDOR
    AND "DATA" >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL ':MESES months')
GROUP BY MES
ORDER BY MES