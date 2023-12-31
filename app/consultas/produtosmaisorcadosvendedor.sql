SELECT OP.IDPRODUTO,
    count(o.idorcamento) as qtdorcamento,
    CAST(SUM(OP.QTDPRODUTO) AS VARCHAR(20)) AS QTDPRODUTO,
    CAST(SUM(OP.VALTOTLIQUIDO) AS VARCHAR(20)) AS VALTOTLIQUIDO
FROM DBA.ORCAMENTO AS O
    LEFT JOIN DBA.ORCAMENTO_PROD AS OP ON O.IDORCAMENTO = OP.IDORCAMENTO
    AND O.IDEMPRESA = OP.IDEMPRESA
WHERE FLAGPRENOTAPAGA = 'F'
    AND FLAGPRENOTA = 'F'
    AND O.DTMOVIMENTO >= CURRENT DATE -30
    AND OP.IDORCAMENTOORIGEM IS NULL
    AND IDPRODUTO IS NOT NULL
    AND O.FLAGCANCELADO = 'F'
    AND OP.IDVENDEDOR in (':IDVENDEDOR')
GROUP BY OP.IDPRODUTO
ORDER BY SUM(OP.VALTOTLIQUIDO) DESC
LIMIT 20