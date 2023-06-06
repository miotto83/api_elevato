SELECT O.IDORCAMENTO,
    O.IDCLIFOR,
    O.DTMOVIMENTO,
    O.NOME AS NOME,
    CAST(SUM(OP.VALTOTLIQUIDO) AS VARCHAR(20)) AS VALTOTLIQUIDO
FROM DBA.ORCAMENTO AS O
    LEFT JOIN DBA.ORCAMENTO_PROD AS OP ON O.IDORCAMENTO = OP.IDORCAMENTO
    AND O.IDEMPRESA = OP.IDEMPRESA
    LEFT JOIN DBA.CLUBE_INDICADOR_ORCAMENTO AS CIO ON O.IDORCAMENTO = CIO.IDORCAMENTO
WHERE FLAGPRENOTA = 'F'
    AND O.DTMOVIMENTO >= CURRENT DATE -120
    AND CIO.IDINDICADOR = ':IDESPECIFICADOR'
    AND OP.IDORCAMENTOORIGEM IS NULL
GROUP BY O.IDORCAMENTO,
    O.IDCLIFOR,
    O.NOME,
    O.DTMOVIMENTO