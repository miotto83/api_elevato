SELECT
        RESUMO.IDSUBPRODUTO,
        RESUMO.DESCRCOMPRODUTO,
        CAST(RESUMO.MULTIVENDAS AS VARCHAR(20)) AS MULTIVENDAS ,
        CAST(RESUMO.VALPROMVAREJO AS VARCHAR(20)) AS VALPROMVAREJO,
        CAST(RESUMO.VALPRECOVAREJO AS VARCHAR(20)) AS VALPRECOVAREJO ,
        RESUMO.DTFIMPROMOCAOVAR,
        RESUMO.EMBALAGEMSAIDA,
        RESUMO.FABRICANTE,
        RESUMO.REFERENCIA,
        MAX(RESUMO.DTALTERACAO) AS DTALTERECAO,
        CAST(MAX(RESUMO.QTDSALDODISPONIVEL) AS VARCHAR(20))AS QTDSALDODISPONIVEL
FROM
(SELECT
        ESTOQUE.IDPRODUTO,
        ESTOQUE.IDSUBPRODUTO,
        ESTOQUE.DESCRCOMPRODUTO,
        ESTOQUE.EMBALAGEMSAIDA,
        ESTOQUE.FLAGLOTE,
        ESTOQUE.DESCRLOTE,
        ESTOQUE.DTALTERACAO,
        ESTOQUE.MULTIVENDAS,
        ESTOQUE.VALPROMVAREJO,
        ESTOQUE.VALPRECOVAREJO,
        ESTOQUE.DTFIMPROMOCAOVAR,
        ESTOQUE.FABRICANTE,
        ESTOQUE.REFERENCIA,
        ESTOQUE.QTDSALDODISPONIVEL/ESTOQUE.MULTIVENDAS AS QTDSALDODISPONIVEL
FROM
(SELECT
        TMP.IDEMPRESA,
        TMP.IDPRODUTO,
        TMP.IDSUBPRODUTO,
        TMP.IDLOCALESTOQUE,
        ECL.DESCRLOCAL AS DESCRLOCALESTOQUE,
        TMP.FLAGLOTE,
        TMP.IDLOTE AS DESCRLOTE,
        TMP.FLAGESTNEGATIVO,
        TMP.QTDATUALESTOQUE AS QTDSALDOATUAL,
        TMP.QTDSALDORESERVA,
        (TMP.QTDATUALESTOQUE - TMP.QTDSALDORESERVA) AS QTDSALDODISPONIVEL,
        TMP.DTALTERACAO,
        CEP.IDCONFIGECOMMERCE,
        CEP.DESCRICAO AS DESCRICAOECOMMERCE,
        CEP.TIPODADO AS TIPODADOECOMMERCE,
        COALESCE (PGE.DESCRICAO,'') AS FLAGECOMMERCE,
        PV.VALMULTIVENDAS AS MULTIVENDAS,
        PV.DESCRCOMPRODUTO,
        PV.EMBALAGEMSAIDA,
        PPP.VALPROMVAREJO,
        PPP.VALPRECOVAREJO,
        PPP.DTFIMPROMOCAOVAR,
        PV.FABRICANTE,
        PV.REFERENCIA
    FROM
        (
        SELECT
            COALESCE((
                CASE WHEN PG.FLAGLOTE = 'T' THEN
                    ESL.QTDATUALESTOQUE
                ELSE
                    ESA.QTDATUALESTOQUE
                END),0) AS QTDATUALESTOQUE,
            COALESCE((
                CASE WHEN CG.FLAGATIVARESERVA = 'T' OR CG.FLAGINATIVARESERVAVENDAFUTURA = 'T' OR CG.FLAGATIVARESERVAPRENOTA = 'T' OR CG.FLAGATIVARESERVAORCAMENTO = 'T' THEN
                    (SELECT
                        SUM(EAT.QTDPRODUTO)
                    FROM
                        DBA.ESTOQUE_ANALITICO_TMP EAT
                    LEFT OUTER JOIN DBA.ORCAMENTO O ON (
                       EAT.NUMPEDIDO = O.IDORCAMENTO AND
                        EAT.IDEMPRESA = O.IDEMPRESA
                        )
                    WHERE
                        EAT.IDPRODUTO = PG.IDPRODUTO AND (
                            (EAT.IDSUBPRODUTO = PG.IDSUBPRODUTO AND PROD.TIPOBAIXAMESTRE = 'I') OR (PROD.TIPOBAIXAMESTRE <> 'I')
                        ) AND (
                            (COALESCE(PG.FLAGLOTE,'F') <> 'T' AND EAT.IDEMPRESABAIXAEST = ESA.IDEMPRESA AND EAT.IDLOCALESTOQUE = ESA.IDLOCALESTOQUE) OR
                            (COALESCE(PG.FLAGLOTE,'F') = 'T' AND EAT.IDEMPRESABAIXAEST = ESL.IDEMPRESA AND EAT.IDLOCALESTOQUE = ESL.IDLOCALESTOQUE AND EAT.IDLOTE = ESL.IDLOTE)
                        )
                        AND
                        (
                         EAT.TIPODOCUMENTO NOT IN('X','O','N','P','U','C','M')
                         OR (
                            EAT.TIPODOCUMENTO IN('X','O') AND
                            (
                                (
                                    COALESCE(O.DTVALIDADE,date('1900-01-01')) >= DBA.TODAY() OR O.FLAGPRENOTAPAGA = 'T'
                                ) OR (
                                    CG.FLAGATIVARESERVA = 'T' AND
                                    CG.FLAGATIVARESERVAORCAMENTO = 'T' AND
                                    EAT.TIPODOCUMENTO = 'O' AND
                                    COALESCE(EAT.TIPOENTREGA,'') NOT IN ('A','E') AND
                                    EAT.NUMPEDIDO IS NULL
                                )
                            )
                        )
                        OR (
                            CG.FLAGATIVARESERVA = 'T' AND
                            CG.FLAGATIVARESERVAPRENOTA = 'T' AND
                            EAT.TIPODOCUMENTO = 'X' AND
                            COALESCE(EAT.TIPOENTREGA,'') NOT IN ('A','E') AND
                            EAT.NUMPEDIDO IS NULL
                        )
                        OR (
                            CG.FLAGATIVARESERVA = 'T' AND
                            EAT.TIPODOCUMENTO IN ('N','P','U','C')
                        )
                        OR (
                            CG.FLAGINATIVARESERVAVENDAFUTURA = 'T' AND
                            EAT.TIPODOCUMENTO = 'M'
                        )
                    )
                )
                ELSE
                    CAST(0 AS DECIMAL(15,3))
                END),0) AS QTDSALDORESERVA,
            CASE WHEN PG.FLAGLOTE = 'T' THEN
                ESL.IDLOCALESTOQUE
            ELSE
                ESA.IDLOCALESTOQUE
            END AS IDLOCALESTOQUE,
            CASE WHEN PG.FLAGLOTE = 'T' THEN
                ESL.IDEMPRESA
            ELSE
                ESA.IDEMPRESA
            END AS IDEMPRESA,
            PG.IDPRODUTO,
            PG.IDSUBPRODUTO,
            PG.FLAGESTNEGATIVO,
            COALESCE(PG.FLAGLOTE,'F') AS FLAGLOTE,
            ESL.IDLOTE,
            CASE WHEN PG.FLAGLOTE = 'T' THEN
                COALESCE(ESA.DTALTERACAO, TIMESTAMP(ESL.DTMOVIMENTO))
            ELSE
                COALESCE(ESA.DTALTERACAO, TIMESTAMP(ESA.DTMOVIMENTO))
            END AS DTALTERACAO
        FROM
            DBA.CONFIG_GERAL CG,
            DBA.PRODUTO_GRADE PG
        INNER JOIN
            DBA.PRODUTO PROD ON (
                PG.IDPRODUTO = PROD.IDPRODUTO
            )
        LEFT OUTER JOIN
            (SELECT
                E.IDPRODUTO,
                E.IDSUBPRODUTO,
                E.IDLOCALESTOQUE,
                E.IDEMPRESA,
                E.QTDATUALESTOQUE,
                E.DTMOVIMENTO,
                E.DTALTERACAO
            FROM
                DBA.ESTOQUE_SALDO_ATUAL E
            WHERE
                E.IDEMPRESA IN ('13','26','1') AND
                E.IDLOCALESTOQUE IN ('6','124','42')
                --AND
-- (IN_IDEMPRESA = 0 OR E.IDEMPRESA = IN_IDEMPRESA) AND
-- (IN_IDLOCALESTOQUE = 0 OR E.IDLOCALESTOQUE = IN_IDLOCALESTOQUE) AND
-- (IN_IDPRODUTO = 0 OR E.IDPRODUTO = IN_IDPRODUTO) AND
-- (IN_IDSUBPRODUTO = 0 OR E.IDSUBPRODUTO = IN_IDSUBPRODUTO)
                AND (
                    SELECT
                        COUNT(0)
                    FROM
                        DBA.PRODUTO_GRADE P
                    WHERE
                        P.IDSUBPRODUTO = E.IDSUBPRODUTO AND
                        P.FLAGLOTE = 'T'
                ) = 0
            ) ESA ON (
                PG.IDPRODUTO = ESA.IDPRODUTO AND
                PG.IDSUBPRODUTO = ESA.IDSUBPRODUTO
                )
        LEFT OUTER JOIN
            (SELECT
                ESLO.IDLOTE,
                ESLO.IDEMPRESA,
                ESLO.IDPRODUTO,
                ESLO.IDSUBPRODUTO,
                ESLO.IDLOCALESTOQUE,
                ESLO.DTMOVIMENTO,
                ESLO.QTDATUALESTOQUE
            FROM
                DBA.ESTOQUE_SINTETICO_LOTE ESLO
            WHERE
-- (IN_IDEMPRESA = 0 OR ESLO.IDEMPRESA = IN_IDEMPRESA) AND
                ESLO.IDEMPRESA IN ('13','26') AND
                ESLO.IDLOCALESTOQUE IN ('6','124')
                --AND
-- (IN_IDLOCALESTOQUE = 0 OR ESLO.IDLOCALESTOQUE = IN_IDLOCALESTOQUE) AND
-- (IN_IDPRODUTO = 0 OR ESLO.IDPRODUTO = IN_IDPRODUTO) AND
-- (IN_IDSUBPRODUTO = 0 OR ESLO.IDSUBPRODUTO = IN_IDSUBPRODUTO)
                AND (
                    SELECT
                        COUNT(0)
                    FROM
                        DBA.PRODUTO_GRADE P
                    WHERE
                        P.IDSUBPRODUTO = ESLO.IDSUBPRODUTO AND
                        P.FLAGLOTE = 'T'
                ) > 0
                AND ESLO.DTMOVIMENTO = (
                    SELECT
                       MAX(TMP.DTMOVIMENTO)
                    FROM
                        DBA.ESTOQUE_SINTETICO_LOTE AS TMP
                    WHERE
                        TMP.IDPRODUTO = ESLO.IDPRODUTO AND
                        TMP.IDSUBPRODUTO = ESLO.IDSUBPRODUTO AND
                        TMP.IDEMPRESA = ESLO.IDEMPRESA AND
                        TMP.IDLOTE = ESLO.IDLOTE AND
                        TMP.IDLOCALESTOQUE = ESLO.IDLOCALESTOQUE
                )
            ) ESL ON (
                PG.IDPRODUTO = ESL.IDPRODUTO AND
                PG.IDSUBPRODUTO = ESL.IDSUBPRODUTO
            )
-- WHERE
-- (IN_IDPRODUTO = 0 OR PG.IDPRODUTO = IN_IDPRODUTO ) AND
-- (IN_IDSUBPRODUTO = 0 OR PG.IDSUBPRODUTO = IN_IDSUBPRODUTO)
        ) AS TMP
        INNER JOIN DBA.ESTOQUE_CADASTRO_LOCAL AS ECL ON(
            ECL.IDLOCALESTOQUE = TMP.IDLOCALESTOQUE
        )
        LEFT JOIN DBA.PRODUTO_GRADE_ECOMMERCE AS PGE ON(
            PGE.IDPRODUTO = TMP.IDPRODUTO AND
            PGE.IDSUBPRODUTO = TMP.IDSUBPRODUTO
        )
                LEFT JOIN DBA.CONFIG_ECOMMERCE_PROPRIEDADES AS CEP ON(
            CEP.IDCONFIGECOMMERCE = PGE.IDCONFIGECOMMERCE
        )
                LEFT JOIN DBA.PRODUTOS_VIEW AS PV ON (
            PV.IDPRODUTO = TMP.IDPRODUTO AND
            PV.IDSUBPRODUTO = TMP.IDSUBPRODUTO
        )
        LEFT JOIN DBA.POLITICA_PRECO_PRODUTO AS PPP ON(
           PPP.IDPRODUTO = TMP.IDPRODUTO AND
           PPP.IDSUBPRODUTO = TMP.IDSUBPRODUTO AND
           PPP.IDEMPRESA = '33')
    WHERE
-- (IN_IDEMPRESA IN (13,26)) AND
-- (IN_IDEMPRESA = 0 OR TMP.IDEMPRESA = IN_IDEMPRESA) AND
        ECL.FLAGDISPONVENDA = 'T') AS ESTOQUE
WHERE
        ESTOQUE.FLAGECOMMERCE='T'
GROUP BY
        ESTOQUE.IDPRODUTO,
        ESTOQUE.IDSUBPRODUTO,
        ESTOQUE.FLAGLOTE,
        ESTOQUE.DESCRLOTE,
        ESTOQUE.DESCRCOMPRODUTO,
        ESTOQUE.EMBALAGEMSAIDA,
        ESTOQUE.QTDSALDODISPONIVEL,
        ESTOQUE.VALPROMVAREJO,
        ESTOQUE.VALPRECOVAREJO,
        ESTOQUE.DTFIMPROMOCAOVAR,
        ESTOQUE.MULTIVENDAS,
        ESTOQUE.FABRICANTE,
        ESTOQUE.REFERENCIA,
        ESTOQUE.DTALTERACAO ) AS RESUMO
GROUP BY

        RESUMO.IDSUBPRODUTO,
        RESUMO.DESCRCOMPRODUTO,
        RESUMO.MULTIVENDAS,
        RESUMO.VALPROMVAREJO,
        RESUMO.VALPRECOVAREJO,
        RESUMO.DTFIMPROMOCAOVAR,
        RESUMO.FABRICANTE,
        RESUMO.REFERENCIA,
        RESUMO.EMBALAGEMSAIDA