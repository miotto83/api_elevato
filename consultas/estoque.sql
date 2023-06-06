select CAST(
        (
            (
                qtdatualestoque -(
                    qtdsaldoreserva + qtdsaldovendafutura + qtdsaldoemtransferencia
                )
            ) / VALMULTIVENDAS
        ) AS VARCHAR(20)
    ) as qtddisponivel
from (
        SELECT IDPRODUTO,
            IDSUBPRODUTO,
            SUM(QTDATUALESTOQUE) AS QTDATUALESTOQUE,
            SUM(QTDSALDORESERVA) AS QTDSALDORESERVA,
            SUM(QTDSALDOVENDAFUTURA) AS QTDSALDOVENDAFUTURA,
            SUM(QTDSALDOEMTRANSFERENCIA) AS QTDSALDOEMTRANSFERENCIA
        FROM (
                SELECT CAST ('F' AS VARCHAR (1)) AS PONTAESTOQUE,
                    CAST ('' AS VARCHAR (1)) AS SINAL5,
                    ESA.IDEMPRESA,
                    ESA.IDPRODUTO,
                    ESA.IDSUBPRODUTO,
                    ESA.IDLOCALESTOQUE,
                    ESA.DTMOVIMENTO,
                    ESA.QTDATUALESTOQUE,
                    ECL.FLAGTROCAPROD,
                    ECL.FLAGDISPONVENDA,
                    ESA.VALATUALESTOQUE,
                    EMP.NOMEFANTASIA,
                    ECL.DESCRLOCAL AS NOMELOCALESTOQUE,
                    PV.VALGRAMAENTRADA,
                    CASE
                        WHEN ESA.QTDATUALESTOQUE = 0 THEN 'T'
                        ELSE 'F'
                    END AS FLAGLOCALINEXISTENTE,
                    VALCUSTOMEDIO,
                    COALESCE (PSV.QTDSALDORESERVA, 0) AS QTDSALDORESERVA,
                    CAST (0 AS DECIMAL (15, 3)) AS QTDSALDOVENDAFUTURA,
                    CAST (0 AS DECIMAL (15, 3)) AS QTDSALDOEMTRANSFERENCIA,
                    CASE
                        WHEN FABRICANTE = '' THEN 'SEM FABRICANTE'
                        ELSE coalesce(fabricante, 'SEM FABRICANTE')
                    END AS FABRICANTE,
                    MARCA.IDMARCAFABRICANTE,
                    DIVISAO.IDDIVISAO,
                    PV.DESCRICAOPRODUTO,
                    PV.REFERENCIA,
                    DIVISAO.DESCRDIVISAO AS DIVISAO,
                    ESA.DTULTIMAVENDA,
                    ESA.DTULTIMACOMPRA
                FROM DBA.PRODUTOS_VIEW AS PV
                    JOIN DBA.ESTOQUE_SALDO_ATUAL ESA ON ESA.IDPRODUTO = PV.IDPRODUTO
                    AND ESA.IDSUBPRODUTO = PV.IDSUBPRODUTO
                    JOIN DBA.ESTOQUE_CADASTRO_LOCAL ECL ON ESA.IDLOCALESTOQUE = ECL.IDLOCALESTOQUE
                    JOIN DBA.EMPRESA EMP ON ESA.IDEMPRESA = EMP.IDEMPRESA
                    JOIN DBA.DIVISAO AS DIVISAO ON PV.IDDIVISAO = DIVISAO.IDDIVISAO
                    JOIN DBA.MARCA AS MARCA ON (PV.IDMARCAFABRICANTE = MARCA.IDMARCAFABRICANTE)
                    LEFT OUTER JOIN (
                        SELECT SUM (PV.QTDSALDORESERVA) AS QTDSALDORESERVA,
                            PV.IDLOCALESTOQUE,
                            PV.IDEMPRESA,
                            PV.IDPRODUTO,
                            PV.IDSUBPRODUTO
                        FROM DBA.PRODUTOS_SALDOS_VIEW PV
                            INNER JOIN DBA.PRODUTO PRO1 ON PRO1.IDPRODUTO = PV.IDPRODUTO
                        GROUP BY PV.IDLOCALESTOQUE,
                            PV.IDEMPRESA,
                            PV.IDPRODUTO,
                            PV.IDSUBPRODUTO
                    ) AS PSV ON ESA.IDPRODUTO = PSV.IDPRODUTO
                    AND ESA.IDSUBPRODUTO = PSV.IDSUBPRODUTO
                    AND ESA.IDLOCALESTOQUE = PSV.IDLOCALESTOQUE
                    AND ESA.IDEMPRESA = PSV.IDEMPRESA
                    LEFT OUTER JOIN DBA.CONFIG_ENTIDADE CE ON (
                        CE.ENTIDADE = 'CME'
                        AND CE.CHAVE1 = 1
                        AND CE.CHAVE2 = 1
                    )
                WHERE ESA.IDLOCALESTOQUE IN (6, 124)
            ) AS TEMP
        GROUP BY IDPRODUTO,
            IDSUBPRODUTO
        UNION ALL
        SELECT VF.IDPRODUTO,
            VF.IDSUBPRODUTO,
            CAST (0 AS DECIMAL) AS QTDATUALESTOQUE,
            CAST (0 AS DECIMAL) AS QTDSALDORESERVA,
            SUM(VF.VENDASFUTURASPENDENTES) AS QTDSALDOVENDAFUTURA,
            CAST (0 AS DECIMAL (15, 3)) AS QTDSALDOEMTRANSFERENCIA
        FROM (
                SELECT SUM (NOTAS_SALDOS.QTDSALDOATUAL) AS VENDASFUTURASPENDENTES,
                    NOTAS_SALDOS.IDPRODUTO,
                    NOTAS_SALDOS.IDSUBPRODUTO,
                    NOTAS_SALDOS.IDEMPRESA
                FROM DBA.NOTAS_SALDOS AS NOTAS_SALDOS,
                    DBA.NOTAS_VFUTURA AS NOTAS_VFUTURA
                WHERE NOTAS_SALDOS.IDEMPRESA = NOTAS_VFUTURA.IDEMPRESA
                    AND NOTAS_SALDOS.IDPLANILHAORIGEM = NOTAS_VFUTURA.IDPLANILHA
                    AND NOTAS_SALDOS.IDPRODUTO = NOTAS_VFUTURA.IDPRODUTO
                    AND NOTAS_SALDOS.IDSUBPRODUTO = NOTAS_VFUTURA.IDSUBPRODUTO
                    AND NOTAS_SALDOS.QTDSALDOATUAL > 0
                    AND COALESCE (NOTAS_SALDOS.IDLOTE, '') = COALESCE (NOTAS_VFUTURA.IDLOTE, '')
                    AND NOT EXISTS (
                        SELECT 1
                        FROM DBA.ESTOQUE_SALDO_ATUAL ESA
                        WHERE ESA.IDPRODUTO = NOTAS_SALDOS.IDPRODUTO
                            AND ESA.IDSUBPRODUTO = NOTAS_SALDOS.IDSUBPRODUTO
                    )
                GROUP BY NOTAS_SALDOS.IDEMPRESA,
                    NOTAS_SALDOS.IDPRODUTO,
                    NOTAS_SALDOS.IDSUBPRODUTO
            ) AS VF
        GROUP BY VF.IDPRODUTO,
            VF.IDSUBPRODUTO
    ) as resumo
    left join dba.produtos_view as pv on resumo.idproduto = pv.idproduto
    and resumo.idsubproduto = pv.idsubproduto
where resumo.idproduto = ':IDPRODUTO'