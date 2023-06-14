SELECT DISTINCT CAST(PRODUTOS_VIEW.IDSUBPRODUTO AS VARCHAR(20)) AS IDSUBPRODUTO
FROM DBA.PRODUTOS_VIEW AS PRODUTOS_VIEW
    LEFT JOIN DBA.DIVISAO AS DIVISAO ON (DIVISAO.IDDIVISAO = PRODUTOS_VIEW.IDDIVISAO)
    LEFT JOIN DBA.SECAO AS SECAO ON (SECAO.IDSECAO = PRODUTOS_VIEW.IDSECAO)
    LEFT JOIN DBA.GRUPO AS GRUPO ON (GRUPO.IDGRUPO = PRODUTOS_VIEW.IDGRUPO)
    LEFT JOIN DBA.SUBGRUPO AS SUBGRUPO ON (SUBGRUPO.IDSUBGRUPO = PRODUTOS_VIEW.IDSUBGRUPO)
    LEFT JOIN DBA.PRODUTO_FORNECEDOR AS PRODUTO_FORNECEDOR ON (
        PRODUTO_FORNECEDOR.IDSUBPRODUTO = PRODUTOS_VIEW.IDSUBPRODUTO
        AND PRODUTO_FORNECEDOR.IDPRODUTO = PRODUTOS_VIEW.IDPRODUTO
        AND PRODUTO_FORNECEDOR.FLAGFORNECEDORPADRAO = 'T'
    )
    LEFT JOIN DBA.CLIENTE_FORNECEDOR AS CLIENTE_FORNECEDOR ON (
        CLIENTE_FORNECEDOR.IDCLIFOR = PRODUTO_FORNECEDOR.IDCLIFOR
    )
    LEFT JOIN DBA.GRUPO_ECONOMICO AS GRUPO_ECONOMICO ON (
        GRUPO_ECONOMICO.IDGRUPOECONOMICO = CLIENTE_FORNECEDOR.IDGRUPOECONOMICO
    )
    LEFT JOIN DBA.POLITICA_PRECO_PRODUTO AS PPP ON (
        PPP.IDPRODUTO = PRODUTOS_VIEW.IDPRODUTO
        AND PPP.IDSUBPRODUTO = PRODUTOS_VIEW.IDSUBPRODUTO
        AND PPP.IDEMPRESA = ':IDEMPRESA'
    )
    LEFT JOIN DBA.PRODUTO_GRADE AS PRODUTO_GRADE ON (
        PRODUTO_GRADE.IDPRODUTO = PRODUTOS_VIEW.IDPRODUTO
        AND PRODUTO_GRADE.IDSUBPRODUTO = PRODUTOS_VIEW.IDSUBPRODUTO
    )
where (
        PRODUTOS_VIEW.DESCRCOMPRODUTO LIKE '%:IDFABRICANTE%'
    )
    AND PRODUTOS_VIEW.FLAGINATIVO = 'F'