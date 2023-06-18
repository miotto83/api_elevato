SELECT  PV.idproduto, PV.DESCRCOMPRODUTO,
        produto_faixa_detalhe.idfaixa,
        produto_faixa_detalhe.idfaixadetalhe,
        produto_faixa_detalhe.idtamanho,
        produto_faixa.descrfaixa

FROM     DBA.PRODUTO_GRADE AS PRODUTO_GRADE
        left join
        DBA.produto_faixa_detalhe AS produto_faixa_detalhe
        on produto_grade.idtamanho=produto_faixa_detalhe.idtamanho
        left join DBA.produto_faixa AS produto_faixa
        on produto_faixa_detalhe.idfaixa = produto_faixa.idfaixa
        left join dba.produtos_view as pv
        on PRODUTO_GRADE.IDPRODUTO=PV.IDPRODUTO AND PRODUTO_GRADE.IDSUBPRODUTO=PV.IDSUBPRODUTO

where
        produto_faixa.idfaixa = ':IDFAIXATAMANHO' and pv.iddivisao = 13