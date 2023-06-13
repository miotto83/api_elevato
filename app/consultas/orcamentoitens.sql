select idorcamento,
    op.idproduto,
    op.idsubproduto,
    descrambiente,
    op.tipoentrega,
    numsequencia,
    op.idlocalretirada,
    lr.descrlocalretirada,
    idvendedor,
    --                idlocalentrega,
    idlote,
    pv.descrcomproduto,
    cast(pv.valmultivendas as varchar(20)) as VALMULTIVENDAS,
    pv.embalagemsaida,
    LR.idlocalestoque,
    cast(valdescontopro as varchar(20)) as VALDESCONTOPRO,
    cast(perdescontopro as varchar(20)) as PERDESCONTOPRO,
    cast(vallucro as varchar(20)) as VALLUCRO,
    cast(permargemlucro as varchar(20)) as PERMARGEMLUCRO,
    CAST(qtdproduto AS VARCHAR(20)) AS QTDPRODUTO,
    CAST(valunitbruto AS VARCHAR(20)) AS VALUNITBRUTO,
    CAST(valtotliquido AS VARCHAR(20)) AS VALTOTLIQUIDO,
    CAST(valfrete AS VARCHAR (20)) AS VALFRETE
from dba.orcamento_prod as op
    left join dba.produtos_view as pv on op.idproduto = pv.idproduto
    and op.idsubproduto = pv.idsubproduto
    left join dba.local_retirada as LR on lr.idlocalretirada = op.idlocalretirada
where idorcamento = ':IDORCAMENTO'