select cast(round(ppp.VALPRECOVAREJO,2) as varchar(20)) as VALPRECOVAREJO,
                (case when ppp.dtfimpromocaovar < current date then 0
                else
                cast(round(ppp.valpromvarejo,2) as varchar(20)) end )as VALPROMVAREJO
from dba.politica_preco_produto as ppp
where ppp.idempresa=':IDEMPRESA' and ppp.idproduto=':IDPRODUTO'