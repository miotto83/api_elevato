select distinct pv.idcodbarprod
from dba.produtos_view pv
where pv.idproduto = ':IDPRODUTO'