select 
                                                    cast(vendadia.valorvendadia as varchar(20)) as VALORVENDADIA,
                                                    cast(vendadia.valormargemdia as varchar(20)) as VALORMARGEMDIA,
                                                    cast(vendames.valorvendames as varchar(20)) as VALORVENDAMES,
                                                    cast(vendames.valormargemmes as varchar(20)) as VALORMARGEMMES,
                                                    cast(fretemes.VALORFRETEMES as varchar(20)) as VALORFRETEMES
from
(select SUM(VALORVENDALIQ)AS VALORVENDADIA,SUM(VALORVENDALIQ*PERMARGEM)/100 AS VALORMARGEMDIA from VENDAS_BI_ELEVATO
WHERE DATA = CURRENT DATE AND IDDEPARTAMENTO in (':IDDEPARTAMENTO')) as vendadia,
(select  SUM(VALORVENDALIQ)AS VALORVENDAMES,SUM(VALORVENDALIQ*PERMARGEM)/100 AS VALORMARGEMMES
from VENDAS_BI_ELEVATO
WHERE (MONTH(DATA) = MONTH(CURRENT DATE) AND YEAR(DATA)= YEAR(CURRENT DATE))
AND IDDEPARTAMENTO in (':IDDEPARTAMENTO') ) as vendames,
(select  SUM(VALORFRETEVENDA)AS VALORFRETEMES
from VENDAS_BI_ELEVATO
WHERE (MONTH(DATA) = MONTH(CURRENT DATE) AND YEAR(DATA)= YEAR(CURRENT DATE))
AND IDDEPARTAMENTO in (':IDDEPARTAMENTO') ) as fretemes ORDER BY VALORVENDAMES DESC