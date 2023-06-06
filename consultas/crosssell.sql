select principal.IDSUBPRODUTO,
    principal.idsubproduto_p2,
    principal.num_sales,
    principal.valor_p1,
    principal.valor_p2,
    principal.margem_per,
    principal.valor_p2 * principal.margem_per / 100 as margem_valor,
    tp.descr_p1,
    tp.grupo_p1,
    tp.subgrupo_p1,
    tp2.descr_p2,
    tp2.grupo_p2,
    tp2.subgrupo_p2
from (
        select t1.IDSUBPRODUTO,
            t2.IDSUBPRODUTO as idsubproduto_p2,
            count(*) as num_sales,
            sum(t1.VALORVENDALIQ) as valor_p1,
            sum(t2.VALORVENDALIQ) as valor_p2,
            avg(t2.PERMARGEM) as margem_per
        from (
                select *
                from vendas_bi_elevato
                where tipovenda = 'NORMAL'
                    and data between current date -365 and current date
                    and idsubproduto = :IDSUBPRODUTO
            ) t1
            join (
                select *
                from vendas_bi_elevato
                where tipovenda = 'NORMAL'
                    and data between current date -365 and current date
            ) t2 on t1.IDORCAMENTO = t2.IDORCAMENTO
            and t1.IDSUBPRODUTO <> t2.IDSUBPRODUTO
        group by t1.IDSUBPRODUTO,
            t2.IDSUBPRODUTO
        order by (count(*)) desc
    ) principal
    join (
        select IDSUBPRODUTO,
            DESCCOMPRODUTO as descr_p1,
            DESCRGRUPO as grupo_p1,
            DESCRSUBGRUPO as subgrupo_p1
        from produtos_bi_elevato
    ) tp on principal.IDSUBPRODUTO = tp.IDSUBPRODUTO
    join (
        select IDSUBPRODUTO,
            DESCCOMPRODUTO as descr_p2,
            DESCRGRUPO as grupo_p2,
            DESCRSUBGRUPO as subgrupo_p2
        from produtos_bi_elevato
    ) tp2 on principal.idsubproduto_p2 = tp2.IDSUBPRODUTO
order by num_sales desc
limit 20