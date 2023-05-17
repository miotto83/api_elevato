select p.*, cf.Nome_Cli_For Cliente, col.Nome_Cli_For Colocador, v.Nome_Cli_For Vendedor, ts.Nome_Servico Servico\
from pedido_servico p\
left join CLI_FOR cf on cf.COD_EMP = p.COD_EMP and cf.COD_CLI_FOR = p.COD_Cli\
left join CLI_FOR col on col.COD_EMP = p.COD_EMP and col.COD_CLI_FOR = p.COD_Colocador1\
left join CLI_FOR v on v.COD_EMP = p.COD_EMP and v.COD_CLI_FOR = p.COD_Vendedor\
left join Tipo_Servico ts on ts.COD_EMP = p.COD_EMP and ts.COD_Servico = p.COD_Servico\
where p.COD_EMP = 1\
and (p.Cotacao = 'N' or p.Cotacao is null)\
}
