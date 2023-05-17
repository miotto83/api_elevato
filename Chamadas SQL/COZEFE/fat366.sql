{\rtf1\ansi\ansicpg1252\cocoartf2709
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\froman\fcharset0 Times-Roman;}
{\colortbl;\red255\green255\blue255;\red0\green0\blue0;}
{\*\expandedcolortbl;;\cssrgb\c0\c0\c0;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\deftab720
\pard\pardeftab720\partightenfactor0

\f0\fs24 \cf0 \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 select p.*, cf.Nome_Cli_For Cliente, col.Nome_Cli_For Colocador, v.Nome_Cli_For Vendedor, ts.Nome_Servico Servico\
from pedido_servico p\
left join CLI_FOR cf on cf.COD_EMP = p.COD_EMP and cf.COD_CLI_FOR = p.COD_Cli\
left join CLI_FOR col on col.COD_EMP = p.COD_EMP and col.COD_CLI_FOR = p.COD_Colocador1\
left join CLI_FOR v on v.COD_EMP = p.COD_EMP and v.COD_CLI_FOR = p.COD_Vendedor\
left join Tipo_Servico ts on ts.COD_EMP = p.COD_EMP and ts.COD_Servico = p.COD_Servico\
where p.COD_EMP = 1\
and (p.Cotacao = 'N' or p.Cotacao is null)\
}