SELECT

    idcondicao,

    diasintervalo,

    qtdpagamentos,

    condicoes_pagrec.idxmargemcontrib,

    cast( qtdpagamentos || 'X'|| ' - ' || DESCRRECEBIMENTO || 'COM PRIMEIRO PAGTO PARA 30 DIAS' as varchar(100)) as descricaocondicao


FROM

    dba.condicoes_pagrec as condicoes_pagrec
    join dba.forma_pagrec as forma_pagrec
    on condicoes_pagrec.idrecebimento = forma_pagrec.idrecebimento

WHERE

 condicoes_pagrec.idrecebimento = ':IDFORMAPGAMENTO' and condicoes_pagrec.flagentrada = 'F' 