SELECT IDDOCUMENTO,NUMSEQUENCIA,PERCMARGEMCONTRIBUICAO,PERCMARGEMCONTRIBUICAOMEDIA,VALCUSTOREPOS
    FROM DBA.MARGEM_CONTRIBUICAO
    WHERE IDDOCUMENTO= :IDORCAMENTO
