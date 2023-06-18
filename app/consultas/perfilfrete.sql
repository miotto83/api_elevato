SELECT
                        IDPERFIL,
			DESCRICAO,
			IDREGIAO,
			DESCRREGIAO,
			CAST(VALORFRETE AS VARCHAR(20)) AS VALORFRETE
                    FROM

                        DBA.PERFIL_FRETE,
    			DBA.TABELA_REGIAO