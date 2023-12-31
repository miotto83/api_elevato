SELECT
            CLIENTE_FORNECEDOR.IDCLIFOR,
            CLIENTE_FORNECEDOR.TIPOFISICAJURIDICA,
            CLIENTE_FORNECEDOR.TIPOCADASTRO,
            CLIENTE_FORNECEDOR.FLAGINATIVO,
            CLIENTE_FORNECEDOR.NOME,
            CLIENTE_FORNECEDOR.NOMEFANTASIA,
            CLIENTE_FORNECEDOR.CNPJCPF,
            TB_PESSOA_FISICA.RG,
            TB_PESSOA_FISICA.EMISSORRG,
            TB_PESSOA_FISICA.TIPOESTADOCIVIL,
            TB_PESSOA_FISICA.DTNASCIMENTO,
            CLIENTE_FORNECEDOR.DTCADASTRO,
            CLIENTE_FORNECEDOR.ENDERECO,
            CLIENTE_FORNECEDOR.NUMERO,
            CLIENTE_FORNECEDOR.BAIRRO,
            TB_CIDADES.DESCRCIDADE,
            CLIENTE_FORNECEDOR.IDCEP,
            CLIENTE_FORNECEDOR.COMPLEMENTO,
            CLIENTE_FORNECEDOR.UFCLIFOR AS UF,
            TB_REGIAO.DESCRREGIAO,
            CLIENTE_FORNECEDOR.OBSGERAL,
            CLIENTE_FORNECEDOR.NOMECONTATO1,
            CLIENTE_FORNECEDOR.NOMECONTATO2,
            CLIENTE_FORNECEDOR.FONE2,
            CLIENTE_FORNECEDOR.FONE1,
            CLIENTE_FORNECEDOR.FONEFAX,
            CLIENTE_FORNECEDOR.FONECELULAR,
            CLIENTE_FORNECEDOR.EMAIL,
            CLIENTE_FORNECEDOR.INSCRMUNICIPAL,
            CLIENTE_FORNECEDOR.INSCRESTADUAL,
            TB_ATIVIDADE.DESCRATIVIDADE,
            TB_GRUPO_ECONOMICO.DESCRGRUPOECONOMICO,
            TB_REDE_NEGOCIOS.IDREDENEGOCIO,
            TB_REDE_NEGOCIOS.DESCRREDENEGOCIO,
            TB_SITUACAO.DESCRSITUACAO,
            CLIENTE_FORNECEDOR.VALLIMITECONVENIO,
            CLIENTE_FORNECEDOR.VALLIMITECREDITO,
            CLIENTE_FORNECEDOR.TIPOREGIMETRIBFEDERAL,
            CLIENTE_FORNECEDOR.TIPOREGIMETRIBUTACAO,
            CLIENTE_FORNECEDOR.DTALTERACAO,
            TB_AUTORIZADOS.TIPOSEXO AS GENERO,
            CLIENTE_FORNECEDOR.IDCONVENIO,
            TB_SITUACAO.STATUSFINAN,
            TB_CIDADES.CODIGOIBGE,
            CASE WHEN
                CLIENTE_FORNECEDOR.INSCRESTADUAL IS NOT NULL
            THEN
                'T'
            ELSE
                'F'
            END AS CONTRIBUINTE,
            CLIENTE_FORNECEDOR.IDVENDEDOR,
            VENDEDOR.NOME AS NOMEVENDEDOR
        FROM
            DBA.CLIENTE_FORNECEDOR AS CLIENTE_FORNECEDOR
            LEFT JOIN
                (
                    SELECT
                        CIDADES_IBGE.IDCIDADE,
                        CIDADES_IBGE.DESCRCIDADE,
                        CIDADES_IBGE.CODIGOIBGE
                    FROM
                        DBA.CIDADES_IBGE AS CIDADES_IBGE
                ) AS TB_CIDADES ON(
                TB_CIDADES.IDCIDADE = CLIENTE_FORNECEDOR.IDCIDADE
            )
            LEFT JOIN
                (   SELECT
                        TABELA_REGIAO.IDREGIAO,
                        TABELA_REGIAO.DESCRREGIAO
                    FROM
                        DBA.TABELA_REGIAO AS TABELA_REGIAO
                ) AS TB_REGIAO ON(
                TB_REGIAO.IDREGIAO = CLIENTE_FORNECEDOR.IDREGIAO
            )
            LEFT JOIN
                (   SELECT
                        ATIVIDADE.IDATIVIDADE,
                        ATIVIDADE.DESCRATIVIDADE
                    FROM
                        DBA.ATIVIDADE AS ATIVIDADE
                ) AS TB_ATIVIDADE ON(
                TB_ATIVIDADE.IDATIVIDADE = CLIENTE_FORNECEDOR.IDATIVIDADE
            )
            LEFT JOIN
                (   SELECT
                        GRUPO_ECONOMICO.IDGRUPOECONOMICO,
                        GRUPO_ECONOMICO.DESCRGRUPOECONOMICO
                    FROM
                        DBA.GRUPO_ECONOMICO AS GRUPO_ECONOMICO
                ) AS TB_GRUPO_ECONOMICO ON (
                TB_GRUPO_ECONOMICO.IDGRUPOECONOMICO = CLIENTE_FORNECEDOR.IDGRUPOECONOMICO
            )
            LEFT JOIN
                (   SELECT
                        REDE_NEGOCIOS.IDREDENEGOCIO,
                        REDE_NEGOCIOS.DESCRREDENEGOCIO
                    FROM
                        DBA.REDE_NEGOCIOS AS REDE_NEGOCIOS
                ) AS TB_REDE_NEGOCIOS ON (
                TB_REDE_NEGOCIOS.IDREDENEGOCIO = CLIENTE_FORNECEDOR.IDREDENEGOCIO
            )
            LEFT JOIN
                (   SELECT
                        TABELA_SITUACAO.IDSITUACAO,
                        TABELA_SITUACAO.DESCRSITUACAO,
                        CASE WHEN
                            TABELA_SITUACAO.TIPOTESTECLIENTE = 'B'
                        THEN
                            'B'
                        ELSE
                            'L'
                        END AS STATUSFINAN
                    FROM
                        DBA.TABELA_SITUACAO AS TABELA_SITUACAO
                ) AS TB_SITUACAO ON(
                TB_SITUACAO.IDSITUACAO = CLIENTE_FORNECEDOR.IDSITUACAO
            )
            LEFT JOIN
                (   SELECT
                        PESSOA_FISICA.IDCLIFOR,
                        PESSOA_FISICA.RG,
                        PESSOA_FISICA.EMISSORRG,
                        PESSOA_FISICA.TIPOESTADOCIVIL,
                        PESSOA_FISICA.DTNASCIMENTO
                    FROM
                        DBA.PESSOA_FISICA AS PESSOA_FISICA
                ) AS TB_PESSOA_FISICA ON(
                TB_PESSOA_FISICA.IDCLIFOR = CLIENTE_FORNECEDOR.IDCLIFOR
            )
            LEFT JOIN
                (   SELECT
                        CLIENTE_AUTORIZADOS.IDCLIFOR,
                        CLIENTE_AUTORIZADOS.TIPOSEXO
                    FROM
                        DBA.CLIENTE_AUTORIZADOS AS CLIENTE_AUTORIZADOS
                    WHERE
                        CLIENTE_AUTORIZADOS.TIPOGRAUDEPENDENCIA = 'T'
                ) AS TB_AUTORIZADOS ON(
                TB_AUTORIZADOS.IDCLIFOR = CLIENTE_FORNECEDOR.IDCLIFOR
            )
            LEFT JOIN
                DBA.CLIENTE_FORNECEDOR AS VENDEDOR ON
                    (CLIENTE_FORNECEDOR.IDVENDEDOR = VENDEDOR.IDCLIFOR)
            WHERE CLIENTE_FORNECEDOR.FLAGINATIVO = 'F' AND (CLIENTE_FORNECEDOR.CNPJCPF = ':CNPJCPF' OR
            CLIENTE_FORNECEDOR.FONE2 = ':CNPJCPF' OR
            CLIENTE_FORNECEDOR.FONE1 = ':CNPJCPF' OR
            CLIENTE_FORNECEDOR.FONEFAX = ':CNPJCPF' OR
            CLIENTE_FORNECEDOR.FONECELULAR = ':CNPJCPF')