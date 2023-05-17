from fastapi import FastAPI
from fastapi import APIRouter
from typing import Optional,List
from fastapi import FastAPI, HTTPException, Response , Form
from pydantic import BaseModel
import sqlalchemy
from sqlalchemy import create_engine
import databases
import pandas as pd
import json
import psycopg2 as pg
import pyodbc
import datetime
import logging
import requests
from io import BytesIO
from PIL import Image
from fastapi.responses import StreamingResponse
from typing import List
from fastapi import FastAPI
from pydantic import BaseModel, Field
import uvicorn




app = FastAPI()

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(filename="app.log", mode="w"),
        logging.StreamHandler(),
    ],
)

@app.get("/")
def read_root():
    return {"Hello": "Elevato Api"}

#classe utilizada para gravação de cliente no CISS#
#novos campos devem ser adicionados nela#
class Cliente(BaseModel):
    cpfcnpj:str
    nome:str 
    rua:str 
    numero:int 
    complemento:str 
    bairro:str 
    cep:str
    #idcliente:str
    fonecelular:str
    email:str
    idusuariocadastro:str
    obsgeral:str
    inscestadual:str
    dtnascimento:str
    nomefantasia:str

#Classe utilizada para gravar pedido no ciss---novos campos devem ser 
#adicionados nela.
class Pedido(BaseModel):
    idempresa:str
    idclifor:str


class PedidoConsumidor(BaseModel):
    idempresa:str
    idclifor:str
    nome:str

#classe utilizada para dar orcamento como perdido
class OrcamentoPerdido(BaseModel):
    idorcamento:str

#classe utilizada para gravar itens do pedido no CISS--Novos Campos adicionar nela
class ItemPedido(BaseModel):
    idpedido:str
    idempresa:str
    idvendedor:str
    idproduto:str
    idsubproduto:str
    numsequencia:str
    idlote:str
    qtdproduto:float
    valunitbruto:float
    valtotliquido:float
    valdescontopro:float
    ambiente:str
    idlocalestoque:str


# class Busca_Cliente(BaseModel):
#     cpfcnpj:str


#busca cliente filtrando por cpfcnpj,descricao e telefone
@app.get("/cliente/{cpfcnpj}")
async def cliente_busca(cpfcnpj:str):
    """
    Esta chamada retorna dados de cadastro de cliente. Use o CPF 00881535923 para simular os dados que retornam.

    É passado um paramêtro chamado cpfcnpj que é utilizado para filtrar o cpfcnpj e número de telefone.
    Em cada execução ele retorna um único registro de cliente. Os campos que retornam serão apresentados na simulação.


    """
    #cpfcnpj:Busca_Cliente.cpfcnpj
    logging.info(f"Endpoint '/cliente/{cpfcnpj}' acessado com sucesso.")
#conexao com banco de dados do CISS Poder
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

#carrega dataframe com o select de busca..podem ser adicionados mais campos no sql conforme necessidade
    carrega_busca_cliente =pd.read_sql(f'''SELECT
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
            WHERE CLIENTE_FORNECEDOR.FLAGINATIVO = 'F' AND (CLIENTE_FORNECEDOR.CNPJCPF = '{cpfcnpj}' OR
            --CLIENTE_FORNECEDOR.NOME LIKE '%{cpfcnpj}%' OR
            CLIENTE_FORNECEDOR.FONE2 = '{cpfcnpj}' OR
            CLIENTE_FORNECEDOR.FONE1 = '{cpfcnpj}' OR
            CLIENTE_FORNECEDOR.FONEFAX = '{cpfcnpj}' OR
            CLIENTE_FORNECEDOR.FONECELULAR = '{cpfcnpj}') ''',conexao)

    cursor.commit()
    cursor.close()

    print(carrega_busca_cliente)

       
    for index, row in carrega_busca_cliente.iterrows():
        colunas = ["IDCLIFOR","NOME","NOMEFANTASIA","CNPJCPF","ENDERECO","IDCEP","NUMERO","BAIRRO","DESCRCIDADE","DTNASCIMENTO","COMPLEMENTO","UF","FONE2","FONE1","FONEFAX","FONECELULAR","EMAIL","CODIGOIBGE","GENERO","IDVENDEDOR","NOMEVENDEDOR"]
        dados_cliente_ciss_inicio = {coluna: row[coluna] for coluna in colunas}
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_cliente_ciss_inicio)
        if dados_cliente_ciss_inicio == {}:
            raise HTTPException(status_code=404, detail="CPf ou Telefone não encontrado")
        else:
            return dados_cliente_ciss_inicio


@app.get("/preco/{idproduto}&{idempresa}")
async def preco_busca(idproduto:str,
                        idempresa:str):
    """
    Esta chamada retorna o preço de venda varejo e o preço de venda promocional de um determinado produto em uma empresa.
    Utilize o código 1059453 e a empresa 1 para simular o retorno.
    Em cada execução ele retorna um único registro. Os campos que retornam serão apresentados na simulação.

    """
    #cpfcnpj:Busca_Cliente.cpfcnpj
    logging.info(f"Endpoint '/preco/{idproduto}&{idempresa}' acessado com sucesso.")
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_preco =pd.read_sql(f'''select cast(round(ppp.VALPRECOVAREJO,2) as varchar(20)) as VALPRECOVAREJO,
                (case when ppp.dtfimpromocaovar < current date then 0
                else
                cast(round(ppp.valpromvarejo,2) as varchar(20)) end )as VALPROMVAREJO
from dba.politica_preco_produto as ppp
where ppp.idempresa='{idempresa}' and ppp.idproduto='{idproduto}' ''',conexao)

    cursor.commit()
    cursor.close()

    for index, row in carrega_busca_preco.iterrows():
        colunas = ["VALPRECOVAREJO","VALPROMVAREJO"]
        dados_preco_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_preco_ciss)

    return dados_preco_ciss

@app.get("/produtocompleto/{idempresa}&{idprodutos}")
async def produto_busca(idempresa:str,
                        idprodutos:str):
    """
    Esta chamada retorna dados completos do cadastro de produto com estoque dos CDs e preço de venda na empresa passada como filtro.
    Ela retorna a informação de uma lista de produtos, que pode ser passada da seguinte forma:
    1059453,1081882,1056157
    Neste exemplo irá retornar dados dos 3 códigos listados.
    Para o exemplo utilize a empresa 1.
    Em cada execução ele retorna uma lista de registros( conforme a quantidade de códigos de produtos enviado).
    Os campos que retornam serão apresentados na simulação.

    """
    logging.info(f"Endpoint '/produtocompleto/{idempresa}&{idprodutos}' acessado com sucesso.")
                        # &{fabricantedescricao}&{estoquemaior},fabricantedescricao:str,
                        #estoquemaior:str
                        # paginas): &{paginas}
    #cpfcnpj:Busca_Cliente.cpfcnpj
    print(idprodutos)
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_produtocompleto =pd.read_sql(f'''SELECT
        IDSUBPRODUTO,
        VALMULTIVENDAS,
        PERCOMAVISTA,
        NCM,
        PESOBRUTO,
        EMBALAGEMSAIDA,
        LARGURA,
        ALTURA,
        COMPRIMENTO,
        DESCRCOMPRODUTO,
        MODELO,
        FABRICANTE,
        FLAGINATIVOCOMPRA,
        IDDIVISAO,
        DESCRDIVISAO,
        IDSECAO,
        DESCRSECAO,
        IDGRUPO,
        DESCRGRUPO,
        IDSUBGRUPO,
        DESCRSUBGRUPO,
        IDCLIFOR,
        IDGRUPOECONOMICO,
        DESCRGRUPOECONOMICO,
        VALPRECOVAREJO,
        VALPROMVAREJO,
        IDCODBARPROD,
        REFERENCIA,
        DTFIMPROMOCAOVAR,
        CAST(SUM(ESTOQUEGRAVATAI) AS VARCHAR(20)) AS ESTOQUEGRAVATAI,
        CAST(SUM(ESTOQUESEVERO) AS VARCHAR(20)) AS ESTOQUESEVERO
FROM
(SELECT
        PRODUTOS_VIEW.IDSUBPRODUTO,
        CAST(PRODUTOS_VIEW.VALMULTIVENDAS AS VARCHAR(20)) AS VALMULTIVENDAS,
        cAST(PRODUTOS_VIEW.PERCOMAVISTA AS VARCHAR(20)) AS PERCOMAVISTA,
        PRODUTOS_VIEW.NCM,
        CAST(PRODUTOS_VIEW.PESOBRUTO AS VARCHAR(20)) AS PESOBRUTO,
        EMBALAGEMSAIDA,
        CAST(LARGURA AS VARCHAR(20)) AS LARGURA,
        CAST(ALTURA AS VARCHAR(20)) AS ALTURA,
        CAST(COMPRIMENTO AS VARCHAR(20)) AS COMPRIMENTO,
        PRODUTOS_VIEW.DESCRCOMPRODUTO,
        PRODUTOS_VIEW.MODELO,
        PRODUTOS_VIEW.FABRICANTE,
        PRODUTOS_VIEW.FLAGINATIVOCOMPRA,
        DIVISAO.IDDIVISAO,
        DIVISAO.DESCRDIVISAO,
        SECAO.IDSECAO,
        SECAO.DESCRSECAO,
        GRUPO.IDGRUPO,
        GRUPO.DESCRGRUPO,
        SUBGRUPO.IDSUBGRUPO,
        SUBGRUPO.DESCRSUBGRUPO,
        PRODUTO_FORNECEDOR.IDCLIFOR,
        CLIENTE_FORNECEDOR.IDGRUPOECONOMICO,
        GRUPO_ECONOMICO.DESCRGRUPOECONOMICO,
        CAST(ROUND(PPP.VALPRECOVAREJO,2) AS VARCHAR(20)) AS VALPRECOVAREJO,
        CAST(ROUND((CASE WHEN PPP.DTFIMPROMOCAOVAR >=CURRENT DATE THEN PPP.VALPROMVAREJO ELSE 0 END),2) AS VARCHAR(20)) AS VALPROMVAREJO,
        PRODUTOS_VIEW.IDCODBARPROD,
        PRODUTOS_VIEW.REFERENCIA,
        PPP.DTFIMPROMOCAOVAR,
        0 AS ESTOQUEGRAVATAI,
        PSV1.QTDDISPONIVEL AS ESTOQUESEVERO
FROM
        DBA.PRODUTOS_VIEW AS PRODUTOS_VIEW
LEFT JOIN DBA.PRODUTOS_SALDOS_VIEW AS PSV1
 ON PRODUTOS_VIEW.IDPRODUTO=PSV1.IDPRODUTO AND PRODUTOS_VIEW.IDSUBPRODUTO=PSV1.IDSUBPRODUTO AND PSV1.IDEMPRESA=13 AND PSV1.IDLOCALESTOQUE=6 AND PSV1.QTDDISPONIVEL>0
                LEFT JOIN DBA.DIVISAO AS DIVISAO ON
                                        (DIVISAO.IDDIVISAO = PRODUTOS_VIEW.IDDIVISAO)
                LEFT JOIN DBA.SECAO AS SECAO ON
                                        (SECAO.IDSECAO = PRODUTOS_VIEW.IDSECAO)
                LEFT JOIN DBA.GRUPO AS GRUPO ON
                                        (GRUPO.IDGRUPO = PRODUTOS_VIEW.IDGRUPO)
                LEFT JOIN DBA.SUBGRUPO AS SUBGRUPO ON
                                        (SUBGRUPO.IDSUBGRUPO = PRODUTOS_VIEW.IDSUBGRUPO)
                LEFT JOIN DBA.PRODUTO_FORNECEDOR AS PRODUTO_FORNECEDOR ON
                                        (PRODUTO_FORNECEDOR.IDSUBPRODUTO = PRODUTOS_VIEW.IDSUBPRODUTO AND
                                        PRODUTO_FORNECEDOR.IDPRODUTO = PRODUTOS_VIEW.IDPRODUTO AND
                                        PRODUTO_FORNECEDOR.FLAGFORNECEDORPADRAO = 'T')
                LEFT JOIN DBA.CLIENTE_FORNECEDOR AS CLIENTE_FORNECEDOR ON
                                        (CLIENTE_FORNECEDOR.IDCLIFOR = PRODUTO_FORNECEDOR.IDCLIFOR)
                LEFT JOIN DBA.GRUPO_ECONOMICO AS GRUPO_ECONOMICO ON
                                        (GRUPO_ECONOMICO.IDGRUPOECONOMICO = CLIENTE_FORNECEDOR.IDGRUPOECONOMICO)
                LEFT JOIN DBA.POLITICA_PRECO_PRODUTO AS PPP ON
                                        (PPP.IDPRODUTO=PRODUTOS_VIEW.IDPRODUTO AND PPP.IDSUBPRODUTO=PRODUTOS_VIEW.IDSUBPRODUTO AND PPP.IDEMPRESA='{idempresa}')
                LEFT JOIN DBA.PRODUTO_GRADE AS PRODUTO_GRADE ON
                                        (PRODUTO_GRADE.IDPRODUTO=PRODUTOS_VIEW.IDPRODUTO AND PRODUTO_GRADE.IDSUBPRODUTO=PRODUTOS_VIEW.IDSUBPRODUTO)
                where  (PRODUTOS_VIEW.IDPRODUTO IN ({idprodutos})) AND PRODUTOS_VIEW.FLAGINATIVO='F'


        UNION ALL

        SELECT

        PRODUTOS_VIEW.IDSUBPRODUTO,
        CAST(PRODUTOS_VIEW.VALMULTIVENDAS AS VARCHAR(20)) AS VALMULTIVENDAS,
        cAST(PRODUTOS_VIEW.PERCOMAVISTA AS VARCHAR(20)) AS PERCOMAVISTA,
        PRODUTOS_VIEW.NCM,
        CAST(PRODUTOS_VIEW.PESOBRUTO AS VARCHAR(20)) AS PESOBRUTO,
        EMBALAGEMSAIDA,
        CAST(LARGURA AS VARCHAR(20)) AS LARGURA,
        CAST(ALTURA AS VARCHAR(20)) AS ALTURA,
        CAST(COMPRIMENTO AS VARCHAR(20)) AS COMPRIMENTO,
        PRODUTOS_VIEW.DESCRCOMPRODUTO,
        PRODUTOS_VIEW.MODELO,
        PRODUTOS_VIEW.FABRICANTE,
        PRODUTOS_VIEW.FLAGINATIVOCOMPRA,
        DIVISAO.IDDIVISAO,
        DIVISAO.DESCRDIVISAO,
        SECAO.IDSECAO,
        SECAO.DESCRSECAO,
        GRUPO.IDGRUPO,
        GRUPO.DESCRGRUPO,
        SUBGRUPO.IDSUBGRUPO,
        SUBGRUPO.DESCRSUBGRUPO,
        PRODUTO_FORNECEDOR.IDCLIFOR,
        CLIENTE_FORNECEDOR.IDGRUPOECONOMICO,
        GRUPO_ECONOMICO.DESCRGRUPOECONOMICO,
        CAST(ROUND(PPP.VALPRECOVAREJO,2) AS VARCHAR(20)) AS VALPRECOVAREJO,
        CAST(ROUND((CASE WHEN PPP.DTFIMPROMOCAOVAR >=CURRENT DATE THEN PPP.VALPROMVAREJO ELSE 0 END),2) AS VARCHAR(20)) AS VALPROMVAREJO,
        PRODUTOS_VIEW.IDCODBARPROD,
        PRODUTOS_VIEW.REFERENCIA,
        PPP.DTFIMPROMOCAOVAR,
        PSV2.QTDDISPONIVEL  AS ESTOQUEGRAVATAI,
        0 AS ESTOQUESEVERO
FROM
        DBA.PRODUTOS_VIEW AS PRODUTOS_VIEW
LEFT JOIN DBA.PRODUTOS_SALDOS_VIEW AS PSV2
 ON PRODUTOS_VIEW.IDPRODUTO=PSV2.IDPRODUTO AND PRODUTOS_VIEW.IDSUBPRODUTO=PSV2.IDSUBPRODUTO AND PSV2.IDEMPRESA=26 AND PSV2.IDLOCALESTOQUE=124 AND PSV2.QTDDISPONIVEL>0
  LEFT JOIN DBA.DIVISAO AS DIVISAO ON
                                       (DIVISAO.IDDIVISAO = PRODUTOS_VIEW.IDDIVISAO)
                LEFT JOIN DBA.SECAO AS SECAO ON
                                        (SECAO.IDSECAO = PRODUTOS_VIEW.IDSECAO)
                LEFT JOIN DBA.GRUPO AS GRUPO ON
                                        (GRUPO.IDGRUPO = PRODUTOS_VIEW.IDGRUPO)
                LEFT JOIN DBA.SUBGRUPO AS SUBGRUPO ON
                                        (SUBGRUPO.IDSUBGRUPO = PRODUTOS_VIEW.IDSUBGRUPO)
                LEFT JOIN DBA.PRODUTO_FORNECEDOR AS PRODUTO_FORNECEDOR ON
                                        (PRODUTO_FORNECEDOR.IDSUBPRODUTO = PRODUTOS_VIEW.IDSUBPRODUTO AND
                                        PRODUTO_FORNECEDOR.IDPRODUTO = PRODUTOS_VIEW.IDPRODUTO AND
                                        PRODUTO_FORNECEDOR.FLAGFORNECEDORPADRAO = 'T')
                LEFT JOIN DBA.CLIENTE_FORNECEDOR AS CLIENTE_FORNECEDOR ON
                                        (CLIENTE_FORNECEDOR.IDCLIFOR = PRODUTO_FORNECEDOR.IDCLIFOR)
                LEFT JOIN DBA.GRUPO_ECONOMICO AS GRUPO_ECONOMICO ON
                                        (GRUPO_ECONOMICO.IDGRUPOECONOMICO = CLIENTE_FORNECEDOR.IDGRUPOECONOMICO)
                LEFT JOIN DBA.POLITICA_PRECO_PRODUTO AS PPP ON
                                        (PPP.IDPRODUTO=PRODUTOS_VIEW.IDPRODUTO AND PPP.IDSUBPRODUTO=PRODUTOS_VIEW.IDSUBPRODUTO AND PPP.IDEMPRESA='{idempresa}')
                LEFT JOIN DBA.PRODUTO_GRADE AS PRODUTO_GRADE ON
                                        (PRODUTO_GRADE.IDPRODUTO=PRODUTOS_VIEW.IDPRODUTO AND PRODUTO_GRADE.IDSUBPRODUTO=PRODUTOS_VIEW.IDSUBPRODUTO)
                where  (PRODUTOS_VIEW.IDPRODUTO IN ({idprodutos}) AND PRODUTOS_VIEW.FLAGINATIVO='F')) AS RESUMO
GROUP BY
        IDSUBPRODUTO,
        VALMULTIVENDAS,
        PERCOMAVISTA,
        NCM,
        PESOBRUTO,
        EMBALAGEMSAIDA,
        LARGURA,
        ALTURA,
        COMPRIMENTO,
        DESCRCOMPRODUTO,
        MODELO,
        FABRICANTE,
        FLAGINATIVOCOMPRA,
        IDDIVISAO,
        DESCRDIVISAO,
        IDSECAO,
        DESCRSECAO,
        IDGRUPO,
        DESCRGRUPO,
        IDSUBGRUPO,
        DESCRSUBGRUPO,
        IDCLIFOR,
        IDGRUPOECONOMICO,
        DESCRGRUPOECONOMICO,
        VALPRECOVAREJO,
        VALPROMVAREJO,
        IDCODBARPROD,
        REFERENCIA,
        DTFIMPROMOCAOVAR ''',conexao)

    cursor.commit()
    cursor.close()

    print(carrega_busca_produtocompleto)
    # pagina = paginas
    # carrega_busca_produtocompleto2 = carrega_busca_produtocompleto.iloc[pagina : (pagina+1)*50, ]
    # print(carrega_busca_produtocompleto2)


    #lista = []
    for index, row in carrega_busca_produtocompleto.iterrows():
        colunas = ["IDSUBPRODUTO", "VALMULTIVENDAS","PERCOMAVISTA","NCM","PESOBRUTO","EMBALAGEMSAIDA","LARGURA","ALTURA","COMPRIMENTO","DESCRCOMPRODUTO","FABRICANTE","VALPRECOVAREJO","VALPROMVAREJO","ESTOQUESEVERO","ESTOQUEGRAVATAI","IDCODBARPROD","REFERENCIA","DTFIMPROMOCAOVAR", "MODELO"]
        dados_produtocompleto_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_produtocompleto_ciss)

        #lista.append(dados_produtocompleto_ciss)
    if dados_produtocompleto_ciss == 1:
            raise HTTPException(status_code=404,detail="Produto não Encontrado") 
    return dados_produtocompleto_ciss
    #{'data':lista} #
    



@app.get("/produtocompletofabricante/{idempresa}&{idfabricante}")
async def produto_busca_fabricante(idempresa:str,
                        idfabricante:str):
    """
    Esta chamada retorna dados apenas o IDSUBPRODUTO de um cadastro filtrando preço por uma empresa e pela descricao do produto.
    Para visualizar o retorno basta colocar PISO PORTINARI NO idfabricante e 1 no idempresa.
    Em cada execução ele uma lista de idsubprodutos.


    """
    logging.info(f"Endpoint '/produtocompletofabricante/{idempresa}&{idfabricante}' acessado com sucesso.")                   
                        # &{fabricantedescricao}&{estoquemaior},fabricantedescricao:str,
                        #estoquemaior:str
                        # paginas): &{paginas}
    #cpfcnpj:Busca_Cliente.cpfcnpj
    #print(idprodutos)
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_produtocompletofabricante =pd.read_sql(f'''SELECT DISTINCT
        CAST(PRODUTOS_VIEW.IDSUBPRODUTO AS VARCHAR(20)) AS IDSUBPRODUTO
       
FROM
        DBA.PRODUTOS_VIEW AS PRODUTOS_VIEW
                LEFT JOIN DBA.DIVISAO AS DIVISAO ON
                                        (DIVISAO.IDDIVISAO = PRODUTOS_VIEW.IDDIVISAO)
                LEFT JOIN DBA.SECAO AS SECAO ON
                                        (SECAO.IDSECAO = PRODUTOS_VIEW.IDSECAO)
                LEFT JOIN DBA.GRUPO AS GRUPO ON
                                        (GRUPO.IDGRUPO = PRODUTOS_VIEW.IDGRUPO)
                LEFT JOIN DBA.SUBGRUPO AS SUBGRUPO ON
                                        (SUBGRUPO.IDSUBGRUPO = PRODUTOS_VIEW.IDSUBGRUPO)
                LEFT JOIN DBA.PRODUTO_FORNECEDOR AS PRODUTO_FORNECEDOR ON
                                        (PRODUTO_FORNECEDOR.IDSUBPRODUTO = PRODUTOS_VIEW.IDSUBPRODUTO AND
                                        PRODUTO_FORNECEDOR.IDPRODUTO = PRODUTOS_VIEW.IDPRODUTO AND
                                        PRODUTO_FORNECEDOR.FLAGFORNECEDORPADRAO = 'T')
                LEFT JOIN DBA.CLIENTE_FORNECEDOR AS CLIENTE_FORNECEDOR ON
                                        (CLIENTE_FORNECEDOR.IDCLIFOR = PRODUTO_FORNECEDOR.IDCLIFOR)
                LEFT JOIN DBA.GRUPO_ECONOMICO AS GRUPO_ECONOMICO ON
                                        (GRUPO_ECONOMICO.IDGRUPOECONOMICO = CLIENTE_FORNECEDOR.IDGRUPOECONOMICO)
                LEFT JOIN DBA.POLITICA_PRECO_PRODUTO AS PPP ON
                                        (PPP.IDPRODUTO=PRODUTOS_VIEW.IDPRODUTO AND PPP.IDSUBPRODUTO=PRODUTOS_VIEW.IDSUBPRODUTO AND PPP.IDEMPRESA='{idempresa}')
                LEFT JOIN DBA.PRODUTO_GRADE AS PRODUTO_GRADE ON
                                        (PRODUTO_GRADE.IDPRODUTO=PRODUTOS_VIEW.IDPRODUTO AND PRODUTO_GRADE.IDSUBPRODUTO=PRODUTOS_VIEW.IDSUBPRODUTO)
                where  (PRODUTOS_VIEW.DESCRCOMPRODUTO LIKE '%{idfabricante}%') AND PRODUTOS_VIEW.FLAGINATIVO='F' ''',conexao)

    cursor.commit()
    cursor.close()

    print(carrega_busca_produtocompletofabricante)
    # pagina = paginas
    # carrega_busca_produtocompleto2 = carrega_busca_produtocompleto.iloc[pagina : (pagina+1)*50, ]
    # print(carrega_busca_produtocompleto2)


    lista = []
    for index, row in carrega_busca_produtocompletofabricante.iterrows():
        colunas = ["IDSUBPRODUTO"]
        dados_produtocompleto_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_produtocompleto_ciss)
        lista.append(dados_produtocompleto_ciss)
    return {'data':lista}


@app.get("/produtocompletoestoque/{idempresa}&{qtdmaior}")
async def produto_busca_estoque(idempresa:str,
                        qtdmaior:str):
    """
    Esta chamada retorna uma lista com dados de cadastro de produto com informações de estoque nos CDs e preço de venda e promoção.
    O filtro de idempresa é para identificar o preço de venda e promoção.
    O filtro de quantidade maior é para retornar somente produtos com quantidade maior que a filtrada nos CDs( a quantidade disponível somada
    dos 2 CDs).
    Exemplo para filtrar:
    idempresa = 1
    qtdmaior = 100

    """
    logging.info(f"Endpoint '/produtocompletoestoque/{idempresa}&{qtdmaior}' acessado com sucesso.")                       
                        # &{fabricantedescricao}&{estoquemaior},fabricantedescricao:str,
                        #estoquemaior:str
                        # paginas): &{paginas}
    #cpfcnpj:Busca_Cliente.cpfcnpj
    #print(idprodutos)
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_produtocompletoestoque =pd.read_sql(f'''SELECT
        IDSUBPRODUTO,
        DESCRCOMPRODUTO,
        PERCOMAVISTA,
        VALPRECOVAREJO,
        VALPROMVAREJO,
        DESCRGRUPO,
        DESCRSUBGRUPO,
        DESCRSECAO,
        CAST(SUM(ESTOQUEGRAVATAI) AS VARCHAR(20)) AS ESTOQUEGRAVATAI,
        CAST(SUM(ESTOQUESEVERO) AS VARCHAR(20)) AS ESTOQUESEVERO
FROM
(SELECT
        PRODUTOS_VIEW.IDSUBPRODUTO,
        CAST(PRODUTOS_VIEW.VALMULTIVENDAS AS VARCHAR(20)) AS VALMULTIVENDAS,
        cAST(PRODUTOS_VIEW.PERCOMAVISTA AS VARCHAR(20)) AS PERCOMAVISTA,
        PRODUTOS_VIEW.NCM,
        CAST(PRODUTOS_VIEW.PESOBRUTO AS VARCHAR(20)) AS PESOBRUTO,
        EMBALAGEMSAIDA,
        CAST(LARGURA AS VARCHAR(20)) AS LARGURA,
        CAST(ALTURA AS VARCHAR(20)) AS ALTURA,
        CAST(COMPRIMENTO AS VARCHAR(20)) AS COMPRIMENTO,
        PRODUTOS_VIEW.DESCRCOMPRODUTO,
        PRODUTOS_VIEW.FABRICANTE,
        PRODUTOS_VIEW.FLAGINATIVOCOMPRA,
        DIVISAO.IDDIVISAO,
        DIVISAO.DESCRDIVISAO,
        SECAO.IDSECAO,
        SECAO.DESCRSECAO,
        GRUPO.IDGRUPO,
        GRUPO.DESCRGRUPO,
        SUBGRUPO.IDSUBGRUPO,
        SUBGRUPO.DESCRSUBGRUPO,
        PRODUTO_FORNECEDOR.IDCLIFOR,
        CLIENTE_FORNECEDOR.IDGRUPOECONOMICO,
        GRUPO_ECONOMICO.DESCRGRUPOECONOMICO,
        CAST(ROUND(PPP.VALPRECOVAREJO,2) AS VARCHAR(20)) AS VALPRECOVAREJO,
        CAST(ROUND((CASE WHEN PPP.DTFIMPROMOCAOVAR >=CURRENT DATE THEN PPP.VALPROMVAREJO ELSE 0 END),2) AS VARCHAR(20)) AS VALPROMVAREJO,
        PRODUTOS_VIEW.IDCODBARPROD,
        PRODUTOS_VIEW.REFERENCIA,
        PPP.DTFIMPROMOCAOVAR,
        0 AS ESTOQUEGRAVATAI,
        PSV1.QTDDISPONIVEL AS ESTOQUESEVERO
FROM
        DBA.PRODUTOS_VIEW AS PRODUTOS_VIEW
LEFT JOIN DBA.PRODUTOS_SALDOS_VIEW AS PSV1
 ON PRODUTOS_VIEW.IDPRODUTO=PSV1.IDPRODUTO AND PRODUTOS_VIEW.IDSUBPRODUTO=PSV1.IDSUBPRODUTO AND PSV1.IDEMPRESA=13 AND PSV1.IDLOCALESTOQUE=6 AND PSV1.QTDDISPONIVEL>0
                LEFT JOIN DBA.DIVISAO AS DIVISAO ON
                                        (DIVISAO.IDDIVISAO = PRODUTOS_VIEW.IDDIVISAO)
                LEFT JOIN DBA.SECAO AS SECAO ON
                                        (SECAO.IDSECAO = PRODUTOS_VIEW.IDSECAO)
                LEFT JOIN DBA.GRUPO AS GRUPO ON
                                        (GRUPO.IDGRUPO = PRODUTOS_VIEW.IDGRUPO)
                LEFT JOIN DBA.SUBGRUPO AS SUBGRUPO ON
                                        (SUBGRUPO.IDSUBGRUPO = PRODUTOS_VIEW.IDSUBGRUPO)
                LEFT JOIN DBA.PRODUTO_FORNECEDOR AS PRODUTO_FORNECEDOR ON
                                        (PRODUTO_FORNECEDOR.IDSUBPRODUTO = PRODUTOS_VIEW.IDSUBPRODUTO AND
                                        PRODUTO_FORNECEDOR.IDPRODUTO = PRODUTOS_VIEW.IDPRODUTO AND
                                        PRODUTO_FORNECEDOR.FLAGFORNECEDORPADRAO = 'T')
                LEFT JOIN DBA.CLIENTE_FORNECEDOR AS CLIENTE_FORNECEDOR ON
                                        (CLIENTE_FORNECEDOR.IDCLIFOR = PRODUTO_FORNECEDOR.IDCLIFOR)
                LEFT JOIN DBA.GRUPO_ECONOMICO AS GRUPO_ECONOMICO ON
                                        (GRUPO_ECONOMICO.IDGRUPOECONOMICO = CLIENTE_FORNECEDOR.IDGRUPOECONOMICO)
                LEFT JOIN DBA.POLITICA_PRECO_PRODUTO AS PPP ON
                                        (PPP.IDPRODUTO=PRODUTOS_VIEW.IDPRODUTO AND PPP.IDSUBPRODUTO=PRODUTOS_VIEW.IDSUBPRODUTO AND PPP.IDEMPRESA='{idempresa}')
                LEFT JOIN DBA.PRODUTO_GRADE AS PRODUTO_GRADE ON
                                        (PRODUTO_GRADE.IDPRODUTO=PRODUTOS_VIEW.IDPRODUTO AND PRODUTO_GRADE.IDSUBPRODUTO=PRODUTOS_VIEW.IDSUBPRODUTO)
                where  PSV1.QTDDISPONIVEL > {qtdmaior} AND PRODUTOS_VIEW.FLAGINATIVO='F'


        UNION ALL

        SELECT

        PRODUTOS_VIEW.IDSUBPRODUTO,
        CAST(PRODUTOS_VIEW.VALMULTIVENDAS AS VARCHAR(20)) AS VALMULTIVENDAS,
        cAST(PRODUTOS_VIEW.PERCOMAVISTA AS VARCHAR(20)) AS PERCOMAVISTA,
        PRODUTOS_VIEW.NCM,
        CAST(PRODUTOS_VIEW.PESOBRUTO AS VARCHAR(20)) AS PESOBRUTO,
        EMBALAGEMSAIDA,
        CAST(LARGURA AS VARCHAR(20)) AS LARGURA,
        CAST(ALTURA AS VARCHAR(20)) AS ALTURA,
        CAST(COMPRIMENTO AS VARCHAR(20)) AS COMPRIMENTO,
        PRODUTOS_VIEW.DESCRCOMPRODUTO,
        PRODUTOS_VIEW.FABRICANTE,
        PRODUTOS_VIEW.FLAGINATIVOCOMPRA,
        DIVISAO.IDDIVISAO,
        DIVISAO.DESCRDIVISAO,
        SECAO.IDSECAO,
        SECAO.DESCRSECAO,
        GRUPO.IDGRUPO,
        GRUPO.DESCRGRUPO,
        SUBGRUPO.IDSUBGRUPO,
        SUBGRUPO.DESCRSUBGRUPO,
        PRODUTO_FORNECEDOR.IDCLIFOR,
        CLIENTE_FORNECEDOR.IDGRUPOECONOMICO,
        GRUPO_ECONOMICO.DESCRGRUPOECONOMICO,
        CAST(ROUND(PPP.VALPRECOVAREJO,2) AS VARCHAR(20)) AS VALPRECOVAREJO,
        CAST(ROUND((CASE WHEN PPP.DTFIMPROMOCAOVAR >=CURRENT DATE THEN PPP.VALPROMVAREJO ELSE 0 END),2) AS VARCHAR(20)) AS VALPROMVAREJO,
        PRODUTOS_VIEW.IDCODBARPROD,
        PRODUTOS_VIEW.REFERENCIA,
        PPP.DTFIMPROMOCAOVAR,
        PSV2.QTDDISPONIVEL  AS ESTOQUEGRAVATAI,
        0 AS ESTOQUESEVERO
FROM
        DBA.PRODUTOS_VIEW AS PRODUTOS_VIEW
LEFT JOIN DBA.PRODUTOS_SALDOS_VIEW AS PSV2
 ON PRODUTOS_VIEW.IDPRODUTO=PSV2.IDPRODUTO AND PRODUTOS_VIEW.IDSUBPRODUTO=PSV2.IDSUBPRODUTO AND PSV2.IDEMPRESA=26 AND PSV2.IDLOCALESTOQUE=124 AND PSV2.QTDDISPONIVEL>0
  LEFT JOIN DBA.DIVISAO AS DIVISAO ON
                                       (DIVISAO.IDDIVISAO = PRODUTOS_VIEW.IDDIVISAO)
                LEFT JOIN DBA.SECAO AS SECAO ON
                                        (SECAO.IDSECAO = PRODUTOS_VIEW.IDSECAO)
                LEFT JOIN DBA.GRUPO AS GRUPO ON
                                        (GRUPO.IDGRUPO = PRODUTOS_VIEW.IDGRUPO)
                LEFT JOIN DBA.SUBGRUPO AS SUBGRUPO ON
                                        (SUBGRUPO.IDSUBGRUPO = PRODUTOS_VIEW.IDSUBGRUPO)
                LEFT JOIN DBA.PRODUTO_FORNECEDOR AS PRODUTO_FORNECEDOR ON
                                        (PRODUTO_FORNECEDOR.IDSUBPRODUTO = PRODUTOS_VIEW.IDSUBPRODUTO AND
                                        PRODUTO_FORNECEDOR.IDPRODUTO = PRODUTOS_VIEW.IDPRODUTO AND
                                        PRODUTO_FORNECEDOR.FLAGFORNECEDORPADRAO = 'T')
                LEFT JOIN DBA.CLIENTE_FORNECEDOR AS CLIENTE_FORNECEDOR ON
                                        (CLIENTE_FORNECEDOR.IDCLIFOR = PRODUTO_FORNECEDOR.IDCLIFOR)
                LEFT JOIN DBA.GRUPO_ECONOMICO AS GRUPO_ECONOMICO ON
                                        (GRUPO_ECONOMICO.IDGRUPOECONOMICO = CLIENTE_FORNECEDOR.IDGRUPOECONOMICO)
                LEFT JOIN DBA.POLITICA_PRECO_PRODUTO AS PPP ON
                                        (PPP.IDPRODUTO=PRODUTOS_VIEW.IDPRODUTO AND PPP.IDSUBPRODUTO=PRODUTOS_VIEW.IDSUBPRODUTO AND PPP.IDEMPRESA='{idempresa}')
                LEFT JOIN DBA.PRODUTO_GRADE AS PRODUTO_GRADE ON
                                        (PRODUTO_GRADE.IDPRODUTO=PRODUTOS_VIEW.IDPRODUTO AND PRODUTO_GRADE.IDSUBPRODUTO=PRODUTOS_VIEW.IDSUBPRODUTO)
                where  PSV2.QTDDISPONIVEL > {qtdmaior} AND PRODUTOS_VIEW.FLAGINATIVO='F'




) AS RESUMO
GROUP BY
        IDSUBPRODUTO,
        DESCRCOMPRODUTO,
        PERCOMAVISTA,
        VALPRECOVAREJO,
        VALPROMVAREJO,
        DESCRGRUPO,
        DESCRSUBGRUPO,
        DESCRSECAO ''',conexao)

    cursor.commit()
    cursor.close()

    print(carrega_busca_produtocompletoestoque)
    # pagina = paginas
    # carrega_busca_produtocompleto2 = carrega_busca_produtocompleto.iloc[pagina : (pagina+1)*50, ]
    # print(carrega_busca_produtocompleto2)


    lista = []
    for index, row in carrega_busca_produtocompletoestoque.iterrows():
        colunas = ["IDSUBPRODUTO"]
        dados_produtocompleto_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_produtocompleto_ciss)
        lista.append(dados_produtocompleto_ciss)
    return {'data':lista}


@app.get("/produtocompletoestoquedescricao/{idempresa}&{fabricante}&{qtdmaior}")
async def produto_busca_estoque(idempresa:str,
                                fabricante:str,
                                qtdmaior:float):
    """
    Esta chamada retorna uma lista com dados de cadastro de produto com informações de estoque nos CDs e preço de venda e promoção.
    O filtro de idempresa é para identificar o preço de venda e promoção.
    O filtro de quantidade maior é para retornar somente produtos com quantidade maior que a filtrada nos CDs( a quantidade disponível somada
    dos 2 CDs).
    O filtro fabricante é para filtrar a descricao do produto
    Exemplo para filtrar:
    idempresa = 1
    fabricante = PISO PORTINARI
    qtdmaior = 100 ( se for passada a quantidade -9999 ele retorna qualquer quantidade)

    """
    logging.info(f"Endpoint '/produtocompletoestoquedescricao/{idempresa}&{fabricante}&{qtdmaior}' acessado com sucesso.")                     
                        # &{fabricantedescricao}&{estoquemaior},fabricantedescricao:str,
                        #estoquemaior:str
                        # paginas): &{paginas}
    #cpfcnpj:Busca_Cliente.cpfcnpj
    #print(idprodutos)
    print({qtdmaior})
    print({fabricante})
    fabricante = fabricante.replace(' ','%')
    print(fabricante)
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_produtocompletoestoquedescricao =pd.read_sql(f'''SELECT
        IDSUBPRODUTO,
        DESCRCOMPRODUTO,
        PERCOMAVISTA,
        VALPRECOVAREJO,
        VALPROMVAREJO,
        DESCRGRUPO,
        DESCRSUBGRUPO,
        DESCRSECAO,
        EMBALAGEMSAIDA,
        DATAFINALPROMOCAO,
        CAST(MULTIPLOVENDA AS VARCHAR(20)) AS MULTIPLOVENDA,
        CAST(SUM(ESTOQUEGRAVATAI) AS VARCHAR(20)) AS ESTOQUEGRAVATAI,
        CAST(SUM(ESTOQUESEVERO) AS VARCHAR(20)) AS ESTOQUESEVERO
FROM
(SELECT
        PRODUTOS_VIEW.IDSUBPRODUTO,
        CAST(PRODUTOS_VIEW.VALMULTIVENDAS AS VARCHAR(20)) AS VALMULTIVENDAS,
        cAST(PRODUTOS_VIEW.PERCOMAVISTA AS VARCHAR(20)) AS PERCOMAVISTA,
        PRODUTOS_VIEW.NCM,
        CAST(PRODUTOS_VIEW.PESOBRUTO AS VARCHAR(20)) AS PESOBRUTO,
        EMBALAGEMSAIDA,
        CAST(LARGURA AS VARCHAR(20)) AS LARGURA,
        CAST(ALTURA AS VARCHAR(20)) AS ALTURA,
        CAST(COMPRIMENTO AS VARCHAR(20)) AS COMPRIMENTO,
        PRODUTOS_VIEW.DESCRCOMPRODUTO,
        PRODUTOS_VIEW.FABRICANTE,
        PRODUTOS_VIEW.FLAGINATIVOCOMPRA,
        DIVISAO.IDDIVISAO,
        DIVISAO.DESCRDIVISAO,
        SECAO.IDSECAO,
        SECAO.DESCRSECAO,
        GRUPO.IDGRUPO,
        GRUPO.DESCRGRUPO,
        SUBGRUPO.IDSUBGRUPO,
        SUBGRUPO.DESCRSUBGRUPO,
        PRODUTO_FORNECEDOR.IDCLIFOR,
        CLIENTE_FORNECEDOR.IDGRUPOECONOMICO,
        GRUPO_ECONOMICO.DESCRGRUPOECONOMICO,
        CAST(ROUND(PPP.VALPRECOVAREJO,2) AS VARCHAR(20)) AS VALPRECOVAREJO,
        CAST(ROUND((CASE WHEN PPP.DTFIMPROMOCAOVAR >=CURRENT DATE THEN PPP.VALPROMVAREJO ELSE 0 END),2) AS VARCHAR(20)) AS VALPROMVAREJO,
        PRODUTOS_VIEW.IDCODBARPROD,
        PRODUTOS_VIEW.REFERENCIA,
        PPP.DTFIMPROMOCAOVAR,
        0 AS ESTOQUEGRAVATAI,
        PSV1.QTDDISPONIVEL AS ESTOQUESEVERO,
        PPP.DTFIMPROMOCAOVAR AS DATAFINALPROMOCAO,
        PRODUTO_GRADE.VALMULTIVENDAS AS MULTIPLOVENDA
FROM
        DBA.PRODUTOS_VIEW AS PRODUTOS_VIEW
LEFT JOIN DBA.PRODUTOS_SALDOS_VIEW AS PSV1
 ON PRODUTOS_VIEW.IDPRODUTO=PSV1.IDPRODUTO AND PRODUTOS_VIEW.IDSUBPRODUTO=PSV1.IDSUBPRODUTO AND PSV1.IDEMPRESA=13 AND PSV1.IDLOCALESTOQUE=6 AND PSV1.QTDDISPONIVEL>0
                LEFT JOIN DBA.DIVISAO AS DIVISAO ON
                                        (DIVISAO.IDDIVISAO = PRODUTOS_VIEW.IDDIVISAO)
                LEFT JOIN DBA.SECAO AS SECAO ON
                                        (SECAO.IDSECAO = PRODUTOS_VIEW.IDSECAO)
                LEFT JOIN DBA.GRUPO AS GRUPO ON
                                        (GRUPO.IDGRUPO = PRODUTOS_VIEW.IDGRUPO)
                LEFT JOIN DBA.SUBGRUPO AS SUBGRUPO ON
                                        (SUBGRUPO.IDSUBGRUPO = PRODUTOS_VIEW.IDSUBGRUPO)
                LEFT JOIN DBA.PRODUTO_FORNECEDOR AS PRODUTO_FORNECEDOR ON
                                        (PRODUTO_FORNECEDOR.IDSUBPRODUTO = PRODUTOS_VIEW.IDSUBPRODUTO AND
                                        PRODUTO_FORNECEDOR.IDPRODUTO = PRODUTOS_VIEW.IDPRODUTO AND
                                        PRODUTO_FORNECEDOR.FLAGFORNECEDORPADRAO = 'T')
                LEFT JOIN DBA.CLIENTE_FORNECEDOR AS CLIENTE_FORNECEDOR ON
                                        (CLIENTE_FORNECEDOR.IDCLIFOR = PRODUTO_FORNECEDOR.IDCLIFOR)
                LEFT JOIN DBA.GRUPO_ECONOMICO AS GRUPO_ECONOMICO ON
                                        (GRUPO_ECONOMICO.IDGRUPOECONOMICO = CLIENTE_FORNECEDOR.IDGRUPOECONOMICO)
                LEFT JOIN DBA.POLITICA_PRECO_PRODUTO AS PPP ON
                                        (PPP.IDPRODUTO=PRODUTOS_VIEW.IDPRODUTO AND PPP.IDSUBPRODUTO=PRODUTOS_VIEW.IDSUBPRODUTO AND PPP.IDEMPRESA='{idempresa}')
                LEFT JOIN DBA.PRODUTO_GRADE AS PRODUTO_GRADE ON
                                        (PRODUTO_GRADE.IDPRODUTO=PRODUTOS_VIEW.IDPRODUTO AND PRODUTO_GRADE.IDSUBPRODUTO=PRODUTOS_VIEW.IDSUBPRODUTO)
                where  (PRODUTOS_VIEW.DESCRCOMPRODUTO LIKE '%{fabricante}%') AND
                        (PSV1.QTDDISPONIVEL >= {qtdmaior}  OR {qtdmaior} = -99999)
                        AND PRODUTOS_VIEW.FLAGINATIVO='F'


        UNION ALL

        SELECT

        PRODUTOS_VIEW.IDSUBPRODUTO,
        CAST(PRODUTOS_VIEW.VALMULTIVENDAS AS VARCHAR(20)) AS VALMULTIVENDAS,
        cAST(PRODUTOS_VIEW.PERCOMAVISTA AS VARCHAR(20)) AS PERCOMAVISTA,
        PRODUTOS_VIEW.NCM,
        CAST(PRODUTOS_VIEW.PESOBRUTO AS VARCHAR(20)) AS PESOBRUTO,
        EMBALAGEMSAIDA,
        CAST(LARGURA AS VARCHAR(20)) AS LARGURA,
        CAST(ALTURA AS VARCHAR(20)) AS ALTURA,
        CAST(COMPRIMENTO AS VARCHAR(20)) AS COMPRIMENTO,
        PRODUTOS_VIEW.DESCRCOMPRODUTO,
        PRODUTOS_VIEW.FABRICANTE,
        PRODUTOS_VIEW.FLAGINATIVOCOMPRA,
        DIVISAO.IDDIVISAO,
        DIVISAO.DESCRDIVISAO,
        SECAO.IDSECAO,
        SECAO.DESCRSECAO,
        GRUPO.IDGRUPO,
        GRUPO.DESCRGRUPO,
        SUBGRUPO.IDSUBGRUPO,
        SUBGRUPO.DESCRSUBGRUPO,
        PRODUTO_FORNECEDOR.IDCLIFOR,
        CLIENTE_FORNECEDOR.IDGRUPOECONOMICO,
        GRUPO_ECONOMICO.DESCRGRUPOECONOMICO,
        CAST(ROUND(PPP.VALPRECOVAREJO,2) AS VARCHAR(20)) AS VALPRECOVAREJO,
        CAST(ROUND((CASE WHEN PPP.DTFIMPROMOCAOVAR >=CURRENT DATE THEN PPP.VALPROMVAREJO ELSE 0 END),2) AS VARCHAR(20)) AS VALPROMVAREJO,
        PRODUTOS_VIEW.IDCODBARPROD,
        PRODUTOS_VIEW.REFERENCIA,
        PPP.DTFIMPROMOCAOVAR,
        PSV2.QTDDISPONIVEL  AS ESTOQUEGRAVATAI,
        0 AS ESTOQUESEVERO,
        PPP.DTFIMPROMOCAOVAR AS DATAFINALPROMOCAO,
        PRODUTO_GRADE.VALMULTIVENDAS AS MULTIPLOVENDA
FROM
        DBA.PRODUTOS_VIEW AS PRODUTOS_VIEW
LEFT JOIN DBA.PRODUTOS_SALDOS_VIEW AS PSV2
 ON PRODUTOS_VIEW.IDPRODUTO=PSV2.IDPRODUTO AND PRODUTOS_VIEW.IDSUBPRODUTO=PSV2.IDSUBPRODUTO AND PSV2.IDEMPRESA=26 AND PSV2.IDLOCALESTOQUE=124 AND PSV2.QTDDISPONIVEL>0
  LEFT JOIN DBA.DIVISAO AS DIVISAO ON
                                       (DIVISAO.IDDIVISAO = PRODUTOS_VIEW.IDDIVISAO)
                LEFT JOIN DBA.SECAO AS SECAO ON
                                        (SECAO.IDSECAO = PRODUTOS_VIEW.IDSECAO)
                LEFT JOIN DBA.GRUPO AS GRUPO ON
                                        (GRUPO.IDGRUPO = PRODUTOS_VIEW.IDGRUPO)
                LEFT JOIN DBA.SUBGRUPO AS SUBGRUPO ON
                                        (SUBGRUPO.IDSUBGRUPO = PRODUTOS_VIEW.IDSUBGRUPO)
                LEFT JOIN DBA.PRODUTO_FORNECEDOR AS PRODUTO_FORNECEDOR ON
                                        (PRODUTO_FORNECEDOR.IDSUBPRODUTO = PRODUTOS_VIEW.IDSUBPRODUTO AND
                                        PRODUTO_FORNECEDOR.IDPRODUTO = PRODUTOS_VIEW.IDPRODUTO AND
                                        PRODUTO_FORNECEDOR.FLAGFORNECEDORPADRAO = 'T')
                LEFT JOIN DBA.CLIENTE_FORNECEDOR AS CLIENTE_FORNECEDOR ON
                                        (CLIENTE_FORNECEDOR.IDCLIFOR = PRODUTO_FORNECEDOR.IDCLIFOR)
                LEFT JOIN DBA.GRUPO_ECONOMICO AS GRUPO_ECONOMICO ON
                                        (GRUPO_ECONOMICO.IDGRUPOECONOMICO = CLIENTE_FORNECEDOR.IDGRUPOECONOMICO)
                LEFT JOIN DBA.POLITICA_PRECO_PRODUTO AS PPP ON
                                        (PPP.IDPRODUTO=PRODUTOS_VIEW.IDPRODUTO AND PPP.IDSUBPRODUTO=PRODUTOS_VIEW.IDSUBPRODUTO AND PPP.IDEMPRESA='{idempresa}')
                LEFT JOIN DBA.PRODUTO_GRADE AS PRODUTO_GRADE ON
                                        (PRODUTO_GRADE.IDPRODUTO=PRODUTOS_VIEW.IDPRODUTO AND PRODUTO_GRADE.IDSUBPRODUTO=PRODUTOS_VIEW.IDSUBPRODUTO)
                where  (PRODUTOS_VIEW.DESCRCOMPRODUTO LIKE '%{fabricante}%') and 
                        (PSV2.QTDDISPONIVEL >= {qtdmaior}  OR {qtdmaior}  = -99999)
                        AND PRODUTOS_VIEW.FLAGINATIVO='F'
) AS RESUMO
GROUP BY
        IDSUBPRODUTO,
        DESCRCOMPRODUTO,
        PERCOMAVISTA,
        VALPRECOVAREJO,
        VALPROMVAREJO,
        DESCRGRUPO,
        DESCRSUBGRUPO,
        DESCRSECAO,
        DATAFINALPROMOCAO,
        MULTIPLOVENDA,
        EMBALAGEMSAIDA ''',conexao)

    cursor.commit()
    cursor.close()

    print(carrega_busca_produtocompletoestoquedescricao)
    # pagina = paginas
    # carrega_busca_produtocompleto2 = carrega_busca_produtocompleto.iloc[pagina : (pagina+1)*50, ]
    # print(carrega_busca_produtocompleto2)


    lista = []
    for index, row in carrega_busca_produtocompletoestoquedescricao.iterrows():
        colunas = ["IDSUBPRODUTO", "DESCRCOMPRODUTO" , "PERCOMAVISTA", "VALPRECOVAREJO", "VALPROMVAREJO", "DESCRGRUPO", "DESCRSUBGRUPO", "DESCRSECAO", "ESTOQUEGRAVATAI", "ESTOQUESEVERO", "EMBALAGEMSAIDA", "DATAFINALPROMOCAO" ]
        dados_produtocompleto_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_produtocompleto_ciss)
        lista.append(dados_produtocompleto_ciss)
    if lista == []:
            raise HTTPException(status_code=404,detail="--- Produto nao Encontrado -- Sem Cadastro -- Quantidade maior que a disponivel para estes produtos -- Tente Modificar a Descricao de Busca--") 
    #return dados_produtocompleto_ciss
    return {'data':lista}


@app.get("/produtocompletoreferencia/{idempresa}&{fabricante}")
async def produto_busca_estoque(idempresa:str,
                                fabricante:str):
    """
    Esta chamada retorna uma lista com dados de cadastro de produto com informações de estoque nos CDs e preço de venda e promoção.
    O filtro de idempresa é para identificar o preço de venda e promoção.
    O filtro fabricante é para filtrar a referencia do produto
    Exemplo para filtrar:
    idempresa = 1
    fabricante = 1990 ( referencia da Deca - chuveiros)

    """
    logging.info(f"Endpoint '/produtocompletoreferencia/{idempresa}&{fabricante}' acessado com sucesso.")                     
                        # &{fabricantedescricao}&{estoquemaior},fabricantedescricao:str,
                        #estoquemaior:str
                        # paginas): &{paginas}
    #cpfcnpj:Busca_Cliente.cpfcnpj
    #print(idprodutos)
    print({fabricante})
    fabricante = fabricante.replace(' ','%')
    print(fabricante)
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_produtocompletoestoquedescricao =pd.read_sql(f'''SELECT
        IDSUBPRODUTO,
        DESCRCOMPRODUTO,
        PERCOMAVISTA,
        VALPRECOVAREJO,
        VALPROMVAREJO,
        DESCRGRUPO,
        DESCRSUBGRUPO,
        DESCRSECAO,
        EMBALAGEMSAIDA,
        DATAFINALPROMOCAO,
        CAST(MULTIPLOVENDA AS VARCHAR(20)) AS MULTIPLOVENDA,
        CAST(SUM(ESTOQUEGRAVATAI) AS VARCHAR(20)) AS ESTOQUEGRAVATAI,
        CAST(SUM(ESTOQUESEVERO) AS VARCHAR(20)) AS ESTOQUESEVERO
FROM
(SELECT
        PRODUTOS_VIEW.IDSUBPRODUTO,
        CAST(PRODUTOS_VIEW.VALMULTIVENDAS AS VARCHAR(20)) AS VALMULTIVENDAS,
        cAST(PRODUTOS_VIEW.PERCOMAVISTA AS VARCHAR(20)) AS PERCOMAVISTA,
        PRODUTOS_VIEW.NCM,
        CAST(PRODUTOS_VIEW.PESOBRUTO AS VARCHAR(20)) AS PESOBRUTO,
        EMBALAGEMSAIDA,
        CAST(LARGURA AS VARCHAR(20)) AS LARGURA,
        CAST(ALTURA AS VARCHAR(20)) AS ALTURA,
        CAST(COMPRIMENTO AS VARCHAR(20)) AS COMPRIMENTO,
        PRODUTOS_VIEW.DESCRCOMPRODUTO,
        PRODUTOS_VIEW.FABRICANTE,
        PRODUTOS_VIEW.FLAGINATIVOCOMPRA,
        DIVISAO.IDDIVISAO,
        DIVISAO.DESCRDIVISAO,
        SECAO.IDSECAO,
        SECAO.DESCRSECAO,
        GRUPO.IDGRUPO,
        GRUPO.DESCRGRUPO,
        SUBGRUPO.IDSUBGRUPO,
        SUBGRUPO.DESCRSUBGRUPO,
        PRODUTO_FORNECEDOR.IDCLIFOR,
        CLIENTE_FORNECEDOR.IDGRUPOECONOMICO,
        GRUPO_ECONOMICO.DESCRGRUPOECONOMICO,
        CAST(ROUND(PPP.VALPRECOVAREJO,2) AS VARCHAR(20)) AS VALPRECOVAREJO,
        CAST(ROUND((CASE WHEN PPP.DTFIMPROMOCAOVAR >=CURRENT DATE THEN PPP.VALPROMVAREJO ELSE 0 END),2) AS VARCHAR(20)) AS VALPROMVAREJO,
        PRODUTOS_VIEW.IDCODBARPROD,
        PRODUTOS_VIEW.REFERENCIA,
        PPP.DTFIMPROMOCAOVAR,
        0 AS ESTOQUEGRAVATAI,
        PSV1.QTDDISPONIVEL AS ESTOQUESEVERO,
        PPP.DTFIMPROMOCAOVAR AS DATAFINALPROMOCAO,
        PRODUTO_GRADE.VALMULTIVENDAS AS MULTIPLOVENDA
FROM
        DBA.PRODUTOS_VIEW AS PRODUTOS_VIEW
LEFT JOIN DBA.PRODUTOS_SALDOS_VIEW AS PSV1
 ON PRODUTOS_VIEW.IDPRODUTO=PSV1.IDPRODUTO AND PRODUTOS_VIEW.IDSUBPRODUTO=PSV1.IDSUBPRODUTO AND PSV1.IDEMPRESA=13 AND PSV1.IDLOCALESTOQUE=6 AND PSV1.QTDDISPONIVEL>0
                LEFT JOIN DBA.DIVISAO AS DIVISAO ON
                                        (DIVISAO.IDDIVISAO = PRODUTOS_VIEW.IDDIVISAO)
                LEFT JOIN DBA.SECAO AS SECAO ON
                                        (SECAO.IDSECAO = PRODUTOS_VIEW.IDSECAO)
                LEFT JOIN DBA.GRUPO AS GRUPO ON
                                        (GRUPO.IDGRUPO = PRODUTOS_VIEW.IDGRUPO)
                LEFT JOIN DBA.SUBGRUPO AS SUBGRUPO ON
                                        (SUBGRUPO.IDSUBGRUPO = PRODUTOS_VIEW.IDSUBGRUPO)
                LEFT JOIN DBA.PRODUTO_FORNECEDOR AS PRODUTO_FORNECEDOR ON
                                        (PRODUTO_FORNECEDOR.IDSUBPRODUTO = PRODUTOS_VIEW.IDSUBPRODUTO AND
                                        PRODUTO_FORNECEDOR.IDPRODUTO = PRODUTOS_VIEW.IDPRODUTO AND
                                        PRODUTO_FORNECEDOR.FLAGFORNECEDORPADRAO = 'T')
                LEFT JOIN DBA.CLIENTE_FORNECEDOR AS CLIENTE_FORNECEDOR ON
                                        (CLIENTE_FORNECEDOR.IDCLIFOR = PRODUTO_FORNECEDOR.IDCLIFOR)
                LEFT JOIN DBA.GRUPO_ECONOMICO AS GRUPO_ECONOMICO ON
                                        (GRUPO_ECONOMICO.IDGRUPOECONOMICO = CLIENTE_FORNECEDOR.IDGRUPOECONOMICO)
                LEFT JOIN DBA.POLITICA_PRECO_PRODUTO AS PPP ON
                                        (PPP.IDPRODUTO=PRODUTOS_VIEW.IDPRODUTO AND PPP.IDSUBPRODUTO=PRODUTOS_VIEW.IDSUBPRODUTO AND PPP.IDEMPRESA='{idempresa}')
                LEFT JOIN DBA.PRODUTO_GRADE AS PRODUTO_GRADE ON
                                        (PRODUTO_GRADE.IDPRODUTO=PRODUTOS_VIEW.IDPRODUTO AND PRODUTO_GRADE.IDSUBPRODUTO=PRODUTOS_VIEW.IDSUBPRODUTO)
                where  (PRODUTOS_VIEW.REFERENCIA LIKE '%{fabricante}%')
                        AND PRODUTOS_VIEW.FLAGINATIVO='F'


        UNION ALL

        SELECT

        PRODUTOS_VIEW.IDSUBPRODUTO,
        CAST(PRODUTOS_VIEW.VALMULTIVENDAS AS VARCHAR(20)) AS VALMULTIVENDAS,
        cAST(PRODUTOS_VIEW.PERCOMAVISTA AS VARCHAR(20)) AS PERCOMAVISTA,
        PRODUTOS_VIEW.NCM,
        CAST(PRODUTOS_VIEW.PESOBRUTO AS VARCHAR(20)) AS PESOBRUTO,
        EMBALAGEMSAIDA,
        CAST(LARGURA AS VARCHAR(20)) AS LARGURA,
        CAST(ALTURA AS VARCHAR(20)) AS ALTURA,
        CAST(COMPRIMENTO AS VARCHAR(20)) AS COMPRIMENTO,
        PRODUTOS_VIEW.DESCRCOMPRODUTO,
        PRODUTOS_VIEW.FABRICANTE,
        PRODUTOS_VIEW.FLAGINATIVOCOMPRA,
        DIVISAO.IDDIVISAO,
        DIVISAO.DESCRDIVISAO,
        SECAO.IDSECAO,
        SECAO.DESCRSECAO,
        GRUPO.IDGRUPO,
        GRUPO.DESCRGRUPO,
        SUBGRUPO.IDSUBGRUPO,
        SUBGRUPO.DESCRSUBGRUPO,
        PRODUTO_FORNECEDOR.IDCLIFOR,
        CLIENTE_FORNECEDOR.IDGRUPOECONOMICO,
        GRUPO_ECONOMICO.DESCRGRUPOECONOMICO,
        CAST(ROUND(PPP.VALPRECOVAREJO,2) AS VARCHAR(20)) AS VALPRECOVAREJO,
        CAST(ROUND((CASE WHEN PPP.DTFIMPROMOCAOVAR >=CURRENT DATE THEN PPP.VALPROMVAREJO ELSE 0 END),2) AS VARCHAR(20)) AS VALPROMVAREJO,
        PRODUTOS_VIEW.IDCODBARPROD,
        PRODUTOS_VIEW.REFERENCIA,
        PPP.DTFIMPROMOCAOVAR,
        PSV2.QTDDISPONIVEL  AS ESTOQUEGRAVATAI,
        0 AS ESTOQUESEVERO,
        PPP.DTFIMPROMOCAOVAR AS DATAFINALPROMOCAO,
        PRODUTO_GRADE.VALMULTIVENDAS AS MULTIPLOVENDA
FROM
        DBA.PRODUTOS_VIEW AS PRODUTOS_VIEW
LEFT JOIN DBA.PRODUTOS_SALDOS_VIEW AS PSV2
 ON PRODUTOS_VIEW.IDPRODUTO=PSV2.IDPRODUTO AND PRODUTOS_VIEW.IDSUBPRODUTO=PSV2.IDSUBPRODUTO AND PSV2.IDEMPRESA=26 AND PSV2.IDLOCALESTOQUE=124 AND PSV2.QTDDISPONIVEL>0
  LEFT JOIN DBA.DIVISAO AS DIVISAO ON
                                       (DIVISAO.IDDIVISAO = PRODUTOS_VIEW.IDDIVISAO)
                LEFT JOIN DBA.SECAO AS SECAO ON
                                        (SECAO.IDSECAO = PRODUTOS_VIEW.IDSECAO)
                LEFT JOIN DBA.GRUPO AS GRUPO ON
                                        (GRUPO.IDGRUPO = PRODUTOS_VIEW.IDGRUPO)
                LEFT JOIN DBA.SUBGRUPO AS SUBGRUPO ON
                                        (SUBGRUPO.IDSUBGRUPO = PRODUTOS_VIEW.IDSUBGRUPO)
                LEFT JOIN DBA.PRODUTO_FORNECEDOR AS PRODUTO_FORNECEDOR ON
                                        (PRODUTO_FORNECEDOR.IDSUBPRODUTO = PRODUTOS_VIEW.IDSUBPRODUTO AND
                                        PRODUTO_FORNECEDOR.IDPRODUTO = PRODUTOS_VIEW.IDPRODUTO AND
                                        PRODUTO_FORNECEDOR.FLAGFORNECEDORPADRAO = 'T')
                LEFT JOIN DBA.CLIENTE_FORNECEDOR AS CLIENTE_FORNECEDOR ON
                                        (CLIENTE_FORNECEDOR.IDCLIFOR = PRODUTO_FORNECEDOR.IDCLIFOR)
                LEFT JOIN DBA.GRUPO_ECONOMICO AS GRUPO_ECONOMICO ON
                                        (GRUPO_ECONOMICO.IDGRUPOECONOMICO = CLIENTE_FORNECEDOR.IDGRUPOECONOMICO)
                LEFT JOIN DBA.POLITICA_PRECO_PRODUTO AS PPP ON
                                        (PPP.IDPRODUTO=PRODUTOS_VIEW.IDPRODUTO AND PPP.IDSUBPRODUTO=PRODUTOS_VIEW.IDSUBPRODUTO AND PPP.IDEMPRESA='{idempresa}')
                LEFT JOIN DBA.PRODUTO_GRADE AS PRODUTO_GRADE ON
                                        (PRODUTO_GRADE.IDPRODUTO=PRODUTOS_VIEW.IDPRODUTO AND PRODUTO_GRADE.IDSUBPRODUTO=PRODUTOS_VIEW.IDSUBPRODUTO)
                where  (PRODUTOS_VIEW.REFERENCIA LIKE '%{fabricante}%')
                        AND PRODUTOS_VIEW.FLAGINATIVO='F'
) AS RESUMO
GROUP BY
        IDSUBPRODUTO,
        DESCRCOMPRODUTO,
        PERCOMAVISTA,
        VALPRECOVAREJO,
        VALPROMVAREJO,
        DESCRGRUPO,
        DESCRSUBGRUPO,
        DESCRSECAO,
        DATAFINALPROMOCAO,
        MULTIPLOVENDA,
        EMBALAGEMSAIDA ''',conexao)

    cursor.commit()
    cursor.close()

    print(carrega_busca_produtocompletoestoquedescricao)
    # pagina = paginas
    # carrega_busca_produtocompleto2 = carrega_busca_produtocompleto.iloc[pagina : (pagina+1)*50, ]
    # print(carrega_busca_produtocompleto2)


    lista = []
    for index, row in carrega_busca_produtocompletoestoquedescricao.iterrows():
        colunas = ["IDSUBPRODUTO", "DESCRCOMPRODUTO" , "PERCOMAVISTA", "VALPRECOVAREJO", "VALPROMVAREJO", "DESCRGRUPO", "DESCRSUBGRUPO", "DESCRSECAO", "ESTOQUEGRAVATAI", "ESTOQUESEVERO", "EMBALAGEMSAIDA", "DATAFINALPROMOCAO" ]
        dados_produtocompleto_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_produtocompleto_ciss)
        lista.append(dados_produtocompleto_ciss)
    if lista == []:
            raise HTTPException(status_code=404,detail="--- Produto nao Encontrado -- Sem Cadastro -- Quantidade maior que a disponivel para estes produtos -- Tente Modificar a Descricao de Busca--") 
    #return dados_produtocompleto_ciss
    return {'data':lista}

@app.get("/produtocodbar/{idproduto}")
async def produto_busca(idproduto:str):
    """
    Esta chamada retorna o código de barras de um determinado produto.
    Exemplo para filtrar:
    idproduto = 1059453

    """
    #cpfcnpj:Busca_Cliente.cpfcnpj
    logging.info(f"Endpoint '/produtocodbar/{idproduto}' acessado com sucesso.")  
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_codigobarra =pd.read_sql(f'''select distinct pv.idcodbarprod
from dba.produtos_view pv
where pv.idproduto='{idproduto}' ''',conexao)

    cursor.commit()
    cursor.close()

    for index, row in carrega_busca_codigobarra.iterrows():
        colunas = ["IDCODBARPROD"]
        dados_codigobarra_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_codigobarra_ciss)

    return dados_codigobarra_ciss


@app.get("/crosssel/{idsubproduto}")
async def produto_busca(idsubproduto:str):
    """
    Esta chamada retorna uma lista dos 20 produtos mais vendidos junto com o produto passado no filtro.
    Exemplo para filtrar:
    idsubproduto = 1059453

    """
    #cpfcnpj:Busca_Cliente.cpfcnpj
    logging.info(f"Endpoint '/crosssel/{idsubproduto}' acessado com sucesso.")  
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_crosssel =pd.read_sql(f'''select
        principal.IDSUBPRODUTO,
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
from
        (
        select
                t1.IDSUBPRODUTO,
                t2.IDSUBPRODUTO as idsubproduto_p2,
                count(*) as num_sales,
                sum(t1.VALORVENDALIQ) as valor_p1,
                sum(t2.VALORVENDALIQ) as valor_p2,
                avg(t2.PERMARGEM) as margem_per
        from
                (
                select
                        *
                from
                        vendas_bi_elevato where tipovenda = 'NORMAL' and data between current date -365 and current date
                  and idsubproduto= {idsubproduto}     ) t1
        join (
                select
                        *
                from
                        vendas_bi_elevato where tipovenda = 'NORMAL' and data between current date -365 and current date ) t2 on
                t1.IDORCAMENTO = t2.IDORCAMENTO
                and t1.IDSUBPRODUTO <> t2.IDSUBPRODUTO
        group by
                t1.IDSUBPRODUTO,
                t2.IDSUBPRODUTO
        order by
                (count(*)) desc) principal
join (
        select
                IDSUBPRODUTO,
                DESCCOMPRODUTO as descr_p1,
                DESCRGRUPO as grupo_p1,
                DESCRSUBGRUPO as subgrupo_p1
        from
                produtos_bi_elevato) tp on
        principal.IDSUBPRODUTO = tp.IDSUBPRODUTO
join (
        select
                IDSUBPRODUTO,
                DESCCOMPRODUTO as descr_p2,
                DESCRGRUPO as grupo_p2,
                DESCRSUBGRUPO as subgrupo_p2
        from
                produtos_bi_elevato) tp2 on
        principal.idsubproduto_p2 = tp2.IDSUBPRODUTO
order by num_sales desc
limit 20 ''',conexao)

    cursor.commit()
    cursor.close()
    
    lista = []
    for index, row in carrega_busca_crosssel.iterrows():
        colunas = ["IDSUBPRODUTO", "IDSUBPRODUTO_P2", "NUM_SALES", "VALOR_P1", "VALOR_P2","MARGEM_PER", "MARGEM_VALOR", "DESCR_P1", "GRUPO_P1", "SUBGRUPO_P1",  "DESCR_P2", "GRUPO_P2", "SUBGRUPO_P2" ]
        dados_crosssel_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_crosssel_ciss)
        lista.append(dados_crosssel_ciss)
    return {'data':lista}
    

@app.get("/vendasresumo/{idvendedor}")
async def vendas_resumo(idvendedor:str):
    """
    Esta chamada retorna um resumo do valor de vendas e margem de um determinado vendedor no mês corrente.

    Exemplo para filtrar:
    idempresa = 1137478

    """
    #cpfcnpj:Busca_Cliente.cpfcnpj
    logging.info(f"Endpoint '/vendasresumo/{idvendedor}' acessado com sucesso.")  
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_vendaresumo =pd.read_sql(f'''select 
                                                    cast(vendadia.valorvendadia as varchar(20)) as VALORVENDADIA, 
                                                    cast(vendadia.valormargemdia as varchar(20)) as VALORMARGEMDIA, 
                                                    cast(vendames.valorvendames as varchar(20)) as VALORVENDAMES,
                                                    cast(vendames.valormargemmes as varchar(20)) as VALORMARGEMMES
from
(select SUM(VALORVENDALIQ)AS VALORVENDADIA,SUM(VALORVENDALIQ*PERMARGEM)/100 AS VALORMARGEMDIA from VENDAS_BI_ELEVATO
WHERE DATA = CURRENT DATE AND IDVENDEDOR in ('{idvendedor}')) as vendadia,
(select  SUM(VALORVENDALIQ)AS VALORVENDAMES,SUM(VALORVENDALIQ*PERMARGEM)/100 AS VALORMARGEMMES
from VENDAS_BI_ELEVATO
WHERE (MONTH(DATA) = MONTH(CURRENT DATE) AND YEAR(DATA)= YEAR(CURRENT DATE))
AND IDVENDEDOR in ('{idvendedor}')) as vendames ''',conexao)

    cursor.commit()
    cursor.close()

    for index, row in carrega_busca_vendaresumo.iterrows():
        colunas = ["VALORVENDADIA","VALORMARGEMDIA","VALORVENDAMES","VALORMARGEMMES"]
        dados_vendaresumo_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_vendaresumo_ciss)

    return dados_vendaresumo_ciss


@app.get("/itenspedido/{idpedido}")
async def vendas_resumo(idpedido:str):
    """
    Esta chamada retorna uma lista de itens de um determinado pedido já efetivado no CISS.

    Exemplo para filtrar:
    idpedido = 3083928

    """
    #cpfcnpj:Busca_Cliente.cpfcnpj
    logging.info(f"Endpoint '/itenspedido/{idpedido}' acessado com sucesso.")  
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_itenspedido =pd.read_sql(f'''select IDEMPRESA,DATA,IDVENDEDOR,IDPRODUTO,IDSUBPRODUTO,MARCA,DESCRPRODUTO,NUMSEQUENCIA,QTDPRODUTO,VALORVENDALIQ,VALORFRETEVENDA
from VENDAS_BI_ELEVATO
WHERE  IDORCAMENTO = '{idpedido}' ''',conexao)

    cursor.commit()
    cursor.close()

    lista = []
    for index, row in carrega_busca_itenspedido.iterrows():
        colunas = ["IDEMPRESA","DATA","IDVENDEDOR","IDPRODUTO","IDSUBPRODUTO","MARCA","DESCRPRODUTO","NUMSEQUENCIA","QTDPRODUTO","VALORVENDALIQ","VALORFRETEVENDA"]
        dados_itenspedido_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_itenspedido_ciss)
        lista.append(dados_itenspedido_ciss)
    return {'data':lista}


@app.get("/estoque/{idproduto}")
async def estoque_busca(idproduto:str):
    """
    Esta chamada retorna a quantidade disponível de estoque nos CDs de um determinado produto.

    Exemplo para filtrar:
    idproduto = 1059453

    """
    #cpfcnpj:Busca_Cliente.cpfcnpj
    logging.info(f"Endpoint '/estoque/{idproduto}' acessado com sucesso.")  
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_estoque =pd.read_sql(f'''select
        CAST(((qtdatualestoque-(qtdsaldoreserva+qtdsaldovendafutura+qtdsaldoemtransferencia))/VALMULTIVENDAS) AS VARCHAR(20)) as qtddisponivel
        from
        (SELECT
            IDPRODUTO,
            IDSUBPRODUTO,
            SUM(QTDATUALESTOQUE) AS QTDATUALESTOQUE,
            SUM(QTDSALDORESERVA) AS QTDSALDORESERVA,
            SUM(QTDSALDOVENDAFUTURA) AS  QTDSALDOVENDAFUTURA,
            SUM(QTDSALDOEMTRANSFERENCIA) AS QTDSALDOEMTRANSFERENCIA
        FROM
            (
                SELECT
                    CAST ('F' AS VARCHAR (1)) AS PONTAESTOQUE,
                    CAST ('' AS VARCHAR (1)) AS SINAL5,
                    ESA.IDEMPRESA,
                    ESA.IDPRODUTO,
                    ESA.IDSUBPRODUTO,
                    ESA.IDLOCALESTOQUE,
                    ESA.DTMOVIMENTO,
                    ESA.QTDATUALESTOQUE,
                    ECL.FLAGTROCAPROD,
                    ECL.FLAGDISPONVENDA,
                    ESA.VALATUALESTOQUE,
                    EMP.NOMEFANTASIA,
                    ECL.DESCRLOCAL AS NOMELOCALESTOQUE,
                    PV.VALGRAMAENTRADA,
                    CASE
                        WHEN ESA.QTDATUALESTOQUE = 0
                            THEN 'T'
                        ELSE 'F'
                    END AS FLAGLOCALINEXISTENTE,
                    VALCUSTOMEDIO,
                    COALESCE (PSV.QTDSALDORESERVA, 0) AS QTDSALDORESERVA,
                    CAST (0 AS DECIMAL (15, 3)) AS QTDSALDOVENDAFUTURA,
                    CAST (0 AS DECIMAL (15, 3)) AS QTDSALDOEMTRANSFERENCIA,
                    CASE
                        WHEN FABRICANTE = ''
                            THEN 'SEM FABRICANTE'
                        ELSE coalesce(fabricante, 'SEM FABRICANTE')END AS FABRICANTE,
                    MARCA.IDMARCAFABRICANTE,
                    DIVISAO.IDDIVISAO,
                    PV.DESCRICAOPRODUTO,
                    PV.REFERENCIA,
                    DIVISAO.DESCRDIVISAO AS DIVISAO,
                    ESA.DTULTIMAVENDA,
                    ESA.DTULTIMACOMPRA

                FROM
                    DBA. PRODUTOS_VIEW AS PV
                    JOIN DBA.ESTOQUE_SALDO_ATUAL ESA
                    ON  ESA.IDPRODUTO = PV.IDPRODUTO AND ESA.IDSUBPRODUTO=PV.IDSUBPRODUTO
                    JOIN DBA.ESTOQUE_CADASTRO_LOCAL ECL
                    ON  ESA.IDLOCALESTOQUE = ECL.IDLOCALESTOQUE
                    JOIN DBA.EMPRESA EMP
                    ON  ESA.IDEMPRESA = EMP.IDEMPRESA
                    JOIN DBA.DIVISAO AS DIVISAO
                    ON PV.IDDIVISAO=DIVISAO.IDDIVISAO
                    JOIN DBA.MARCA AS MARCA
                    ON (PV.IDMARCAFABRICANTE = MARCA.IDMARCAFABRICANTE)
                    LEFT OUTER JOIN
                        (
                            SELECT
                                SUM (PV.QTDSALDORESERVA) AS QTDSALDORESERVA,
                                PV.IDLOCALESTOQUE,
                                PV.IDEMPRESA,
                                PV.IDPRODUTO,
                                PV.IDSUBPRODUTO
                            FROM
                                DBA.PRODUTOS_SALDOS_VIEW PV
                                INNER JOIN DBA.PRODUTO PRO1
                                ON  PRO1.IDPRODUTO = PV.IDPRODUTO
                            GROUP BY
                                PV.IDLOCALESTOQUE,
                                PV.IDEMPRESA,
                                PV.IDPRODUTO,
                                PV.IDSUBPRODUTO
                        )
                        AS PSV
                    ON  ESA.IDPRODUTO = PSV.IDPRODUTO AND
                        ESA.IDSUBPRODUTO = PSV.IDSUBPRODUTO AND
                        ESA.IDLOCALESTOQUE = PSV.IDLOCALESTOQUE AND
                        ESA.IDEMPRESA = PSV.IDEMPRESA
                    LEFT OUTER JOIN DBA.CONFIG_ENTIDADE CE
                    ON  (
                            CE.ENTIDADE = 'CME' AND
                            CE.CHAVE1 = 1 AND
                            CE.CHAVE2 = 1
                        )
                WHERE ESA.IDLOCALESTOQUE IN (6,124)
            )
            AS TEMP
            GROUP BY
                IDPRODUTO,
                IDSUBPRODUTO

        UNION ALL

        SELECT
            VF.IDPRODUTO,
            VF.IDSUBPRODUTO,
            CAST (0 AS DECIMAL) AS QTDATUALESTOQUE,
            CAST (0 AS DECIMAL) AS QTDSALDORESERVA,
            SUM(VF.VENDASFUTURASPENDENTES) AS QTDSALDOVENDAFUTURA,
            CAST (0 AS DECIMAL (15, 3)) AS QTDSALDOEMTRANSFERENCIA

        FROM
            (
                SELECT
                    SUM (NOTAS_SALDOS.QTDSALDOATUAL) AS VENDASFUTURASPENDENTES,
                    NOTAS_SALDOS.IDPRODUTO,
                    NOTAS_SALDOS.IDSUBPRODUTO,
                    NOTAS_SALDOS.IDEMPRESA

                FROM
                    DBA.NOTAS_SALDOS AS NOTAS_SALDOS,
                    DBA.NOTAS_VFUTURA AS NOTAS_VFUTURA
                WHERE
                    NOTAS_SALDOS.IDEMPRESA = NOTAS_VFUTURA.IDEMPRESA AND
                    NOTAS_SALDOS.IDPLANILHAORIGEM = NOTAS_VFUTURA.IDPLANILHA AND
                    NOTAS_SALDOS.IDPRODUTO = NOTAS_VFUTURA.IDPRODUTO AND
                    NOTAS_SALDOS.IDSUBPRODUTO = NOTAS_VFUTURA.IDSUBPRODUTO AND
                    NOTAS_SALDOS.QTDSALDOATUAL > 0 AND
                    COALESCE (NOTAS_SALDOS.IDLOTE, '') = COALESCE (NOTAS_VFUTURA.IDLOTE, '') AND
                    NOT EXISTS
                    (
                        SELECT
                            1
                        FROM
                            DBA.ESTOQUE_SALDO_ATUAL ESA
                        WHERE
                            ESA.IDPRODUTO = NOTAS_SALDOS.IDPRODUTO AND
                            ESA.IDSUBPRODUTO = NOTAS_SALDOS.IDSUBPRODUTO
                    )
                GROUP BY
                    NOTAS_SALDOS.IDEMPRESA,
                    NOTAS_SALDOS.IDPRODUTO,
                    NOTAS_SALDOS.IDSUBPRODUTO

            )
            AS VF

        GROUP BY
        VF.IDPRODUTO,
        VF.IDSUBPRODUTO) as resumo
        left join dba.produtos_view as pv
        on resumo.idproduto=pv.idproduto and resumo.idsubproduto=pv.idsubproduto
        where resumo.idproduto= '{idproduto}'  ''',conexao)

    cursor.commit()
    cursor.close()

    for index, row in carrega_busca_estoque.iterrows():
        colunas = ["QTDDISPONIVEL"]
        dados_estoque_ciss = {coluna: row[coluna] for coluna in colunas}
        #ados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_estoque_ciss)

    return dados_estoque_ciss


@app.get("/especificadores/{cnpjcpf}")
async def produto_busca(cnpjcpf:str):
    logging.info(f"Endpoint '/especificadores/{cnpjcpf}' acessado com sucesso.")  
    pyodbc.drivers()
    """
    Esta chamada retorna dados de um especificador filtrando pelo cpf ou pelo número do telefone.

    Exemplo para filtrar:
    cnpjcpf = 56581580015 ou 51997038022

    """
    #cpfcnpj:Busca_Cliente.cpfcnpj

    

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_especificador =pd.read_sql(f''' SELECT
        CLIENTE_FORNECEDOR.IDCLIFOR,
        CLIENTE_FORNECEDOR.NOME,
        CI.IDCLUBE,
        CF.DESCRCLUBE
FROM
        DBA.CLIENTE_FORNECEDOR AS CLIENTE_FORNECEDOR
       LEFT OUTER JOIN DBA.GRUPO_ECONOMICO AS GRUPO_ECONOMICO
        ON (CLIENTE_FORNECEDOR.IDGRUPOECONOMICO = GRUPO_ECONOMICO.IDGRUPOECONOMICO)
      LEFT JOIN DBA.CLUBE_INDICADOR AS CI
        ON (CLIENTE_FORNECEDOR.IDCLIFOR = CI.IDINDICADOR)
      LEFT JOIN DBA.CLUBE_FIDELIZACAO AS CF
        ON (CI.IDCLUBE = CF.IDCLUBE)
      LEFT JOIN DBA.CLIENTE_AUTORIZADOS AS CLIENTE_AUTORIZADOS
        ON (CLIENTE_FORNECEDOR.IDCLIFOR = CLIENTE_AUTORIZADOS.IDCLIFOR)
      LEFT JOIN DBA.PESSOA_FISICA AS PESSOA_FISICA
        ON (CLIENTE_FORNECEDOR.IDCLIFOR = PESSOA_FISICA.IDCLIFOR)
WHERE
        CI.IDCLUBE IS NOT NULL AND CI.FLAGINATIVO='F' AND (CLIENTE_FORNECEDOR.CNPJCPF = '{cnpjcpf}' OR
           -- CLIENTE_FORNECEDOR.NOME LIKE '%{cnpjcpf}%' OR
            CLIENTE_FORNECEDOR.FONE2 = '{cnpjcpf}' OR
            CLIENTE_FORNECEDOR.FONE1 = '{cnpjcpf}' OR
            CLIENTE_FORNECEDOR.FONEFAX = '{cnpjcpf}' OR
            CLIENTE_FORNECEDOR.FONECELULAR = '{cnpjcpf}') ''',conexao)

    cursor.commit()
    cursor.close()

    for index, row in carrega_busca_especificador.iterrows():
        colunas = ["IDCLIFOR", "NOME", "IDCLUBE","DESCRCLUBE"]
        dados_especificador_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_especificador_ciss)

    return dados_especificador_ciss


@app.get("/orcamentocabecalho/{idvendedor}")
async def orcamentocabecalho_busca(idvendedor:str):
    """
    Esta chamada retorna uma lista com dados de um cabecalho de orçamento filtrando pelo idvendedor.

    Exemplo para filtrar:
    idvendedor = 1137478

    """
    #cpfcnpj:Busca_Cliente.cpfcnpj
    logging.info(f"Endpoint '/orcamentocabecalho/{idvendedor}' acessado com sucesso.")  
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_orcamentocabecalho =pd.read_sql(f''' SELECT
        O.IDORCAMENTO,
        O.IDCLIFOR,
        O.DTMOVIMENTO,
        O.NOME AS NOME,
        O.FLAGCANCELADO,
        CAST(SUM(OP.VALTOTLIQUIDO) AS VARCHAR(20)) AS VALTOTLIQUIDO
FROM DBA.ORCAMENTO AS O
        LEFT JOIN DBA.ORCAMENTO_PROD AS OP
        ON O.IDORCAMENTO=OP.IDORCAMENTO AND O.IDEMPRESA=OP.IDEMPRESA
WHERE
        FLAGPRENOTA= 'F' AND O.DTMOVIMENTO>= CURRENT DATE-120 AND OP.IDVENDEDOR in ({idvendedor})
        AND OP.IDORCAMENTOORIGEM IS NULL
GROUP BY
        O.IDORCAMENTO,
        O.IDCLIFOR,
        O.NOME,
        O.DTMOVIMENTO,
        O.FLAGCANCELADO ''',conexao)

    cursor.commit()
    cursor.close()

    lista =[]
    for index, row in carrega_busca_orcamentocabecalho.iterrows():
        colunas = ["IDORCAMENTO","IDCLIFOR", "NOME", "VALTOTLIQUIDO","DTMOVIMENTO", "FLAGCANCELADO"]
        dados_orcamentocompleto_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_orcamentocompleto_ciss)
        lista.append(dados_orcamentocompleto_ciss)
    return {'data':lista}

@app.get("/pedidocabecalho/{idvendedor}")
async def pedidocabecalho_busca(idvendedor:str):
    """
    Esta chamada retorna uma lista com dados de um cabecalho de pedido efetivado filtrando pelo idvendedor.

    Exemplo para filtrar:
    idvendedor = 1137478

    """
    #cpfcnpj:Busca_Cliente.cpfcnpj
    logging.info(f"Endpoint '/pedidocabecalho/{idvendedor}' acessado com sucesso.")  
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_pedidocabecalho =pd.read_sql(f''' SELECT DISTINCT
        'P' AS TIPO,
        ORCAMENTO_PROD.IDEMPRESA,
        CAST(ORCAMENTO_PROD.IDORCAMENTOORIGEM AS VARCHAR(20)) AS IDORCAMENTOORIGEM,
        EMPRESA.NOMEFANTASIA,
        ORCAMENTO.IDCLIFOR,
        CLIENTE_FORNECEDOR.NOME,
        CI.DESCRCIDADE,
        CI.UF,
        --DATE(ORCAMENTO.DTMOVIMENTO) AS DATA,
        --ORCAMENTO.DTMOVIMENTO AS DATAHORA,
        ORCAMENTO_PROD.IDORCAMENTO,
        CAST(COALESCE(SUM(ORCAMENTO_PROD.VALTOTLIQUIDO),0) AS VARCHAR(20)) AS VALOR_VENDA,
        CAST(COALESCE(SUM(ORCAMENTO_PROD.VALFRETE),0) AS VARCHAR(20)) AS VALFRETE_VENDA
    FROM
        DBA.EMPRESA AS EMPRESA
                JOIN DBA.ORCAMENTO AS ORCAMENTO ON
                                                (EMPRESA.IDEMPRESA = ORCAMENTO.IDEMPRESA)
                JOIN DBA.CLIENTE_FORNECEDOR AS CLIENTE_FORNECEDOR ON
                                                (ORCAMENTO.IDCLIFOR = CLIENTE_FORNECEDOR.IDCLIFOR)
                JOIN DBA.ORCAMENTO_PROD AS ORCAMENTO_PROD ON
                                                (ORCAMENTO.IDEMPRESA = ORCAMENTO_PROD.IDEMPRESA AND
                                                ORCAMENTO.IDORCAMENTO = ORCAMENTO_PROD.IDORCAMENTO)
                JOIN DBA.PRODUTOS_VIEW AS PRODUTOS_VIEW ON
                                                (ORCAMENTO_PROD.IDPRODUTO = PRODUTOS_VIEW.IDPRODUTO AND
                                                ORCAMENTO_PROD.IDSUBPRODUTO = PRODUTOS_VIEW.IDSUBPRODUTO)
                JOIN DBA.CIDADES_IBGE AS CI ON
                                                (CI.IDCIDADE = ORCAMENTO.IDCIDADE)
                LEFT JOIN DBA.MARCA AS MARCA ON
                                                (MARCA.DESCRICAO = PRODUTOS_VIEW.FABRICANTE)
    WHERE
        ORCAMENTO.FLAGPRENOTAPAGA = 'T' AND
        ORCAMENTO_PROD.IDVENDEDOR in ({idvendedor})  AND
        DATE(ORCAMENTO.DTMOVIMENTO)  BETWEEN CURRENT DATE-120  AND CURRENT DATE
--        ORCAMENTO_PROD.IDORCAMENTO IN (:RA_IDORCAMENTO) AND
--        ORCAMENTO_PROD.IDEMPRESA IN (:RA_IDEMPRESA) AND
        AND NOT EXISTS
        (
        SELECT
            1
        FROM
            DBA.CLIENTE_FORNECEDOR AS CF,
            DBA.EMPRESA AS EMP
        WHERE
            CF.CNPJCPF = EMP.CNPJ AND
            CF.IDCLIFOR = ORCAMENTO.IDCLIFOR AND
            EMP.IDEMPRESA IN (SELECT DISTINCT IDEMPRESAENTRADA FROM DBA.CONFIG_TRANSF_NOTA)
        )
GROUP BY
        ORCAMENTO_PROD.IDEMPRESA,
        CAST(ORCAMENTO_PROD.IDORCAMENTOORIGEM AS VARCHAR(20)),
        EMPRESA.NOMEFANTASIA,
        ORCAMENTO.IDCLIFOR,
        CLIENTE_FORNECEDOR.NOME,
        CI.DESCRCIDADE,
        CI.UF,
        --DATE(ORCAMENTO.DTMOVIMENTO) ,
        --ORCAMENTO.DTMOVIMENTO ,
        ORCAMENTO_PROD.IDORCAMENTO ''',conexao)

    cursor.commit()
    cursor.close()

    lista =[]
    for index, row in carrega_busca_pedidocabecalho.iterrows():
        colunas = ["IDEMPRESA", "IDORCAMENTOORIGEM", "NOMEFANTASIA", "IDCLIFOR", "NOME", "DESCRCIDADE", "UF", "IDORCAMENTO","VALOR_VENDA", "VALFRETE_VENDA"]
        dados_pedidocabecalhocompleto_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_pedidocabecalhocompleto_ciss)
        lista.append(dados_pedidocabecalhocompleto_ciss)
    return {'data':lista}




@app.get("/orcamentocabecalhoorcamento/{idorcamento}")
async def orcamentocabecalho_busca(idorcamento:str):
    """
    Esta chamada retorna  dados de cabecalho de orçamento filtrando pelo idorcamento.

    Exemplo para filtrar:
    idorcamento = 3083769( obs.: este id pode ter sido cancelado já, sugiro consultar o CISS para ter um id orçamento mais atual)

    """
    #cpfcnpj:Busca_Cliente.cpfcnpj
    logging.info(f"Endpoint '/orcamentocabecalhoorcamento/{idorcamento}' acessado com sucesso.")  
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_orcamentocabecalhoorc =pd.read_sql(f''' SELECT
        O.IDORCAMENTO,
        O.ENDERECO || ','|| O.NUMERO ||' - COMPL.: '|| O.COMPLEMENTO||' - BAIRRO: '|| O.BAIRRO || ' - CIDADE: '||CI.DESCRCIDADE||'-'||O.UF AS ENDERECOCOMPLETO,
        O.IDCEP,
        O.UF,
        O.OBSERVACAO,
        O.CNPJCPF,
        O.BAIRRO,
        O.NUMERO,
        O.COMPLEMENTO,
        O.FONECELULAR,
        O.IDCLIFOR || '-' || O.NOME AS CLIENTE,
        O.NOME AS NOME,
        OP.IDVENDEDOR,
        VENDEDOR.NOME AS NOMEVENDEDOR,
        VENDEDOR.EMAIL AS EMAILVENDEDOR,
        O.IDEMPRESA,
        O.FLAGCANCELADO,
        EMPRESA.EMPALIAS AS NOMELOJA,
        EMPRESA.FONE AS FONEEMPRESA,
        EMPRESA.EMAIL AS EMAILEMPRESA,
        EMPRESA.ENDERECO || ',' || EMPRESA.NUMERO || ' - '|| EMPRESA.BAIRRO || ' - ' || CIDEMP.DESCRCIDADE AS ENDERECOEMPRESA,
        PERFIL.DESCRICAO AS PERFILFRETE,
        CAST(SUM(OP.VALTOTLIQUIDO) AS VARCHAR(20)) AS VALTOTLIQUIDO
FROM DBA.ORCAMENTO AS O
        LEFT JOIN DBA.ORCAMENTO_PROD AS OP
        ON O.IDORCAMENTO=OP.IDORCAMENTO AND O.IDEMPRESA=OP.IDEMPRESA
        LEFT JOIN DBA.CLIENTE_FORNECEDOR AS VENDEDOR
        ON OP.IDVENDEDOR=VENDEDOR.IDCLIFOR
        LEFT JOIN DBA.EMPRESA AS EMPRESA
        ON EMPRESA.IDEMPRESA=O.IDEMPRESA
        LEFT JOIN DBA.CIDADES_IBGE AS CI
        ON CI.IDCIDADE=O.IDCIDADE
        LEFT JOIN DBA.CIDADES_IBGE AS CIDEMP
        ON EMPRESA.IDCIDADE=CIDEMP.IDCIDADE
        LEFT JOIN DBA.PERFIL_FRETE AS PERFIL
        ON O.IDPERFIL= PERFIL.IDPERFIL
WHERE
      O.IDORCAMENTO= '{idorcamento}'
GROUP BY
        O.IDORCAMENTO,
        O.ENDERECO || ','|| O.NUMERO ||' - COMPL.: '|| O.COMPLEMENTO||' - BAIRRO: '|| O.BAIRRO || ' - CIDADE: '||CI.DESCRCIDADE||'-'||O.UF ,
        O.IDCEP,
        O.UF,
        O.OBSERVACAO,
        O.CNPJCPF,
        O.BAIRRO,
        O.NUMERO,
        O.COMPLEMENTO,
        O.FONECELULAR,
        O.IDCLIFOR || '-' || O.NOME,
        O.NOME ,
        OP.IDVENDEDOR,
        VENDEDOR.NOME,
        VENDEDOR.EMAIL ,
        O.IDEMPRESA,
        O.FLAGCANCELADO,
        EMPRESA.EMPALIAS ,
        EMPRESA.FONE,
        EMPRESA.EMAIL ,
        EMPRESA.ENDERECO || ',' || EMPRESA.NUMERO || ' - '|| EMPRESA.BAIRRO || ' - ' || CIDEMP.DESCRCIDADE,
        PERFIL.DESCRICAO ''',conexao)

    cursor.commit()
    cursor.close()

    #lista =[]
    for index, row in carrega_busca_orcamentocabecalhoorc.iterrows():
        colunas = ["IDORCAMENTO", "ENDERECOCOMPLETO", "IDCEP", "UF", "OBSERVACAO", "CNPJCPF", "BAIRRO", "NUMERO", "COMPLEMENTO", "FONECELULAR", "CLIENTE", "NOME", "IDVENDEDOR" , "NOMEVENDEDOR" , "EMAILVENDEDOR", "IDEMPRESA" , "FONEEMPRESA", "ENDERECOEMPRESA",  "EMAILEMPRESA", "NOMELOJA", "PERFILFRETE" , "VALTOTLIQUIDO", "FLAGCANCELADO"]
        dados_orcamentocompletoorc_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_orcamentocompletoorc_ciss)
        #lista.append(dados_orcamentocompletoorc_ciss)
    return dados_orcamentocompletoorc_ciss


@app.get("/orcamentoespecificador/{idespecificador}")
async def orcamentocabecalho_busca(idespecificador:str):
    """
    Esta chamada retorna uma lista com dados de um cabecalho de orçamento filtrando pelo idespecificador.

    Exemplo para filtrar:
    idespecificador = 1188771

    """
    #cpfcnpj:Busca_Cliente.cpfcnpj
    logging.info(f"Endpoint '/orcamentoespecificador/{idespecificador}' acessado com sucesso.") 
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_orcamentocabecalhoespecificador =pd.read_sql(f''' SELECT
        O.IDORCAMENTO,
        O.IDCLIFOR,
        O.DTMOVIMENTO,
        O.NOME AS NOME,
        CAST(SUM(OP.VALTOTLIQUIDO) AS VARCHAR(20)) AS VALTOTLIQUIDO
FROM DBA.ORCAMENTO AS O
        LEFT JOIN DBA.ORCAMENTO_PROD AS OP
        ON O.IDORCAMENTO=OP.IDORCAMENTO AND O.IDEMPRESA=OP.IDEMPRESA
        LEFT JOIN DBA.CLUBE_INDICADOR_ORCAMENTO AS CIO
        ON O.IDORCAMENTO=CIO.IDORCAMENTO
WHERE
        FLAGPRENOTA= 'F' AND O.DTMOVIMENTO>= CURRENT DATE-120 AND CIO.IDINDICADOR='{idespecificador}'
        AND OP.IDORCAMENTOORIGEM IS NULL
GROUP BY
        O.IDORCAMENTO,
        O.IDCLIFOR,
        O.NOME,
        O.DTMOVIMENTO ''',conexao)

    cursor.commit()
    cursor.close()

    lista =[]
    for index, row in carrega_busca_orcamentocabecalhoespecificador.iterrows():
        colunas = ["IDORCAMENTO","IDCLIFOR", "NOME", "VALTOTLIQUIDO","DTMOVIMENTO"]
        dados_orcamentocompletoespecificador_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_orcamentocompletoespecificador_ciss)
        lista.append(dados_orcamentocompletoespecificador_ciss)
    return {'data':lista}


@app.get("/orcamentoprodutovendedor/{idvendedor}&{idproduto}")
async def orcamentocabecalho_busca(idvendedor:str,
                                   idproduto:str):
    """
    Esta chamada retorna uma lista com dados de orcamento/itens de orcamento filtrando um item e um vendedor.

    Exemplo para filtrar:
    idvendedor = 1137478
    idproduto = 1059453

    """
    #cpfcnpj:Busca_Cliente.cpfcnpj
    logging.info(f"Endpoint '/orcamentoprodutovendedor/{idvendedor}&{idproduto}' acessado com sucesso.") 
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_orcamentoprodutovendedor =pd.read_sql(f''' select
                O.idorcamento,
                OP.IDVENDEDOR,
                CAST(qtdproduto AS VARCHAR(20)) AS QTDPRODUTO,
                CAST(valunitbruto AS VARCHAR(20)) AS VALUNITBRUTO,
                CAST(valtotliquido AS VARCHAR(20)) AS VALTOTLIQUIDO,
                CAST(valfrete AS VARCHAR (20)) AS VALFRETE
                from dba.orcamento_prod AS OP
                LEFT JOIN DBA.ORCAMENTO AS O
                ON OP.IDORCAMENTO=O.IDORCAMENTO AND OP.IDEMPRESA=O.IDEMPRESA
                where  O.FLAGPRENOTA = 'F' AND O.DTMOVIMENTO>= CURRENT DATE-120 AND OP.IDPRODUTO in ({idproduto})
                and op.idvendedor in ({idvendedor})''',conexao)

    cursor.commit()
    cursor.close()

    lista =[]
    for index, row in carrega_busca_orcamentoprodutovendedor.iterrows():
        colunas = ["IDORCAMENTO","IDVENDEDOR", "QTDPRODUTO", "VALTOTLIQUIDO","VALUNITBRUTO", "VALFRETE"]
        dados_orcamentoproduto_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_orcamentoproduto_ciss)
        lista.append(dados_orcamentoproduto_ciss)
    return {'data':lista}


@app.get("/especificadorvendedor/{idvendedor}")
async def especificadorvendedor_busca(idvendedor:str):
    """
    Esta chamada retorna uma lista com dados de especificadores filtrando pelo vendedor(idvendedor).

    Exemplo para filtrar:
    idvendedor = 1137478

    """
    #cpfcnpj:Busca_Cliente.cpfcnpj
    logging.info(f"Endpoint '/especificadorvendedor/{idvendedor}' acessado com sucesso.") 
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_especificadorvendedor =pd.read_sql(f'''         SELECT
                CLUBE.IDINDICADOR,
                CLUBE.NOMEINDICADOR,
                CLUBE.CNPJCPF_IND,
                CAST(count(temp.idorcamento) AS VARCHAR(10)) as qtdvendas,
                CLUBE.FONECELULAR,
                VENDEDOR.IDCLIFOR AS IDVENDEDOR,
                VENDEDOR.NOME AS NOMEVENDEDOR,
                MAX(TEMP.DATA)  AS ULTIMACOMPRA,
                CAST(DAYS(CURRENT DATE) - DAYS(MAX(TEMP.DATA)) AS VARCHAR(10)) AS DIASSEMCOMPRAS,
                (CASE
                        WHEN (CURRENT DATE - MAX(TEMP.DATA))BETWEEN 61 AND 90 THEN 'ATENÇÃO'
                        WHEN (CURRENT DATE - MAX(TEMP.DATA))>91 THEN 'INATIVO'
                        ELSE 'ATIVO' END) AS STATUS,
                CAST(ROUND(SUM(COALESCE(TEMP.VALOR_VENDA,0)),2) AS VARCHAR(20)) AS VALOR_VENDA_INDICADOR,
                CAST(ROUND(SUM(COALESCE(TEMP.VALFRETE_VENDA,0)),2) AS VARCHAR(20)) AS VALFRETE_VENDA_INDICADOR,
                CAST(ROUND(SUM(COALESCE(TEMP.VALOR_DEV,0)) - SUM(COALESCE(TEMP.VALFRETE_DEV,0)),2) AS VARCHAR(20)) AS VALOR_DEV_INDICADOR,
                CAST(ROUND(SUM(COALESCE(TEMP.VALOR_CAN,0)),2) AS VARCHAR(20)) AS VALOR_CAN_INDICADOR,
                CAST(ROUND(((SUM(COALESCE(TEMP.VALOR_VENDA,0)) - SUM(COALESCE(TEMP.VALFRETE_VENDA,0))) - (SUM(COALESCE(TEMP.VALOR_DEV,0)) - SUM(COALESCE(TEMP.VALFRETE_DEV,0))) - (SUM(COALESCE(TEMP.VALOR_CAN,0)))),2) AS VARCHAR(20)) AS VALORLIQUIDOVENDAINDICADOR
        FROM
                (
                SELECT DISTINCT
                        'P' AS TIPO,
                        ORCAMENTO_PROD.IDEMPRESA,
                        EMPRESA.NOMEFANTASIA,
                        ORCAMENTO.IDCLIFOR,
                        CLIENTE_FORNECEDOR.NOME,
                                        CI.DESCRCIDADE,
                        CI.UF,
                        DATE(ORCAMENTO.DTMOVIMENTO) AS DATA,
                        ORCAMENTO_PROD.IDORCAMENTO,
                        ORCAMENTO_PROD.IDVENDEDOR,
                        ORCAMENTO_PROD.IDPRODUTO,
                        ORCAMENTO_PROD.IDSUBPRODUTO,
                        PRODUTOS_VIEW.FABRICANTE AS MARCA,
                        ORCAMENTO_PROD.NUMSEQUENCIA,
                        PRODUTOS_VIEW.DESCRICAOPRODUTO AS DESCRICAOPRODUTO,
                        COALESCE(ORCAMENTO_PROD.VALTOTLIQUIDO,0) AS VALOR_VENDA,
                        COALESCE(ORCAMENTO_PROD.VALFRETE,0) AS VALFRETE_VENDA,
                        0 AS VALOR_DEV,
                        0 AS VALFRETE_DEV,
                        0 AS VALOR_CAN
                FROM
                        DBA.EMPRESA AS EMPRESA JOIN DBA.ORCAMENTO AS ORCAMENTO ON
                        (EMPRESA.IDEMPRESA = ORCAMENTO.IDEMPRESA) JOIN DBA.CLIENTE_FORNECEDOR AS CLIENTE_FORNECEDOR ON
                        (ORCAMENTO.IDCLIFOR = CLIENTE_FORNECEDOR.IDCLIFOR) JOIN DBA.ORCAMENTO_PROD AS ORCAMENTO_PROD ON
                        (ORCAMENTO.IDEMPRESA = ORCAMENTO_PROD.IDEMPRESA AND
                        ORCAMENTO.IDORCAMENTO = ORCAMENTO_PROD.IDORCAMENTO) JOIN DBA.PRODUTOS_VIEW AS PRODUTOS_VIEW ON
                        (ORCAMENTO_PROD.IDPRODUTO = PRODUTOS_VIEW.IDPRODUTO AND
                        ORCAMENTO_PROD.IDSUBPRODUTO = PRODUTOS_VIEW.IDSUBPRODUTO)
                                        JOIN DBA.CIDADES_IBGE AS CI ON
                        (CI.IDCIDADE = ORCAMENTO.IDCIDADE)
                                        JOIN DBA.MARCA AS MARCA ON (MARCA.DESCRICAO = PRODUTOS_VIEW.FABRICANTE)
                WHERE
                        ORCAMENTO.FLAGPRENOTAPAGA = 'T' AND
        --        ORCAMENTO_PROD.IDVENDEDOR IN (:RA_IDVENDEDOR)  AND
                        DATE(ORCAMENTO.DTMOVIMENTO) >= current date-365   AND
        --       ORCAMENTO_PROD.IDORCAMENTO IN (:RA_IDORCAMENTO) AND
        --        ORCAMENTO_PROD.IDEMPRESA IN (:RA_IDEMPRESA) AND
                        NOT EXISTS
                        (
                        SELECT
                                1
                        FROM
                                DBA.CLIENTE_FORNECEDOR AS CF,
                                DBA.EMPRESA AS EMP
                        WHERE
                                CF.CNPJCPF = EMP.CNPJ AND
                                CF.IDCLIFOR = ORCAMENTO.IDCLIFOR AND
                                EMP.IDEMPRESA IN (SELECT DISTINCT IDEMPRESAENTRADA FROM DBA.CONFIG_TRANSF_NOTA)
                        )

                UNION ALL

                SELECT DISTINCT
                        'D' AS TIPO,
                        ORCAMENTO_PROD.IDEMPRESA,
                        EMPRESA.NOMEFANTASIA,
                        ORCAMENTO.IDCLIFOR,
                        CLIENTE_FORNECEDOR.NOME,
                                        CI.DESCRCIDADE,
                        CI.UF,
                        DATE(ESTOQUE_ANALITICO.DTMOVIMENTO) AS DATA,
                        ORCAMENTO_PROD.IDORCAMENTO,
                        ORCAMENTO_PROD.IDVENDEDOR,
                        ORCAMENTO_PROD.IDPRODUTO,
                        ORCAMENTO_PROD.IDSUBPRODUTO,
                        PRODUTOS_VIEW.FABRICANTE AS MARCA,
                        ORCAMENTO_PROD.NUMSEQUENCIA,
                        PRODUTOS_VIEW.DESCRICAOPRODUTO AS DESCRICAOPRODUTO,
                        0 AS VALOR_VENDA,
                        0 AS VALFRETE_VENDA,
                        COALESCE(NOTAS_DEVOLUCAO.VALTOTLIQUIDO,0) AS VALOR_DEV,
        -- INICIO AJUSTE POR SERGIO JUNIOR [29/06/2020]
                        CASE WHEN COALESCE(ESTOQUE_ANALITICO.VALFRETE,0) = 0 THEN
                                        (COALESCE(ORCAMENTO_PROD.VALFRETE,0) / ORCAMENTO_PROD.QTDPRODUTO)* ESTOQUE_ANALITICO.QTDPRODUTO
                        ELSE
                                        COALESCE(ESTOQUE_ANALITICO.VALFRETE,0)
                        END AS VALFRETE_DEV,
        -- FIM AJUSTE POR SERGIO JUNIOR [29/06/2020]
                        0 AS VALOR_CAN
                FROM
                        DBA.ESTOQUE_ANALITICO AS ESTOQUE_ANALITICO JOIN DBA.NOTAS_DEVOLUCAO AS NOTAS_DEVOLUCAO ON
                        (NOTAS_DEVOLUCAO.IDEMPRESA = ESTOQUE_ANALITICO.IDEMPRESA AND
                        NOTAS_DEVOLUCAO.IDPLANILHA = ESTOQUE_ANALITICO.IDPLANILHA AND
                        NOTAS_DEVOLUCAO.IDPRODUTO = ESTOQUE_ANALITICO.IDPRODUTO AND
                        NOTAS_DEVOLUCAO.IDSUBPRODUTO = ESTOQUE_ANALITICO.IDSUBPRODUTO AND
                        NOTAS_DEVOLUCAO.NUMSEQUENCIADEVOLUCAO = ESTOQUE_ANALITICO.NUMSEQUENCIA) JOIN DBA.DEVOLUCAO_LOGISTICA_MOVIMENTO AS DEVOLUCAO_LOGISTICA_MOVIMENTO ON
                        (DEVOLUCAO_LOGISTICA_MOVIMENTO.IDEMPRESA = NOTAS_DEVOLUCAO.IDEMPRESA AND
                        DEVOLUCAO_LOGISTICA_MOVIMENTO.IDPLANILHA = NOTAS_DEVOLUCAO.IDPLANILHA AND
                        DEVOLUCAO_LOGISTICA_MOVIMENTO.NUMSEQUENCIALANC = NOTAS_DEVOLUCAO.NUMSEQUENCIADEVOLUCAO AND
                        DEVOLUCAO_LOGISTICA_MOVIMENTO.IDPRODUTO = NOTAS_DEVOLUCAO.IDPRODUTO AND
                        DEVOLUCAO_LOGISTICA_MOVIMENTO.IDSUBPRODUTO = NOTAS_DEVOLUCAO.IDSUBPRODUTO AND
                        DEVOLUCAO_LOGISTICA_MOVIMENTO.FLAGGERARREENTREGA = 'F') JOIN DBA.ORCAMENTO_PRE_NOTA AS ORCAMENTO_PRE_NOTA ON
                        (ORCAMENTO_PRE_NOTA.IDEMPRESAPRENOTA = DEVOLUCAO_LOGISTICA_MOVIMENTO.IDEMPRESA AND
                        ORCAMENTO_PRE_NOTA.IDPLANILHAPRENOTA = DEVOLUCAO_LOGISTICA_MOVIMENTO.IDPLANILHADOCORI) JOIN DBA.ORCAMENTO_PROD_NOTA AS ORCAMENTO_PROD_NOTA ON
                        (ORCAMENTO_PRE_NOTA.IDEMPRESAORCAMENTO = ORCAMENTO_PROD_NOTA.IDEMPRESAORCAMENTO AND
                        ORCAMENTO_PRE_NOTA.IDORCAMENTO = ORCAMENTO_PROD_NOTA.IDORCAMENTO AND
                        ORCAMENTO_PROD_NOTA.IDPRODUTO = DEVOLUCAO_LOGISTICA_MOVIMENTO.IDPRODUTO AND
                        ORCAMENTO_PROD_NOTA.IDSUBPRODUTO = DEVOLUCAO_LOGISTICA_MOVIMENTO.IDSUBPRODUTO AND
                        ORCAMENTO_PROD_NOTA.NUMSEQUENCIA = DEVOLUCAO_LOGISTICA_MOVIMENTO.NUMSEQUENCIADOC) JOIN DBA.ORCAMENTO_PROD AS ORCAMENTO_PROD ON
                        (ORCAMENTO_PROD.IDEMPRESA = ORCAMENTO_PROD_NOTA.IDEMPRESAORCAMENTO AND
                        ORCAMENTO_PROD.IDORCAMENTO = ORCAMENTO_PROD_NOTA.IDORCAMENTO AND
                        ORCAMENTO_PROD.IDPRODUTO = ORCAMENTO_PROD_NOTA.IDPRODUTO AND
                        ORCAMENTO_PROD.IDSUBPRODUTO = ORCAMENTO_PROD_NOTA.IDSUBPRODUTO AND
                        ORCAMENTO_PROD.NUMSEQUENCIA = ORCAMENTO_PROD_NOTA.NUMSEQUENCIA) JOIN DBA.PRODUTOS_VIEW AS PRODUTOS_VIEW ON
                        (ORCAMENTO_PROD.IDPRODUTO = PRODUTOS_VIEW.IDPRODUTO AND
                        ORCAMENTO_PROD.IDSUBPRODUTO = PRODUTOS_VIEW.IDSUBPRODUTO) JOIN DBA.ORCAMENTO AS ORCAMENTO ON
                        (ORCAMENTO.IDEMPRESA = ORCAMENTO_PROD.IDEMPRESA AND
                        ORCAMENTO.IDORCAMENTO = ORCAMENTO_PROD.IDORCAMENTO) JOIN DBA.CLIENTE_FORNECEDOR AS CLIENTE_FORNECEDOR ON
                        (ORCAMENTO.IDCLIFOR = CLIENTE_FORNECEDOR.IDCLIFOR) JOIN DBA.EMPRESA AS EMPRESA ON
                        (EMPRESA.IDEMPRESA = ORCAMENTO.IDEMPRESA)
                                        JOIN DBA.CIDADES_IBGE AS CI ON
                        (CI.IDCIDADE = ORCAMENTO.IDCIDADE)
                                        JOIN DBA.MARCA AS MARCA ON (MARCA.DESCRICAO = PRODUTOS_VIEW.FABRICANTE)
                WHERE
                        DEVOLUCAO_LOGISTICA_MOVIMENTO.FLAGENTREGACANCELADA = 'F' AND
                        ORCAMENTO.FLAGPRENOTAPAGA = 'T' AND
        --        ORCAMENTO_PROD.IDVENDEDOR IN (:RA_IDVENDEDOR)  AND
                        ESTOQUE_ANALITICO.DTMOVIMENTO >= current date-365   AND
        --       ORCAMENTO_PROD.IDORCAMENTO IN (:RA_IDORCAMENTO) AND
        --        ESTOQUE_ANALITICO.IDEMPRESA IN (:RA_IDEMPRESA) AND
                        NOT EXISTS
                        (
                        SELECT
                                1
                        FROM
                                DBA.CLIENTE_FORNECEDOR AS CF,
                                DBA.EMPRESA AS EMP
                        WHERE
                                CF.CNPJCPF = EMP.CNPJ AND
                                CF.IDCLIFOR = ORCAMENTO.IDCLIFOR AND
                                EMP.IDEMPRESA IN (SELECT DISTINCT IDEMPRESAENTRADA FROM DBA.CONFIG_TRANSF_NOTA)
                        )

                UNION ALL

                SELECT DISTINCT
                        'C' AS TIPO,
                        ORCAMENTO_PROD.IDEMPRESA,
                        EMPRESA.NOMEFANTASIA,
                        ORCAMENTO.IDCLIFOR,
                        CLIENTE_FORNECEDOR.NOME,
                                        CI.DESCRCIDADE,
                        CI.UF,
                        DATE(DEVOLUCAO_LOGISTICA_MOVIMENTO.DTDEVOLUCAO) AS DATA,
                        ORCAMENTO_PROD.IDORCAMENTO,
                        ORCAMENTO_PROD.IDVENDEDOR,
                        ORCAMENTO_PROD.IDPRODUTO,
                        ORCAMENTO_PROD.IDSUBPRODUTO,
                        PRODUTOS_VIEW.FABRICANTE AS MARCA,
                        ORCAMENTO_PROD.NUMSEQUENCIA,
                        PRODUTOS_VIEW.DESCRICAOPRODUTO AS DESCRICAOPRODUTO,
                        0 AS VALOR_VENDA,
                        0 AS VALFRETE_VENDA,
                        0 AS VALOR_DEV,
                        0 AS VALFRETE_DEV,
                        CASE WHEN ROUND((COALESCE(DEVOLUCAO_LOGISTICA_MOVIMENTO.QTDPRODUTO,0) * (COALESCE(ORCAMENTO_PROD.VALTOTLIQUIDO,0) / COALESCE(ORCAMENTO_PROD.QTDPRODUTO,0))),2) > 0 THEN
                                ROUND(((COALESCE(DEVOLUCAO_LOGISTICA_MOVIMENTO.QTDPRODUTO,0) * (COALESCE(ORCAMENTO_PROD.VALTOTLIQUIDO,0) / COALESCE(ORCAMENTO_PROD.QTDPRODUTO,0))) - COALESCE(ORCAMENTO_PROD.VALFRETE,0)),2)
                        ELSE
                                ROUND((COALESCE(DEVOLUCAO_LOGISTICA_MOVIMENTO.QTDPRODUTO,0) * (COALESCE(ORCAMENTO_PROD.VALTOTLIQUIDO,0) / COALESCE(ORCAMENTO_PROD.QTDPRODUTO,0))),2)
                        END AS VALOR_CAN
                FROM
                        DBA.EMPRESA AS EMPRESA JOIN DBA.ORCAMENTO AS ORCAMENTO ON
                        (EMPRESA.IDEMPRESA = ORCAMENTO.IDEMPRESA) JOIN DBA.CLIENTE_FORNECEDOR AS CLIENTE_FORNECEDOR ON
                        (ORCAMENTO.IDCLIFOR = CLIENTE_FORNECEDOR.IDCLIFOR) JOIN DBA.ORCAMENTO_PROD AS ORCAMENTO_PROD ON
                        (ORCAMENTO.IDEMPRESA = ORCAMENTO_PROD.IDEMPRESA AND
                        ORCAMENTO.IDORCAMENTO = ORCAMENTO_PROD.IDORCAMENTO) JOIN DBA.PRODUTOS_VIEW AS PRODUTOS_VIEW ON
                        (ORCAMENTO_PROD.IDPRODUTO = PRODUTOS_VIEW.IDPRODUTO AND
                        ORCAMENTO_PROD.IDSUBPRODUTO = PRODUTOS_VIEW.IDSUBPRODUTO) JOIN DBA.ORCAMENTO_PRE_NOTA AS ORCAMENTO_PRE_NOTA ON
                        (ORCAMENTO_PROD.IDEMPRESA = ORCAMENTO_PRE_NOTA.IDEMPRESAORCAMENTO AND
                        ORCAMENTO_PROD.IDORCAMENTO = ORCAMENTO_PRE_NOTA.IDORCAMENTO) JOIN DBA.DEVOLUCAO_LOGISTICA_MOVIMENTO AS DEVOLUCAO_LOGISTICA_MOVIMENTO ON
                        (ORCAMENTO_PRE_NOTA.IDEMPRESAPRENOTA = DEVOLUCAO_LOGISTICA_MOVIMENTO.IDEMPRESA AND
                        ORCAMENTO_PRE_NOTA.IDPLANILHAPRENOTA = DEVOLUCAO_LOGISTICA_MOVIMENTO.IDPLANILHADOCORI AND
                        ORCAMENTO_PROD.IDPRODUTO = DEVOLUCAO_LOGISTICA_MOVIMENTO.IDPRODUTO AND
                        ORCAMENTO_PROD.IDSUBPRODUTO = DEVOLUCAO_LOGISTICA_MOVIMENTO.IDSUBPRODUTO AND
                        ORCAMENTO_PROD.NUMSEQUENCIA = DEVOLUCAO_LOGISTICA_MOVIMENTO.NUMSEQUENCIADOC)
                                        JOIN DBA.CIDADES_IBGE AS CI ON
                        (CI.IDCIDADE = ORCAMENTO.IDCIDADE)
                                        JOIN DBA.MARCA AS MARCA ON (MARCA.DESCRICAO = PRODUTOS_VIEW.FABRICANTE)
                WHERE
                        DEVOLUCAO_LOGISTICA_MOVIMENTO.FLAGENTREGACANCELADA = 'T' AND
        --       ORCAMENTO_PROD.IDORCAMENTO IN (:RA_IDORCAMENTO) AND
                        ORCAMENTO.FLAGPRENOTAPAGA = 'T' AND
        --        ORCAMENTO_PROD.IDVENDEDOR IN (:RA_IDVENDEDOR)  AND
                        DATE(DEVOLUCAO_LOGISTICA_MOVIMENTO.DTDEVOLUCAO) >= current date-365   AND
        --        ORCAMENTO_PROD.IDEMPRESA IN (:RA_IDEMPRESA) AND
                        NOT EXISTS
                        (
                        SELECT
                                1
                        FROM
                                DBA.CLIENTE_FORNECEDOR AS CF,
                                DBA.EMPRESA AS EMP
                        WHERE
                                CF.CNPJCPF = EMP.CNPJ AND
                                CF.IDCLIFOR = ORCAMENTO.IDCLIFOR AND
                                EMP.IDEMPRESA IN (SELECT DISTINCT IDEMPRESAENTRADA FROM DBA.CONFIG_TRANSF_NOTA)
                        )
                ) AS TEMP LEFT JOIN DBA.MARGEM_CONTRIBUICAO AS MARGEM_CONTRIBUICAO ON
                (TEMP.IDEMPRESA = MARGEM_CONTRIBUICAO.IDEMPRESA AND
                TEMP.IDORCAMENTO = MARGEM_CONTRIBUICAO.IDDOCUMENTO AND
                TEMP.NUMSEQUENCIA = MARGEM_CONTRIBUICAO.NUMSEQUENCIA AND
                MARGEM_CONTRIBUICAO.TIPODOCUMENTO = 'P') LEFT JOIN DBA.CLIENTE_FORNECEDOR AS VENDEDOR ON
                (TEMP.IDVENDEDOR = VENDEDOR.IDCLIFOR) LEFT JOIN DBA.DEPARTAMENTOS AS DEPARTAMENTOS ON
                (VENDEDOR.IDDEPARTAMENTO = DEPARTAMENTOS.IDDEPARTAMENTO)
                        LEFT JOIN
                (
                SELECT
                        CLUBE_INDICADOR_ORCAMENTO.IDEMPRESA,
                        CLUBE_INDICADOR_ORCAMENTO.IDORCAMENTO,
                        CLUBE_INDICADOR_ORCAMENTO.IDCLUBE,
                        CLUBE_FIDELIZACAO.DESCRCLUBE,
                        CLUBE_INDICADOR_ORCAMENTO.IDINDICADOR,
                        INDICADOR.NOME AS NOMEINDICADOR,
                        INDICADOR.CNPJCPF AS CNPJCPF_IND,
                        INDICADOR.FONECELULAR AS FONECELULAR
                FROM
                        DBA.CLUBE_INDICADOR_ORCAMENTO AS CLUBE_INDICADOR_ORCAMENTO JOIN DBA.CLUBE_FIDELIZACAO AS CLUBE_FIDELIZACAO ON
                        (CLUBE_INDICADOR_ORCAMENTO.IDCLUBE = CLUBE_FIDELIZACAO.IDCLUBE)  JOIN DBA.CLIENTE_FORNECEDOR AS INDICADOR ON
                        (CLUBE_INDICADOR_ORCAMENTO.IDINDICADOR = INDICADOR.IDCLIFOR)
        --   WHERE
        --        CLUBE_INDICADOR_ORCAMENTO.IDEMPRESA IN (:RA_IDEMPRESA) AND
        --       CLUBE_INDICADOR_ORCAMENTO.IDORCAMENTO IN (:RA_IDORCAMENTO)
        --        CLUBE_INDICADOR_ORCAMENTO.IDINDICADOR IN (:RA_IDINDICADOR)
                ) AS CLUBE ON
                (TEMP.IDEMPRESA = CLUBE.IDEMPRESA AND
                TEMP.IDORCAMENTO = CLUBE.IDORCAMENTO)

        WHERE
                1 = 1 and VENDEDOR.IDCLIFOR in ('{idvendedor}') and CLUBE.IDINDICADOR IS NOT NULL

GROUP BY
                CLUBE.IDINDICADOR,
                CLUBE.NOMEINDICADOR,
                CLUBE.CNPJCPF_IND,
                CLUBE.FONECELULAR,
                VENDEDOR.IDCLIFOR,
                VENDEDOR.NOME

        UNION ALL

        SELECT

                CLUBE.IDINDICADOR,
                CLUBE.NOMEINDICADOR,
                CLUBE.CNPJCPF_IND,
                CAST(count(temp.idorcamento) AS VARCHAR(10)) as qtdvendas,
                CLUBE.FONECELULAR,
                VENDEDOR.IDCLIFOR AS IDVENDEDOR,
                VENDEDOR.NOME AS NOMEVENDEDOR,
                MAX(TEMP.DATA) AS ULTIMACOMPRA,
                CAST(DAYS(CURRENT DATE) -DAYS(MAX(TEMP.DATA)) AS VARCHAR(10)) AS DIASSEMCOMPRA,
                (CASE
                        WHEN (CURRENT DATE - MAX(TEMP.DATA))BETWEEN 61 AND 90 THEN 'ATENÇÃO'
                        WHEN (CURRENT DATE - MAX(TEMP.DATA))>91 THEN 'INATIVO'
                        ELSE 'ATIVO' END) AS STATUS,
                CAST(ROUND(SUM(COALESCE(TEMP.VALOR_VENDA,0)),2) AS VARCHAR(20)) AS VALOR_VENDA_INDICADOR,
                CAST(ROUND(SUM(COALESCE(TEMP.VALFRETE_VENDA,0)),2) AS VARCHAR(20)) AS VALFRETE_VENDA_INDICADOR,
                CAST(ROUND(SUM(COALESCE(TEMP.VALOR_DEV,0)) - SUM(COALESCE(TEMP.VALFRETE_DEV,0)),2) AS VARCHAR(20)) AS VALOR_DEV_INDICADOR,
                CAST(ROUND(SUM(COALESCE(TEMP.VALOR_CAN,0)),2) AS VARCHAR(20)) AS VALOR_CAN_INDICADOR,
                CAST(ROUND(((SUM(COALESCE(TEMP.VALOR_VENDA,0)) - SUM(COALESCE(TEMP.VALFRETE_VENDA,0))) - (SUM(COALESCE(TEMP.VALOR_DEV,0)) - SUM(COALESCE(TEMP.VALFRETE_DEV,0))) - (SUM(COALESCE(TEMP.VALOR_CAN,0)))),2) AS VARCHAR(20)) AS VALORLIQUIDOVENDAINDICADOR
        FROM
                (
                SELECT
                        'VF' AS TIPO,
                        ESTOQUE_ANALITICO.IDEMPRESA,
                        EMPRESA.NOMEFANTASIA,
                        NOTAS.IDCLIFOR,
                        CLIENTE_FORNECEDOR.NOME,
                                        CI.DESCRCIDADE,
                        CI.UF,
                        DATE(ESTOQUE_ANALITICO.DTMOVIMENTO) AS DATA,
                        NOTAS.IDPLANILHA AS IDORCAMENTO,
                        ESTOQUE_ANALITICO.IDVENDEDOR,
                        ESTOQUE_ANALITICO.IDPRODUTO,
                        ESTOQUE_ANALITICO.IDSUBPRODUTO,
                        PRODUTOS_VIEW.FABRICANTE AS MARCA,
                        ESTOQUE_ANALITICO.NUMSEQUENCIA,
                        PRODUTOS_VIEW.DESCRICAOPRODUTO AS DESCRICAOPRODUTO,
                        COALESCE(ESTOQUE_ANALITICO.VALTOTLIQUIDO,0) AS VALOR_VENDA,
                        COALESCE(ESTOQUE_ANALITICO.VALFRETE,0) AS VALFRETE_VENDA,
                        0 AS VALOR_DEV,
                        0 AS VALFRETE_DEV,
                        0 AS VALOR_CAN
                FROM
                        DBA.EMPRESA AS EMPRESA JOIN DBA.NOTAS AS NOTAS ON
                        (EMPRESA.IDEMPRESA = NOTAS.IDEMPRESA) JOIN DBA.CLIENTE_FORNECEDOR AS CLIENTE_FORNECEDOR ON
                        (NOTAS.IDCLIFOR = CLIENTE_FORNECEDOR.IDCLIFOR) JOIN DBA.NOTAS_ENTRADA_SAIDA AS NOTAS_ENTRADA_SAIDA ON
                        (NOTAS.IDEMPRESA = NOTAS_ENTRADA_SAIDA.IDEMPRESA AND
                        NOTAS.IDPLANILHA = NOTAS_ENTRADA_SAIDA.IDPLANILHA) JOIN DBA.ESTOQUE_ANALITICO AS ESTOQUE_ANALITICO ON
                        (NOTAS.IDEMPRESA = ESTOQUE_ANALITICO.IDEMPRESA AND
                        NOTAS.IDPLANILHA = ESTOQUE_ANALITICO.IDPLANILHA) JOIN DBA.PRODUTOS_VIEW AS PRODUTOS_VIEW ON
                        (ESTOQUE_ANALITICO.IDPRODUTO = PRODUTOS_VIEW.IDPRODUTO AND
                        ESTOQUE_ANALITICO.IDSUBPRODUTO = PRODUTOS_VIEW.IDSUBPRODUTO) JOIN DBA.OPERACAO_INTERNA AS OPERACAO_INTERNA ON
                        (ESTOQUE_ANALITICO.IDOPERACAO = OPERACAO_INTERNA.IDOPERACAO)
                                        JOIN DBA.CIDADES_IBGE AS CI ON
                        (CI.IDCIDADE = NOTAS_ENTRADA_SAIDA.IDCIDADE)
                                        JOIN DBA.MARCA AS MARCA ON (MARCA.DESCRICAO = PRODUTOS_VIEW.FABRICANTE)
                WHERE
                        OPERACAO_INTERNA.TIPOMOVIMENTO IN ('V') AND
                        NOTAS_ENTRADA_SAIDA.TIPOCATEGORIA IN ('A') AND
                        NOTAS_ENTRADA_SAIDA.TIPOITEMCATEGORIA IN ('A2') AND
                        COALESCE(ESTOQUE_ANALITICO.NUMSEQUENCIAKIT,0) <= 0 AND
                        NOTAS.FLAGNOTACANCEL = 'F' AND
        --        ESTOQUE_ANALITICO.IDVENDEDOR IN (:RA_IDVENDEDOR)  AND
                        ESTOQUE_ANALITICO.DTMOVIMENTO >= current date-365   AND
        --       NOTAS.NUMNOTA IN (:RA_IDORCAMENTO) AND
        --        ESTOQUE_ANALITICO.IDEMPRESA IN (:RA_IDEMPRESA) AND
                        NOT EXISTS
                        (
                        SELECT
                                1
                        FROM
                                DBA.CLIENTE_FORNECEDOR AS CF,
                                DBA.EMPRESA AS EMP
                        WHERE
                                CF.CNPJCPF = EMP.CNPJ AND
                                CF.IDCLIFOR = NOTAS.IDCLIFOR AND
                                EMP.IDEMPRESA IN (SELECT DISTINCT IDEMPRESAENTRADA FROM DBA.CONFIG_TRANSF_NOTA)
                        )

                UNION ALL

                SELECT
                        'DVF' AS TIPO,
                        NOTAS_DEVOLUCAO.IDEMPRESA,
                        EMPRESA.NOMEFANTASIA,
                        NOTAS.IDCLIFOR,
                        CLIENTE_FORNECEDOR.NOME,
                                        CI.DESCRCIDADE,
                        CI.UF,
                        DATE(ESTOQUE_ANALITICO.DTMOVIMENTO) AS DATA,
                        NOTAS_DEVOLUCAO.IDPLANILHADEVOLUCAO AS IDORCAMENTO,
                        ESTOQUE_ANALITICO.IDVENDEDOR,
                        ESTOQUE_ANALITICO.IDPRODUTO,
                        ESTOQUE_ANALITICO.IDSUBPRODUTO,
                        PRODUTOS_VIEW.FABRICANTE AS MARCA,
                        NOTAS_DEVOLUCAO.NUMSEQUENCIADEVOLUCAO,
                        PRODUTOS_VIEW.DESCRICAOPRODUTO AS DESCRICAOPRODUTO,
                        0 AS VALOR_VENDA,
                        0 AS VALFRETE_VENDA,
                        COALESCE(ESTOQUE_ANALITICO.VALTOTLIQUIDO,0) AS VALOR_DEV,
                        COALESCE(ESTOQUE_ANALITICO.VALFRETE,0) AS VALFRETE_DEV,
                        0 AS VALOR_CAN
                FROM
                        DBA.EMPRESA AS EMPRESA JOIN DBA.NOTAS AS NOTAS ON
                        (EMPRESA.IDEMPRESA = NOTAS.IDEMPRESA) JOIN DBA.CLIENTE_FORNECEDOR AS CLIENTE_FORNECEDOR ON
                        (NOTAS.IDCLIFOR = CLIENTE_FORNECEDOR.IDCLIFOR) JOIN DBA.NOTAS_ENTRADA_SAIDA AS NOTAS_ENTRADA_SAIDA ON
                        (NOTAS.IDEMPRESA = NOTAS_ENTRADA_SAIDA.IDEMPRESA AND
                        NOTAS.IDPLANILHA = NOTAS_ENTRADA_SAIDA.IDPLANILHA) JOIN DBA.ESTOQUE_ANALITICO AS ESTOQUE_ANALITICO ON
                        (NOTAS.IDEMPRESA = ESTOQUE_ANALITICO.IDEMPRESA AND
                        NOTAS.IDPLANILHA = ESTOQUE_ANALITICO.IDPLANILHA) JOIN DBA.NOTAS_DEVOLUCAO AS NOTAS_DEVOLUCAO ON
                        (ESTOQUE_ANALITICO.IDEMPRESA = NOTAS_DEVOLUCAO.IDEMPRESA AND
                        ESTOQUE_ANALITICO.IDPLANILHA = NOTAS_DEVOLUCAO.IDPLANILHA AND
                        ESTOQUE_ANALITICO.NUMSEQUENCIA = NOTAS_DEVOLUCAO.NUMSEQUENCIADEVOLUCAO AND
                        ESTOQUE_ANALITICO.IDPRODUTO = NOTAS_DEVOLUCAO.IDPRODUTO AND
                        ESTOQUE_ANALITICO.IDSUBPRODUTO = NOTAS_DEVOLUCAO.IDSUBPRODUTO) JOIN DBA.PRODUTOS_VIEW AS PRODUTOS_VIEW ON
                        (ESTOQUE_ANALITICO.IDPRODUTO = PRODUTOS_VIEW.IDPRODUTO AND
                        ESTOQUE_ANALITICO.IDSUBPRODUTO = PRODUTOS_VIEW.IDSUBPRODUTO) JOIN DBA.OPERACAO_INTERNA AS OPERACAO_INTERNA ON
                        (ESTOQUE_ANALITICO.IDOPERACAO = OPERACAO_INTERNA.IDOPERACAO)
                                        JOIN DBA.CIDADES_IBGE AS CI ON
                        (CI.IDCIDADE = NOTAS_ENTRADA_SAIDA.IDCIDADE)
                                        JOIN DBA.MARCA AS MARCA ON (MARCA.DESCRICAO = PRODUTOS_VIEW.FABRICANTE)
                WHERE
                        OPERACAO_INTERNA.TIPOMOVIMENTO IN ('E') AND
                        NOTAS_ENTRADA_SAIDA.TIPOCATEGORIA IN ('D') AND
                        NOTAS_ENTRADA_SAIDA.TIPOITEMCATEGORIA IN ('D7') AND
                        COALESCE(ESTOQUE_ANALITICO.NUMSEQUENCIAKIT,0) <= 0 AND
                        NOTAS.FLAGNOTACANCEL = 'F' AND
        --        ESTOQUE_ANALITICO.IDVENDEDOR IN (:RA_IDVENDEDOR)  AND
                        ESTOQUE_ANALITICO.DTMOVIMENTO >= current date-365   AND
        --       NOTAS.NUMNOTA IN (:RA_IDORCAMENTO) AND
        --        ESTOQUE_ANALITICO.IDEMPRESA IN (:RA_IDEMPRESA) AND
                        NOT EXISTS
                        (
                        SELECT
                                1
                        FROM
                                DBA.CLIENTE_FORNECEDOR AS CF,
                                DBA.EMPRESA AS EMP
                        WHERE
                                CF.CNPJCPF = EMP.CNPJ AND
                                CF.IDCLIFOR = NOTAS.IDCLIFOR AND
                                EMP.IDEMPRESA IN (SELECT DISTINCT IDEMPRESAENTRADA FROM DBA.CONFIG_TRANSF_NOTA)
                        )
                ) AS TEMP LEFT JOIN DBA.MARGEM_CONTRIBUICAO AS MARGEM_CONTRIBUICAO ON
                (TEMP.IDEMPRESA = MARGEM_CONTRIBUICAO.IDEMPRESA AND
                TEMP.IDORCAMENTO = MARGEM_CONTRIBUICAO.IDDOCUMENTO AND
                TEMP.NUMSEQUENCIA = MARGEM_CONTRIBUICAO.NUMSEQUENCIA AND
                MARGEM_CONTRIBUICAO.TIPODOCUMENTO = 'N') LEFT JOIN DBA.CLIENTE_FORNECEDOR AS VENDEDOR ON
                (TEMP.IDVENDEDOR = VENDEDOR.IDCLIFOR) LEFT JOIN DBA.DEPARTAMENTOS AS DEPARTAMENTOS ON
                (VENDEDOR.IDDEPARTAMENTO = DEPARTAMENTOS.IDDEPARTAMENTO)
                        LEFT JOIN
                (
                SELECT
                        CLUBE_INDICADOR_ORCAMENTO.IDEMPRESA,
                        CLUBE_INDICADOR_ORCAMENTO.IDORCAMENTO,
                        CLUBE_INDICADOR_ORCAMENTO.IDCLUBE,
                        CLUBE_FIDELIZACAO.DESCRCLUBE,
                        CLUBE_INDICADOR_ORCAMENTO.IDINDICADOR,
                        INDICADOR.NOME AS NOMEINDICADOR,
                        INDICADOR.CNPJCPF AS CNPJCPF_IND,
                        INDICADOR.FONECELULAR AS FONECELULAR
                FROM
                        DBA.CLUBE_INDICADOR_ORCAMENTO AS CLUBE_INDICADOR_ORCAMENTO
                        JOIN DBA.CLUBE_FIDELIZACAO AS CLUBE_FIDELIZACAO ON
                        (CLUBE_INDICADOR_ORCAMENTO.IDCLUBE = CLUBE_FIDELIZACAO.IDCLUBE)
                        JOIN DBA.CLIENTE_FORNECEDOR AS INDICADOR ON
                        (CLUBE_INDICADOR_ORCAMENTO.IDINDICADOR = INDICADOR.IDCLIFOR)
        --   WHERE
        --        CLUBE_INDICADOR_ORCAMENTO.IDEMPRESA IN (:RA_IDEMPRESA) AND
        --       CLUBE_INDICADOR_ORCAMENTO.IDORCAMENTO IN (:RA_IDORCAMENTO)
        --        CLUBE_INDICADOR_ORCAMENTO.IDINDICADOR IN (:RA_IDINDICADOR)
                ) AS CLUBE ON
                (TEMP.IDEMPRESA = CLUBE.IDEMPRESA AND
                TEMP.IDORCAMENTO = CLUBE.IDORCAMENTO)
        WHERE
                1 = 1 and VENDEDOR.IDCLIFOR in ('{idvendedor}') and CLUBE.IDINDICADOR IS NOT NULL

        GROUP BY

                CLUBE.IDINDICADOR,
                CLUBE.NOMEINDICADOR,
                CLUBE.CNPJCPF_IND,
                CLUBE.FONECELULAR,
                VENDEDOR.IDCLIFOR,
                VENDEDOR.NOME
 ''',conexao)

    cursor.commit()
    cursor.close()

    lista =[]
    for index, row in carrega_busca_especificadorvendedor.iterrows():
        colunas = ["IDINDICADOR","NOMEINDICADOR", "FONECELULAR", "QTDVENDAS", "IDVENDEDOR", "NOMEVENDEDOR", "ULTIMACOMPRA", "DIASSEMCOMPRAS", "STATUS", "VALOR_VENDA_INDICADOR", "VALFRETE_VENDA_INDICADOR", "VALOR_DEV_INDICADOR", "VALOR_CAN_INDICADOR", "VALORLIQUIDOVENDAINDICADOR"]
        dados_especificadorvendedor_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_especificadorvendedor_ciss)
        lista.append(dados_especificadorvendedor_ciss)
    return {'data':lista}



@app.get("/orcamentoitens/{idorcamento}")
async def orcamentocabecalho_busca(idorcamento:str):
    """
    Esta chamada retorna uma lista com dados de um itens de  um determiando orçamento. O filtro é pelo idorcamento.

    Exemplo para filtrar:
    idorcamento = 3081473( este orçamento pode ter sido cancelado o efetivado. Para filtrar como teste sugiro consultar um novo idorcamento no CISS.)

    """
    #cpfcnpj:Busca_Cliente.cpfcnpj
    logging.info(f"Endpoint '/orcamentoitens/{idorcamento}' acessado com sucesso.") 
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_orcamentoitens =pd.read_sql(f''' select
                idorcamento,
                op.idproduto,
                op.idsubproduto,
                descrambiente,
                op.tipoentrega,
                numsequencia,
                op.idlocalretirada,
                lr.descrlocalretirada,
                idvendedor,
--                idlocalentrega,
                idlote,
                pv.descrcomproduto,
                cast(pv.valmultivendas as varchar(20)) as VALMULTIVENDAS,
                pv.embalagemsaida,
                LR.idlocalestoque,
                cast(valdescontopro as varchar(20)) as VALDESCONTOPRO,
                cast(perdescontopro as varchar(20)) as PERDESCONTOPRO,
                cast(vallucro as varchar(20)) as VALLUCRO,
                cast(permargemlucro as varchar(20)) as PERMARGEMLUCRO,
                CAST(qtdproduto AS VARCHAR(20)) AS QTDPRODUTO,
                CAST(valunitbruto AS VARCHAR(20)) AS VALUNITBRUTO,
                CAST(valtotliquido AS VARCHAR(20)) AS VALTOTLIQUIDO,
                CAST(valfrete AS VARCHAR (20)) AS VALFRETE
                from dba.orcamento_prod as op
                left join dba.produtos_view as pv
                on op.idproduto=pv.idproduto and op.idsubproduto=pv.idsubproduto
                left join dba.local_retirada as LR
                on lr.idlocalretirada = op.idlocalretirada
                where idorcamento =  '{idorcamento}' ''',conexao)

    cursor.commit()
    cursor.close()

    lista =[]
    for index, row in carrega_busca_orcamentoitens.iterrows():
        colunas = ["IDORCAMENTO","IDPRODUTO", "IDSUBPRODUTO", "DESCRAMBIENTE", "NUMSEQUENCIA", "IDVENDEDOR", "VALMULTIVENDAS", "EMBALAGEMSAIDA", "DESCRCOMPRODUTO",  "VALDESCONTOPRO", "PERDESCONTOPRO", "VALLUCRO", "PERMARGEMLUCRO", "TIPOENTREGA", "NUMSEQUENCIA", "IDLOCALRETIRADA", "DESCRLOCALRETIRADA", "IDLOCALESTOQUE" , "IDLOTE", "QTDPRODUTO", "VALUNITBRUTO", "VALFRETE", "VALTOTLIQUIDO"]
        dados_orcamentoitens_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_orcamentoitens_ciss)
        lista.append(dados_orcamentoitens_ciss)
    return {'data':lista}

@app.get("/produtosmaisorcados/")
async def orcamentocabecalho_busca():
    """
    Esta chamada retorna uma lista com os 20 itens mais orçados nos últimos 30 dias em todo o Grupo Elevato.

    Não é aplicado nenhum filtro.

    """
    #cpfcnpj:Busca_Cliente.cpfcnpj
    logging.info(f"Endpoint '/produtosmaisorcados/' acessado com sucesso.") 
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_orcamentoitens =pd.read_sql(f''' SELECT
        OP.IDPRODUTO,
        count(o.idorcamento) as qtdorcamento,
        CAST(SUM(OP.QTDPRODUTO) AS VARCHAR(20)) AS QTDPRODUTO,
        CAST(SUM(OP.VALTOTLIQUIDO) AS VARCHAR(20)) AS VALTOTLIQUIDO
FROM DBA.ORCAMENTO AS O
        LEFT JOIN DBA.ORCAMENTO_PROD AS OP
        ON O.IDORCAMENTO=OP.IDORCAMENTO AND O.IDEMPRESA=OP.IDEMPRESA
WHERE
        FLAGPRENOTAPAGA= 'F' AND FLAGPRENOTA = 'F' AND O.DTMOVIMENTO>= CURRENT DATE-30 
        AND OP.IDORCAMENTOORIGEM IS NULL AND IDPRODUTO IS NOT NULL AND O.FLAGCANCELADO = 'F'
GROUP BY
        OP.IDPRODUTO
ORDER BY SUM(OP.VALTOTLIQUIDO) DESC
LIMIT 20 ''',conexao)

    cursor.commit()
    cursor.close()

    lista =[]
    for index, row in carrega_busca_orcamentoitens.iterrows():
        colunas = ["IDPRODUTO",  "QTDPRODUTO",  "VALTOTLIQUIDO", "QTDORCAMENTO"]
        dados_orcamentoitens_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_orcamentoitens_ciss)
        lista.append(dados_orcamentoitens_ciss)
    return {'data':lista}

@app.get("/produtosmaisorcadosvendedor/{idvendedor}")
async def orcamentocabecalho_busca(idvendedor:str):
    """
    Esta chamada retorna uma lista com os 20 itens mais orçados de um vendedor nos últimos 30 dias.

    Exemplo para filtrar:
    idvendedor = 1137478

    """
    #cpfcnpj:Busca_Cliente.cpfcnpj
    logging.info(f"Endpoint '/produtosmaisorcados/{idvendedor}' acessado com sucesso.") 
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_orcamentoitens =pd.read_sql(f''' SELECT
        OP.IDPRODUTO,
        count(o.idorcamento) as qtdorcamento,
        CAST(SUM(OP.QTDPRODUTO) AS VARCHAR(20)) AS QTDPRODUTO,
        CAST(SUM(OP.VALTOTLIQUIDO) AS VARCHAR(20)) AS VALTOTLIQUIDO
FROM DBA.ORCAMENTO AS O
        LEFT JOIN DBA.ORCAMENTO_PROD AS OP
        ON O.IDORCAMENTO=OP.IDORCAMENTO AND O.IDEMPRESA=OP.IDEMPRESA
WHERE
        FLAGPRENOTAPAGA= 'F' AND FLAGPRENOTA = 'F' AND O.DTMOVIMENTO>= CURRENT DATE-30 
        AND OP.IDORCAMENTOORIGEM IS NULL AND IDPRODUTO IS NOT NULL AND O.FLAGCANCELADO = 'F'
        AND OP.IDVENDEDOR in ('{idvendedor}')
GROUP BY
        OP.IDPRODUTO
ORDER BY SUM(OP.VALTOTLIQUIDO) DESC
LIMIT 20 ''',conexao)

    cursor.commit()
    cursor.close()

    lista =[]
    for index, row in carrega_busca_orcamentoitens.iterrows():
        colunas = ["IDPRODUTO",  "QTDPRODUTO",  "VALTOTLIQUIDO", "QTDORCAMENTO"]
        dados_orcamentoitens_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_orcamentoitens_ciss)
        lista.append(dados_orcamentoitens_ciss)
    return {'data':lista}

@app.get("/produtosmaisorcadosdepartamento/{iddepartamento}")
async def orcamentocabecalho_busca(iddepartamento:str):
    """
    Esta chamada retorna uma lista com os 20 itens mais orçados em um departamento nos últimos 30 dias

    Exemplo para filtrar:
    iddepartamento = 4

    """
    #cpfcnpj:Busca_Cliente.cpfcnpj
    logging.info(f"Endpoint '/produtosmaisorcados/{iddepartamento}' acessado com sucesso.") 
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_orcamentoitens =pd.read_sql(f'''SELECT
        TEMP.IDPRODUTO,
        sum(temp.qtdorcamento) as qtdorcamento,
        CAST(SUM(TEMP.QTDPRODUTO) AS VARCHAR(20)) AS QTDPRODUTO,
        CAST(SUM(TEMP.VALTOTLIQUIDO) AS VARCHAR(20)) AS VALTOTLIQUIDO
FROM
(SELECT
        OP.IDPRODUTO,
        OP.IDVENDEDOR,
        O.IDEMPRESA,
        count(o.idorcamento) as qtdorcamento,
        SUM(OP.QTDPRODUTO) AS QTDPRODUTO,
        SUM(OP.VALTOTLIQUIDO) AS VALTOTLIQUIDO,
        (       SELECT
                        VDH.IDDEPARTAMENTO
                FROM
                        DBA.VENDEDOR_DEPARTAMENTO_HIST AS VDH
                WHERE
                        VDH.IDVENDEDOR = OP.IDVENDEDOR AND
                        VDH.DTCADASTRO = (      SELECT
                                                           MAX(VDH.DTCADASTRO) AS DT
                                                FROM
                                                           DBA.VENDEDOR_DEPARTAMENTO_HIST AS VDH
                                                WHERE
                                                           VDH.IDVENDEDOR = OP.IDVENDEDOR AND
                                                           VDH.DTCADASTRO <= O.DTMOVIMENTO
                                         )
        ) AS IDDEPARTAMENTO
FROM DBA.ORCAMENTO AS O
        LEFT JOIN DBA.ORCAMENTO_PROD AS OP
        ON O.IDORCAMENTO=OP.IDORCAMENTO AND O.IDEMPRESA=OP.IDEMPRESA
WHERE
        FLAGPRENOTAPAGA= 'F' AND FLAGPRENOTA = 'F' AND O.DTMOVIMENTO>= CURRENT DATE-30 --AND IDPRODUTO= 1096336
        AND OP.IDORCAMENTOORIGEM IS NULL AND IDPRODUTO IS NOT NULL AND O.FLAGCANCELADO = 'F'

GROUP BY
        OP.IDPRODUTO,
        OP.IDVENDEDOR,
        O.IDEMPRESA,
        O.DTMOVIMENTO
ORDER BY SUM(OP.VALTOTLIQUIDO) DESC
) AS TEMP
WHERE
        TEMP.IDDEPARTAMENTO= '{iddepartamento}'
GROUP BY
        TEMP.IDPRODUTO
ORDER BY SUM(TEMP.VALTOTLIQUIDO) DESC
LIMIT 20 ''',conexao)

    cursor.commit()
    cursor.close()

    lista =[]
    for index, row in carrega_busca_orcamentoitens.iterrows():
        colunas = ["IDPRODUTO",  "QTDPRODUTO",  "VALTOTLIQUIDO","QTDORCAMENTO"]
        dados_orcamentoitens_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_orcamentoitens_ciss)
        lista.append(dados_orcamentoitens_ciss)
    return {'data':lista}


@app.get("/buscaestoquelote/{idsubproduto}")
async def orcamentocabecalho_busca(idsubproduto:str):
    """
    Esta chamada retorna uma lista com os dados de Lote de um determinado produto e sua quantidade disponível.

    Exemplo para filtrar:
    idsubproduto = 1059453

    """
    #cpfcnpj:Busca_Cliente.cpfcnpj
    logging.info(f"Endpoint '/buscaestoquelote/{idsubproduto}' acessado com sucesso.") 
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")
    
    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_busca_orcamentoitens =pd.read_sql(f'''SELECT
        RESUMO.IDPRODUTO,
        RESUMO.IDSUBPRODUTO,
        RESUMO.DESCRLOTE,
        RESUMO.IDLOCALESTOQUE,
        RESUMO.DESCRLOCALESTOQUE,
        MAX(RESUMO.DTALTERACAO) AS DTALTERECAO,
        CAST(MAX(RESUMO.QTDSALDODISPONIVEL) AS VARCHAR(20)) AS QTDSALDODISPONIVEL
FROM
(SELECT
        ESTOQUE.IDPRODUTO,
        ESTOQUE.IDSUBPRODUTO,
        ESTOQUE.IDLOCALESTOQUE,
        ESTOQUE.DESCRLOCALESTOQUE,
        ESTOQUE.FLAGLOTE,
        ESTOQUE.DESCRLOTE,
        ESTOQUE.DTALTERACAO,
        ESTOQUE.QTDSALDODISPONIVEL AS QTDSALDODISPONIVEL
FROM
(SELECT
        TMP.IDEMPRESA,
        TMP.IDPRODUTO,
        TMP.IDSUBPRODUTO,
        TMP.IDLOCALESTOQUE,
        ECL.DESCRLOCAL AS DESCRLOCALESTOQUE,
        TMP.FLAGLOTE,
        TMP.IDLOTE AS DESCRLOTE,
        TMP.FLAGESTNEGATIVO,
        TMP.QTDATUALESTOQUE AS QTDSALDOATUAL,
        TMP.QTDSALDORESERVA,
        (TMP.QTDATUALESTOQUE - TMP.QTDSALDORESERVA) AS QTDSALDODISPONIVEL,
        TMP.DTALTERACAO,
        CEP.IDCONFIGECOMMERCE,
        CEP.DESCRICAO AS DESCRICAOECOMMERCE,
        CEP.TIPODADO AS TIPODADOECOMMERCE,
        COALESCE (PGE.DESCRICAO,'') AS FLAGECOMMERCE,
        PV.VALMULTIVENDAS AS MULTIVENDAS
    FROM
        (
        SELECT
            COALESCE((
                CASE WHEN PG.FLAGLOTE = 'T' THEN
                    ESL.QTDATUALESTOQUE
                ELSE
                    ESA.QTDATUALESTOQUE
                END),0) AS QTDATUALESTOQUE,
            COALESCE((
                CASE WHEN CG.FLAGATIVARESERVA = 'T' OR CG.FLAGINATIVARESERVAVENDAFUTURA = 'T' OR CG.FLAGATIVARESERVAPRENOTA = 'T' OR CG.FLAGATIVARESERVAORCAMENTO = 'T' THEN
                    (SELECT
                        SUM(EAT.QTDPRODUTO)
                    FROM
                        DBA.ESTOQUE_ANALITICO_TMP EAT
                    LEFT OUTER JOIN DBA.ORCAMENTO O ON (
                       EAT.NUMPEDIDO = O.IDORCAMENTO AND
                        EAT.IDEMPRESA = O.IDEMPRESA
                        )
                    WHERE
                        EAT.IDPRODUTO = PG.IDPRODUTO AND (
                            (EAT.IDSUBPRODUTO = PG.IDSUBPRODUTO AND PROD.TIPOBAIXAMESTRE = 'I') OR (PROD.TIPOBAIXAMESTRE <> 'I')
                        ) AND (
                            (COALESCE(PG.FLAGLOTE,'F') <> 'T' AND EAT.IDEMPRESABAIXAEST = ESA.IDEMPRESA AND EAT.IDLOCALESTOQUE = ESA.IDLOCALESTOQUE) OR
                            (COALESCE(PG.FLAGLOTE,'F') = 'T' AND EAT.IDEMPRESABAIXAEST = ESL.IDEMPRESA AND EAT.IDLOCALESTOQUE = ESL.IDLOCALESTOQUE AND EAT.IDLOTE = ESL.IDLOTE)
                        )
                        AND
                        (
                         EAT.TIPODOCUMENTO NOT IN('X','O','N','P','U','C','M')
                         OR (
                            EAT.TIPODOCUMENTO IN('X','O') AND
                            (
                                (
                                    COALESCE(O.DTVALIDADE,date('1900-01-01')) >= DBA.TODAY() OR O.FLAGPRENOTAPAGA = 'T'
                                ) OR (
                                    CG.FLAGATIVARESERVA = 'T' AND
                                    CG.FLAGATIVARESERVAORCAMENTO = 'T' AND
                                    EAT.TIPODOCUMENTO = 'O' AND
                                    COALESCE(EAT.TIPOENTREGA,'') NOT IN ('A','E') AND
                                    EAT.NUMPEDIDO IS NULL
                                )
                            )
                        )
                        OR (
                            CG.FLAGATIVARESERVA = 'T' AND
                            CG.FLAGATIVARESERVAPRENOTA = 'T' AND
                            EAT.TIPODOCUMENTO = 'X' AND
                            COALESCE(EAT.TIPOENTREGA,'') NOT IN ('A','E') AND
                            EAT.NUMPEDIDO IS NULL
                        )
                        OR (
                            CG.FLAGATIVARESERVA = 'T' AND
                            EAT.TIPODOCUMENTO IN ('N','P','U','C')
                        )
                        OR (
                            CG.FLAGINATIVARESERVAVENDAFUTURA = 'T' AND
                            EAT.TIPODOCUMENTO = 'M'
                        )
                    )
                )
                ELSE
                    CAST(0 AS DECIMAL(15,3))
                END),0) AS QTDSALDORESERVA,
            CASE WHEN PG.FLAGLOTE = 'T' THEN
                ESL.IDLOCALESTOQUE
            ELSE
                ESA.IDLOCALESTOQUE
            END AS IDLOCALESTOQUE,
            CASE WHEN PG.FLAGLOTE = 'T' THEN
                ESL.IDEMPRESA
            ELSE
                ESA.IDEMPRESA
            END AS IDEMPRESA,
            PG.IDPRODUTO,
            PG.IDSUBPRODUTO,
            PG.FLAGESTNEGATIVO,
            COALESCE(PG.FLAGLOTE,'F') AS FLAGLOTE,
            ESL.IDLOTE,
            CASE WHEN PG.FLAGLOTE = 'T' THEN
                COALESCE(ESA.DTALTERACAO, TIMESTAMP(ESL.DTMOVIMENTO))
            ELSE
                COALESCE(ESA.DTALTERACAO, TIMESTAMP(ESA.DTMOVIMENTO))
            END AS DTALTERACAO
        FROM
            DBA.CONFIG_GERAL CG,
            DBA.PRODUTO_GRADE PG
        INNER JOIN
            DBA.PRODUTO PROD ON (
                PG.IDPRODUTO = PROD.IDPRODUTO
            )
        LEFT OUTER JOIN
            (SELECT
                E.IDPRODUTO,
                E.IDSUBPRODUTO,
                E.IDLOCALESTOQUE,
                E.IDEMPRESA,
                E.QTDATUALESTOQUE,
                E.DTMOVIMENTO,
                E.DTALTERACAO
            FROM
                DBA.ESTOQUE_SALDO_ATUAL E
            WHERE
                E.IDEMPRESA IN ('13','26','1') AND
                E.IDLOCALESTOQUE IN ('6','124','42')
                --AND
-- (IN_IDEMPRESA = 0 OR E.IDEMPRESA = IN_IDEMPRESA) AND
-- (IN_IDLOCALESTOQUE = 0 OR E.IDLOCALESTOQUE = IN_IDLOCALESTOQUE) AND
-- (IN_IDPRODUTO = 0 OR E.IDPRODUTO = IN_IDPRODUTO) AND
-- (IN_IDSUBPRODUTO = 0 OR E.IDSUBPRODUTO = IN_IDSUBPRODUTO)
                AND (
                    SELECT
                        COUNT(0)
                    FROM
                        DBA.PRODUTO_GRADE P
                    WHERE
                        P.IDSUBPRODUTO = E.IDSUBPRODUTO AND
                        P.FLAGLOTE = 'T'
                ) = 0
            ) ESA ON (
                PG.IDPRODUTO = ESA.IDPRODUTO AND
                PG.IDSUBPRODUTO = ESA.IDSUBPRODUTO
                )
        LEFT OUTER JOIN
            (SELECT
                ESLO.IDLOTE,
                ESLO.IDEMPRESA,
                ESLO.IDPRODUTO,
                ESLO.IDSUBPRODUTO,
                ESLO.IDLOCALESTOQUE,
                ESLO.DTMOVIMENTO,
                ESLO.QTDATUALESTOQUE
            FROM
                DBA.ESTOQUE_SINTETICO_LOTE ESLO
            WHERE
-- (IN_IDEMPRESA = 0 OR ESLO.IDEMPRESA = IN_IDEMPRESA) AND
                ESLO.IDEMPRESA IN ('13','26') AND
                ESLO.IDLOCALESTOQUE IN ('6','124')
                --AND
-- (IN_IDLOCALESTOQUE = 0 OR ESLO.IDLOCALESTOQUE = IN_IDLOCALESTOQUE) AND
-- (IN_IDPRODUTO = 0 OR ESLO.IDPRODUTO = IN_IDPRODUTO) AND
-- (IN_IDSUBPRODUTO = 0 OR ESLO.IDSUBPRODUTO = IN_IDSUBPRODUTO)
                AND (
                    SELECT
                        COUNT(0)
                    FROM
                        DBA.PRODUTO_GRADE P
                    WHERE
                        P.IDSUBPRODUTO = ESLO.IDSUBPRODUTO AND
                        P.FLAGLOTE = 'T'
                ) > 0
                AND ESLO.DTMOVIMENTO = (
                    SELECT
                       MAX(TMP.DTMOVIMENTO)
                    FROM
                        DBA.ESTOQUE_SINTETICO_LOTE AS TMP
                    WHERE
                        TMP.IDPRODUTO = ESLO.IDPRODUTO AND
                        TMP.IDSUBPRODUTO = ESLO.IDSUBPRODUTO AND
                        TMP.IDEMPRESA = ESLO.IDEMPRESA AND
                        TMP.IDLOTE = ESLO.IDLOTE AND
                        TMP.IDLOCALESTOQUE = ESLO.IDLOCALESTOQUE
                )
            ) ESL ON (
                PG.IDPRODUTO = ESL.IDPRODUTO AND
                PG.IDSUBPRODUTO = ESL.IDSUBPRODUTO
            )
WHERE
        PG.IDSUBPRODUTO={idsubproduto}
-- (IN_IDPRODUTO = 0 OR PG.IDPRODUTO = IN_IDPRODUTO ) AND
-- (IN_IDSUBPRODUTO = 0 OR PG.IDSUBPRODUTO = IN_IDSUBPRODUTO)
        ) AS TMP
        INNER JOIN DBA.ESTOQUE_CADASTRO_LOCAL AS ECL ON(
            ECL.IDLOCALESTOQUE = TMP.IDLOCALESTOQUE
        )
        LEFT JOIN DBA.PRODUTO_GRADE_ECOMMERCE AS PGE ON(
            PGE.IDPRODUTO = TMP.IDPRODUTO AND
            PGE.IDSUBPRODUTO = TMP.IDSUBPRODUTO
        )
                LEFT JOIN DBA.CONFIG_ECOMMERCE_PROPRIEDADES AS CEP ON(
            CEP.IDCONFIGECOMMERCE = PGE.IDCONFIGECOMMERCE
        )
                LEFT JOIN DBA.PRODUTOS_VIEW AS PV ON (
            PV.IDPRODUTO = TMP.IDPRODUTO AND
            PV.IDSUBPRODUTO = TMP.IDSUBPRODUTO
        )
    WHERE
-- (IN_IDEMPRESA IN (13,26)) AND
-- (IN_IDEMPRESA = 0 OR TMP.IDEMPRESA = IN_IDEMPRESA) AND
        ECL.FLAGDISPONVENDA = 'T') AS ESTOQUE
-- WHERE
--         ESTOQUE.FLAGECOMMERCE='T'
GROUP BY
        ESTOQUE.IDPRODUTO,
        ESTOQUE.IDSUBPRODUTO,
        ESTOQUE.FLAGLOTE,
        ESTOQUE.DESCRLOTE,
        ESTOQUE.QTDSALDODISPONIVEL,
        ESTOQUE.MULTIVENDAS,
        ESTOQUE.DTALTERACAO,
        ESTOQUE.IDLOCALESTOQUE,
        ESTOQUE.DESCRLOCALESTOQUE ) AS RESUMO
WHERE RESUMO.QTDSALDODISPONIVEL>0
GROUP BY
        RESUMO.IDPRODUTO,
        RESUMO.IDSUBPRODUTO,
        RESUMO.DESCRLOTE,
        RESUMO.IDLOCALESTOQUE,
        RESUMO.DESCRLOCALESTOQUE
''',conexao)

    cursor.commit()
    cursor.close()

    lista =[]
    for index, row in carrega_busca_orcamentoitens.iterrows():
        colunas = ["IDPRODUTO", "IDSUBPRODUTO", "DESCRLOTE", "IDLOCALESTOQUE", "DESCRLOCALESTOQUE", "QTDSALDODISPONIVEL"]
        dados_orcamentoitens_ciss = {coluna: row[coluna] for coluna in colunas}
        #dados_estoque_ciss_inicio= float(dados_estoque_ciss['QTDDISPONIVEL'])
        #dados_cliente_ciss = json.dumps(dados_cliente_ciss_inicio, sort_keys=True, indent=4)
        print(dados_orcamentoitens_ciss)
        lista.append(dados_orcamentoitens_ciss)
    return {'data':lista}

@app.post('/clientes/')
async def create_cliente(cliente:Cliente):
    cpfcnpj:Cliente.cpfcnpj
    nome:Cliente.nome
    rua:Cliente.rua 
    numero:Cliente.numero 
    complemento:Cliente.complemento
    bairro:Cliente.bairro
    cep:Cliente.cep 
    #idcliente:Cliente.idcliente
    fonecelular:Cliente.fonecelular
    email:Cliente.email
    idusuariocadastro:Cliente.idusuariocadastro
    obsgeral:Cliente.obsgeral
    inscestadual:Cliente.inscestadual
    dtnascimento:Cliente.dtnascimento
    nomefantasia:Cliente.nomefantasia


    logging.info(f"Endpoint '/clientes/{cliente.cpfcnpj}&{cliente.nome}&{cliente.cep}&{cliente.rua}&{cliente.bairro}&{cliente.complemento}&{cliente.email}&{cliente.fonecelular}&{cliente.dtnascimento}&{cliente.inscestadual}&{cliente.nomefantasia}&{cliente.numero}' acessado com sucesso.") 
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")

    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()
    #conexao1 = pyodbc.connect(dados_conexao_pg)
    carrega_grava_clientes = pd.read_sql(f'''SELECT
           cast(idcliente as varchar(20)) as idcliente
        FROM
        TABLE(  DBA.UF_ADD_CLIENTE_BUBBLE(
                                    '{cliente.cpfcnpj}',
                                    '{cliente.nome}',
                                    '{cliente.rua}',
                                    '{cliente.numero}',
                                    '{cliente.complemento}',
                                    '{cliente.bairro}',
                                    '{cliente.cep}',
                                    '{cliente.fonecelular}',
                                    '{cliente.email}',
                                    '{cliente.idusuariocadastro}',
                                    '{cliente.obsgeral}',
                                    '{cliente.inscestadual}',
                                    '{cliente.dtnascimento}',
                                    '{cliente.nomefantasia}'

                )
        ) AS TBL''',conexao)
    cursor.commit()
    cursor.close()

    #print(carrega_grava_clientes)
    for index, row in carrega_grava_clientes.iterrows():
        colunas = ["IDCLIENTE"]
        idclienteciss = {row[coluna] for coluna in colunas}
        print(idclienteciss)
        for idciss in idclienteciss:
            print(idciss)
        #Cliente.idcliente:idciss
        #print(Cliente.idcliente)
    #idcliente:"100"
    #idciss1 = int(idciss)
   # print(idciss)
   # idcliente:idciss
    #Cliente.idcliente:idciss
   # print(cliente.idcliente)

    # return carrega_grava_clientes.idcliente   
    return idclienteciss

@app.post('/pedido/')
async def create_pedido(pedido:Pedido):
    idempresa:Pedido.idempresa
    idclifor:Pedido.idclifor
    logging.info(f"Endpoint '/pedido/{pedido.idempresa}&{pedido.idclifor}' acessado com sucesso.") 
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"

                "PWD=BUQ8~5x?mOPLI18")

    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_grava_pedido = pd.read_sql(f'''SELECT
           cast(idpedido as varchar(20)) as idpedido
        FROM
        TABLE(  BIELEVATO.SET_PEDIDO_BUBBLE(
                                    '{pedido.idempresa}',
                                    '{pedido.idclifor}'
                )
        ) AS TBL''',conexao)

    cursor.commit()
    cursor.close()

    #print(carrega_grava_clientes)
    for index, row in carrega_grava_pedido.iterrows():
        colunas = ["IDPEDIDO"]
        idpedidociss = {row[coluna] for coluna in colunas}
        print(idpedidociss)
        for idciss in idpedidociss:
            print(idciss)

    return idpedidociss


@app.post('/pedidoconsumidor/')
async def create_pedidoconsumidor(pedidoconsumidor:PedidoConsumidor):
    idempresa:PedidoConsumidor.idempresa
    idclifor:PedidoConsumidor.idclifor
    nome:PedidoConsumidor.nome
    logging.info(f"Endpoint '/pedidoconsumidor/{pedidoconsumidor.idempresa}&{pedidoconsumidor.idclifor}&{pedidoconsumidor.nome}' acessado com sucesso.") 
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")

    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_grava_pedido = pd.read_sql(f'''SELECT
           cast(idpedido as varchar(20)) as idpedido
        FROM
        TABLE(  BIELEVATO.SET_PEDIDO_BUBBLE_CONSUMIDOR(
                                    '{pedidoconsumidor.idempresa}',
                                    '{pedidoconsumidor.idclifor}',
                                    '{pedidoconsumidor.nome}'
                )
        ) AS TBL''',conexao)

    cursor.commit()
    cursor.close()

    #print(carrega_grava_clientes)
    for index, row in carrega_grava_pedido.iterrows():
        colunas = ["IDPEDIDO"]
        idpedidociss = {row[coluna] for coluna in colunas}
        print(idpedidociss)
        for idciss in idpedidociss:
            print(idciss)

    return idpedidociss


@app.post('/orcamentocancelamento/')
async def create_orcamento(orcamentocancelamento:OrcamentoPerdido):
    idorcamento:OrcamentoPerdido.idorcamento
    logging.info(f"Endpoint '/orcamentocancelamento/{orcamentocancelamento.idorcamento}' acessado com sucesso.") 
    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")

    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_cancela_orcamento = pd.read_sql(f'''SELECT
           STATUS 
        FROM
        TABLE(  BIELEVATO.CANCELA_ORCAMENTO_BUBBLE(
                                    '{orcamentocancelamento.idorcamento}'
                )
        ) AS TBL''',conexao)

    cursor.commit()
    cursor.close()

    #print(carrega_grava_clientes)
    for index, row in carrega_cancela_orcamento.iterrows():
        colunas = ["STATUS"]
        status = {row[coluna] for coluna in colunas}
        print(status)
        for tstatus in status:
            print(tstatus)

    return status


@app.post('/itempedido/')
async def create_item_pedido(itempedido:ItemPedido):
    idpedido:ItemPedido.idpedido
    idempresa:ItemPedido.idempresa
    idvendedor:ItemPedido.idvendedor
    idproduto:ItemPedido.idproduto
    idsubproduto:ItemPedido.idsubproduto
    numsequencia:ItemPedido.numsequencia
    idlote:ItemPedido.idlote
    qtdproduto:ItemPedido.qtdproduto
    valunitbruto:ItemPedido.valunitbruto
    valtotliquido:ItemPedido.valtotliquido
    valdescontopro:ItemPedido.valdescontopro
    ambiente:ItemPedido.ambiente
    idlocalestoque:ItemPedido.idlocalestoque
    
    logging.info(f"Endpoint '/pedido/{itempedido.idempresa}&{itempedido.ambiente}&{itempedido.idpedido}&{itempedido.idvendedor}&{itempedido.idproduto}&{itempedido.idsubproduto}&{itempedido.qtdproduto}&{itempedido.valtotliquido}&{itempedido.idproduto}&{itempedido.numsequencia}&{itempedido.valdescontopro}&{itempedido.valunitbruto}' acessado com sucesso.") 
    print(itempedido.valtotliquido)
    print(itempedido.valunitbruto)
    print(itempedido.qtdproduto)
    print(itempedido.idlote)

    pyodbc.drivers()

    dados_conexao_db2= ( "DRIVER={IBM DB2 ODBC DRIVER - DB2COPY1};"
                "HOSTNAME=acesso.elevato.com.br;"
                    "PORT=50000;"
                    "PROTOCOL=TCPIP;"
                    "DATABASE=CISSERP;"
                "UID=bielevato;"
                "PWD=BUQ8~5x?mOPLI18")

    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_grava_item_pedido = pd.read_sql(f'''SELECT
           cast(statusitenspedido as varchar(10)) as statusitenspedido
        FROM
        TABLE(  BIELEVATO.SET_ITEM_PEDIDO_BUBBLE_3(
                                    '{itempedido.idpedido}',
                                    '{itempedido.idempresa}',
                                    '{itempedido.idvendedor}',
                                    '{itempedido.idproduto}',
                                    '{itempedido.idsubproduto}',
                                    '{itempedido.numsequencia}',
                                    '{itempedido.qtdproduto}',
                                    '{itempedido.valunitbruto}',
                                    '{itempedido.valtotliquido}',
                                    '{itempedido.valdescontopro}',
                                    '{itempedido.ambiente}',
                                    '{itempedido.idlote}',
                                    '{itempedido.idlocalestoque}'
                )
        ) AS TBL''',conexao)
    logging.info(f"Endpoint '/pedido/{itempedido.idpedido}&{itempedido.idvendedor}&{itempedido.idproduto}' passou no carregaitem e gravou com com sucesso.") 
    cursor.commit()
    cursor.close()

    #print(carrega_grava_clientes)
    for index, row in carrega_grava_item_pedido.iterrows():
        colunas = ["STATUSITENSPEDIDO"]
        idstatusciss = {row[coluna] for coluna in colunas}
        print(idstatusciss)
        for idciss in idstatusciss:
            print(idciss)

    return idstatusciss



# @app.post("/convertimg/")
# async def convert_image(url: str):
#     # Obter a imagem da URL
#     try:
#         headers = {"accept": "application/json"}
#         response = requests.get(url, headers=headers)
#         response.raise_for_status()
#     except requests.exceptions.RequestException as e:
#         raise HTTPException(status_code=400, detail="Não foi possível obter a imagem da URL fornecida") from e
    
#     # Converter a imagem WEBP para PNG usando o Pillow
#     with Image.open(BytesIO(response.content)) as im:
#         output_buffer = BytesIO()
#         im.save(output_buffer, format='PNG')
#         png_image = await url.read() #output_buffer.getvalue()

#     # Retornar a imagem PNG convertida
#     return StreamingResponse(BytesIO(png_image), media_type="image/png")



# async def clientes(
#     as_cpfcnpj:str = Form(),
#     as_nome:str = Form(),
#     as_rua:str = Form(),
#     as_numero:int = Form(),
#     as_complemento:str = Form(),
#     as_bairro:str = Form(),
#     as_cep:str = Form()):
#    as_cpfcnpj = Cliente.cpfcnpj,
#    as_nome = Cliente.nome,
#    as_rua = Cliente.rua,
#    as_numero = Cliente.numero,
#    as_complemento = Cliente.complemento,
#    as_bairro = Cliente.bairro,
#    as_cep = Cliente.cep
#    as_ruacobranca:str = Form(),
#    as_numerocobranca:str = Form(),
#    as_complementocobranca:str = Form(),
#    as_bairrocobranca:str = Form(),
#    as_cepcobranca:str = Form(),
#    as_fone:str = Form(),
#    as_fonecelular:str = Form(),
#    as_email:str = Form(),
#    as_idusuariocadastro:int = Form(),
#    as_obsgeral:str = Form(),
#    as_progpontuacao:int = Form(),
#    as_inscestadual:str = Form(),
#    ai_idatividade:int = Form(),
#    ac_tiporegimetribfederal:int = Form(),
#    ac_tiporegimetributacao:int = Form(),
#    ac_tipocadastro:str = Form(),
#    ai_redenegocio:int = Form(),
#    ac_tiposexo:str = Form(),
#    as_flagnaoenviadadoscliforxml:str = Form(),
#    ad_dtnascimento:str = Form(),
#    as_nomefantasia:str = Form(),
#    ai_idconvenio:int = Form(),
#   ai_idpagamento:int = Form()):







#app = FastAPI()


# class ExampleItem(BaseModel):
#     name: str = Field(description="O nome do item.")
#     value: int = Field(description="O valor do item.")

# class ExampleResponse(BaseModel):
#     success: bool = Field(description="Indica se a operação foi bem-sucedida.")
#     message: str = Field(description="Mensagem explicando o resultado da operação.")
#     data: List[ExampleItem] = Field(description="Lista de objetos ExampleItem.")

# @app.get("/example_list_with_model", response_model=ExampleResponse, response_model_exclude_unset=True)
# async def get_example_list_with_model():
#     """
#     Exemplo com documentação e modelo.

#     Retorna uma lista de objetos Pydantic ExampleItem.

#     ### Response

#     - `success` (bool): Indica se a operação foi bem-sucedida.
#     - `message` (str): Mensagem explicando o resultado da operação.
#     - `data` (list[ExampleItem]): Lista de objetos ExampleItem.

#         - `name` (str): O nome do item.
#         - `value` (int): O valor do item.
#     """
#     return {"success": True, "message": "Dados retornados com sucesso", "data": [{"name": "example_name_1", "value": 1}, {"name": "example_name_2", "value": 2}]}