from fastapi import FastAPI
from typing import List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import pyodbc
import logging
from fastapi.responses import StreamingResponse
from typing import List
from fastapi import FastAPI
from pydantic import BaseModel


app = FastAPI()

dados_conexao_db2 = (
    "DRIVER={IBM DB2 ODBC DRIVER};"
    "HOSTNAME=acesso.elevato.com.br;"
    "PORT=50000;"
    "PROTOCOL=TCPIP;"
    "DATABASE=CISSERP;"
    "UID=bielevato;"
    "PWD=BUQ8~5x?mOPLI18"
)

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


# classe utilizada para gravação de cliente no CISS#
# novos campos devem ser adicionados nela#
class Cliente(BaseModel):
    cpfcnpj: str
    nome: str
    rua: str
    numero: int
    complemento: str
    bairro: str
    cep: str
    # idcliente:str
    fonecelular: str
    email: str
    idusuariocadastro: str
    obsgeral: str
    inscestadual: str
    dtnascimento: str
    nomefantasia: str


# Classe utilizada para gravar pedido no ciss---novos campos devem ser
# adicionados nela.
class Pedido(BaseModel):
    idempresa: str
    idclifor: str


class PedidoConsumidor(BaseModel):
    idempresa: str
    idclifor: str
    nome: str


# classe utilizada para dar orcamento como perdido
class OrcamentoPerdido(BaseModel):
    idorcamento: str


# classe utilizada para gravar itens do pedido no CISS--Novos Campos adicionar nela
class ItemPedido(BaseModel):
    idpedido: str
    idempresa: str
    idvendedor: str
    idproduto: str
    idsubproduto: str
    numsequencia: str
    idlote: str
    qtdproduto: float
    valunitbruto: float
    valtotliquido: float
    valdescontopro: float
    ambiente: str
    idlocalestoque: str


# class Busca_Cliente(BaseModel):
#     cpfcnpj:str


async def execute_query(file_path: str, replace_dict: dict):
    logging.info(f"Running query from '{file_path}'")
    pyodbc.drivers()

    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    query = open(file_path, "r").read()
    for key, value in replace_dict.items():
        query = query.replace(key, value)

    df = pd.read_sql(query, conexao)

    cursor.commit()
    cursor.close()

    return df


async def process_results(df: pd.DataFrame, columns: List[str], return_with_data: bool = False):
    data = []
    for _, row in df.iterrows():
        data_dict = {col: row[col] for col in columns}
        data.append(data_dict)

    print(data)

    if data:
        if return_with_data:
            return {"data": data}
        else:
            return data[0]
    else:
        raise HTTPException(status_code=404, detail="Data not found")


# busca cliente filtrando por cpfcnpj,descricao e telefone
@app.get("/cliente/{cpfcnpj}")
async def cliente_busca(cpfcnpj: str):
    """
    Esta chamada retorna dados de cadastro de cliente. Use o CPF 00881535923 para simular os dados que retornam.
    É passado um paramêtro chamado cpfcnpj que é utilizado para filtrar o cpfcnpj e número de telefone.
    Em cada execução ele retorna um único registro de cliente. Os campos que retornam serão apresentados na simulação.
    """

    logging.info(f"Endpoint '/cliente/{cpfcnpj}' acessado com sucesso.")

    df_cliente = await execute_query("consultas/cliente.sql", {":CNPJCPF": cpfcnpj})

    colunas = [
        "IDCLIFOR",
        "NOME",
        "NOMEFANTASIA",
        "CNPJCPF",
        "ENDERECO",
        "IDCEP",
        "NUMERO",
        "BAIRRO",
        "DESCRCIDADE",
        "DTNASCIMENTO",
        "COMPLEMENTO",
        "UF",
        "FONE2",
        "FONE1",
        "FONEFAX",
        "FONECELULAR",
        "EMAIL",
        "CODIGOIBGE",
        "GENERO",
        "IDVENDEDOR",
        "NOMEVENDEDOR",
    ]

    return await process_results(df_cliente, colunas)


@app.get("/preco/{idproduto}&{idempresa}")
async def preco_busca(idproduto: str, idempresa: str):
    """
    Esta chamada retorna o preço de venda varejo e o preço de venda promocional de um determinado produto em uma empresa.
    Utilize o código 1059453 e a empresa 1 para simular o retorno.
    Em cada execução ele retorna um único registro. Os campos que retornam serão apresentados na simulação.
    """

    logging.info(f"Endpoint '/preco/{idproduto}&{idempresa}' acessado com sucesso.")

    df_preco = await execute_query("consultas/preco.sql", {":IDPRODUTO": idproduto, ":IDEMPRESA": idempresa})

    colunas = ["VALPRECOVAREJO", "VALPROMVAREJO"]

    return await process_results(df_preco, colunas)


@app.get("/produtocompleto/{idempresa}&{idprodutos}")
async def produto_busca(idempresa: str, idprodutos: str):
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

    df_produtocompleto = await execute_query(
        "consultas/produtocompleto.sql", {":IDEMPRESA": idempresa, ":IDPRODUTOS": idprodutos}
    )

    colunas = [
        "IDSUBPRODUTO",
        "VALMULTIVENDAS",
        "PERCOMAVISTA",
        "NCM",
        "PESOBRUTO",
        "EMBALAGEMSAIDA",
        "LARGURA",
        "ALTURA",
        "COMPRIMENTO",
        "DESCRCOMPRODUTO",
        "FABRICANTE",
        "VALPRECOVAREJO",
        "VALPROMVAREJO",
        "ESTOQUESEVERO",
        "ESTOQUEGRAVATAI",
        "IDCODBARPROD",
        "REFERENCIA",
        "DTFIMPROMOCAOVAR",
        "MODELO",
    ]

    return await process_results(df_produtocompleto, colunas)


@app.get("/produtocompletofabricante/{idempresa}&{idfabricante}")
async def produto_busca_fabricante(idempresa: str, idfabricante: str):
    """
    Esta chamada retorna dados apenas o IDSUBPRODUTO de um cadastro filtrando preço por uma empresa e pela descricao do produto.
    Para visualizar o retorno basta colocar PISO PORTINARI NO idfabricante e 1 no idempresa.
    Em cada execução ele uma lista de idsubprodutos.
    """
    df_produtocompletofabricante = await execute_query(
        "consultas/produtocompletofabricante.sql", {":IDEMPRESA": idempresa, ":IDFABRICANTE": idfabricante}
    )
    colunas = ["IDSUBPRODUTO"]
    return await process_results(df_produtocompletofabricante, colunas, return_with_data=True)


@app.get("/produtocompletoestoque/{idempresa}&{qtdmaior}")
async def produto_busca_estoque(idempresa: str, qtdmaior: str):
    """
    Esta chamada retorna uma lista com dados de cadastro de produto com informações de estoque nos CDs e preço de venda e promoção.
    O filtro de idempresa é para identificar o preço de venda e promoção.
    O filtro de quantidade maior é para retornar somente produtos com quantidade maior que a filtrada nos CDs( a quantidade disponível somada
    dos 2 CDs).
    Exemplo para filtrar:
    idempresa = 1
    qtdmaior = 100
    """
    df_produtocompletoestoque = await execute_query(
        "consultas/produtocompletoestoque.sql", {":IDEMPRESA": idempresa, ":QTDMAIOR": qtdmaior}
    )
    colunas = ["IDSUBPRODUTO"]
    return await process_results(df_produtocompletoestoque, colunas, return_with_data=True)


@app.get("/produtocompletoestoquedescricao/{idempresa}&{fabricante}&{qtdmaior}")
async def produto_busca_estoque(idempresa: str, fabricante: str, qtdmaior: float):
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
    fabricante = fabricante.replace(" ", "%")
    df_produtocompletoestoquedescricao = await execute_query(
        "consultas/produtocompletoestoquedescricao.sql",
        {":IDEMPRESA": idempresa, ":FABRICANTE": fabricante, ":QTDMAIOR": str(qtdmaior)},
    )
    colunas = [
        "IDSUBPRODUTO",
        "DESCRCOMPRODUTO",
        "PERCOMAVISTA",
        "VALPRECOVAREJO",
        "VALPROMVAREJO",
        "DESCRGRUPO",
        "DESCRSUBGRUPO",
        "DESCRSECAO",
        "ESTOQUEGRAVATAI",
        "ESTOQUESEVERO",
        "EMBALAGEMSAIDA",
        "DATAFINALPROMOCAO",
    ]
    return await process_results(df_produtocompletoestoquedescricao, colunas, return_with_data=True)


@app.get("/produtocompletoreferencia/{idempresa}&{fabricante}")
async def produto_busca_estoque(idempresa: str, fabricante: str):
    """
    Esta chamada retorna uma lista com dados de cadastro de produto com informações de estoque nos CDs e preço de venda e promoção.
    O filtro de idempresa é para identificar o preço de venda e promoção.
    O filtro fabricante é para filtrar a referencia do produto
    Exemplo para filtrar:
    idempresa = 1
    fabricante = 1990 ( referencia da Deca - chuveiros)
    """
    fabricante = fabricante.replace(" ", "%")
    df_produtocompletoreferencia = await execute_query(
        "consultas/produtocompletoreferencia.sql", {":IDEMPRESA": idempresa, ":FABRICANTE": fabricante}
    )
    colunas = [
        "IDSUBPRODUTO",
        "DESCRCOMPRODUTO",
        "PERCOMAVISTA",
        "VALPRECOVAREJO",
        "VALPROMVAREJO",
        "DESCRGRUPO",
        "DESCRSUBGRUPO",
        "DESCRSECAO",
        "ESTOQUEGRAVATAI",
        "ESTOQUESEVERO",
        "EMBALAGEMSAIDA",
        "DATAFINALPROMOCAO",
    ]
    return await process_results(df_produtocompletoreferencia, colunas, return_with_data=True)


@app.get("/produtocodbar/{idproduto}")
async def produto_busca(idproduto: str):
    logging.info(f"Endpoint '/produtocodbar/{idproduto}' accessed successfully.")

    df = await execute_query("consultas/produtocodbar.sql", {":IDPRODUTO": idproduto})

    columns = ["IDCODBARPROD"]

    return await process_results(df, columns)


@app.get("/crosssel/{idsubproduto}")
async def produto_busca(idsubproduto: str):
    """
    Esta chamada retorna uma lista dos 20 produtos mais vendidos junto com o produto passado no filtro.
    Exemplo para filtrar:
    idsubproduto = 1059453

    """
    logging.info(f"Endpoint '/crosssel/{idsubproduto}' accessed successfully.")

    df = await execute_query("consultas/crosssell.sql", {":IDSUBPRODUTO": idsubproduto})

    columns = [
        "IDSUBPRODUTO",
        "IDSUBPRODUTO_P2",
        "NUM_SALES",
        "VALOR_P1",
        "VALOR_P2",
        "MARGEM_PER",
        "MARGEM_VALOR",
        "DESCR_P1",
        "GRUPO_P1",
        "SUBGRUPO_P1",
        "DESCR_P2",
        "GRUPO_P2",
        "SUBGRUPO_P2",
    ]

    return await process_results(df, columns, return_with_data=True)


@app.get("/vendasresumo/{idvendedor}")
async def vendas_resumo(idvendedor: str):
    """
    Esta chamada retorna um resumo do valor de vendas e margem de um determinado vendedor no mês corrente.
    Exemplo para filtrar:
    idempresa = 1137478
    """
    logging.info(f"Endpoint '/vendasresumo/{idvendedor}' accessed successfully.")

    df = await execute_query("consultas/vendasresumo.sql", {":IDVENDEDOR": idvendedor})

    columns = ["VALORVENDADIA", "VALORMARGEMDIA", "VALORVENDAMES", "VALORMARGEMMES"]

    return await process_results(df, columns)


@app.get("/itenspedido/{idpedido}")
async def itens_pedido(idpedido: str):
    """
    Esta chamada retorna uma lista de itens de um determinado pedido já efetivado no CISS.

    Exemplo para filtrar:
    idpedido = 3083928
    """
    logging.info(f"Endpoint '/itenspedido/{idpedido}' accessed successfully.")

    df = await execute_query("consultas/itenspedido.sql", {":IDPEDIDO": idpedido})

    columns = [
        "IDEMPRESA",
        "DATA",
        "IDVENDEDOR",
        "IDPRODUTO",
        "IDSUBPRODUTO",
        "MARCA",
        "DESCRPRODUTO",
        "NUMSEQUENCIA",
        "QTDPRODUTO",
        "VALORVENDALIQ",
        "VALORFRETEVENDA",
    ]

    return await process_results(df, columns, return_with_data=True)


@app.get("/estoque/{idproduto}")
async def estoque_busca(idproduto: str):
    """
    Esta chamada retorna a quantidade disponível de estoque nos CDs de um determinado produto.

    Exemplo para filtrar:
    idproduto = 1059453
    """
    logging.info(f"Endpoint '/estoque/{idproduto}' accessed successfully.")

    df = await execute_query("consultas/estoque.sql", {":IDPRODUTO": idproduto})

    columns = ["QTDDISPONIVEL"]

    return await process_results(df, columns)


@app.get("/especificadores/{cnpjcpf}")
async def especificadores_busca(cnpjcpf: str):
    """
    Esta chamada retorna dados de um especificador filtrando pelo cpf ou pelo número do telefone.

    Exemplo para filtrar:
    cnpjcpf = 56581580015 ou 51997038022
    """
    logging.info(f"Endpoint '/especificadores/{cnpjcpf}' accessed successfully.")

    df = await execute_query("consultas/especificadores.sql", {":CNPJCPF": cnpjcpf})

    columns = ["IDCLIFOR", "NOME", "IDCLUBE", "DESCRCLUBE"]

    return await process_results(df, columns)


@app.get("/orcamentocabecalho/{idvendedor}")
async def orcamentocabecalho_busca(idvendedor: str):
    """
    Esta chamada retorna uma lista com dados de um cabecalho de orçamento filtrando pelo idvendedor.

    Exemplo para filtrar:
    idvendedor = 1137478
    """
    logging.info(f"Endpoint '/orcamentocabecalho/{idvendedor}' accessed successfully.")

    df = await execute_query("consultas/orcamentocabecalho.sql", {":IDVENDEDOR": idvendedor})

    columns = ["IDORCAMENTO", "IDCLIFOR", "NOME", "VALTOTLIQUIDO", "DTMOVIMENTO", "FLAGCANCELADO"]

    return await process_results(df, columns, return_with_data=True)


@app.get("/pedidocabecalho/{idvendedor}")
async def pedidocabecalho_busca(idvendedor: str):
    """
    Esta chamada retorna uma lista com dados de um cabecalho de pedido efetivado filtrando pelo idvendedor.

    Exemplo para filtrar:
    idvendedor = 1137478
    """
    logging.info(f"Endpoint '/pedidocabecalho/{idvendedor}' accessed successfully.")

    df = await execute_query("consultas/pedidocabecalho.sql", {":IDVENDEDOR": idvendedor})

    columns = [
        "IDEMPRESA",
        "IDORCAMENTOORIGEM",
        "NOMEFANTASIA",
        "IDCLIFOR",
        "NOME",
        "DESCRCIDADE",
        "UF",
        "IDORCAMENTO",
        "VALOR_VENDA",
        "VALFRETE_VENDA",
    ]

    return await process_results(df, columns, return_with_data=True)


@app.get("/orcamentocabecalhoorcamento/{idorcamento}")
async def orcamentocabecalho_busca(idorcamento: str):
    """
    Esta chamada retorna dados de cabecalho de orçamento filtrando pelo idorcamento.

    Exemplo para filtrar:
    idorcamento = 3083769
    """
    logging.info(f"Endpoint '/orcamentocabecalhoorcamento/{idorcamento}' accessed successfully.")

    df = await execute_query("consultas/orcamentocabecalhoorcamento.sql", {":IDORCAMENTO": idorcamento})

    columns = [
        "IDORCAMENTO",
        "ENDERECOCOMPLETO",
        "IDCEP",
        "UF",
        "OBSERVACAO",
        "CNPJCPF",
        "BAIRRO",
        "NUMERO",
        "COMPLEMENTO",
        "FONECELULAR",
        "CLIENTE",
        "NOME",
        "IDVENDEDOR",
        "NOMEVENDEDOR",
        "EMAILVENDEDOR",
        "IDEMPRESA",
        "FONEEMPRESA",
        "ENDERECOEMPRESA",
        "EMAILEMPRESA",
        "NOMELOJA",
        "PERFILFRETE",
        "VALTOTLIQUIDO",
        "FLAGCANCELADO",
    ]

    return await process_results(df, columns)


@app.get("/orcamentoespecificador/{idespecificador}")
async def orcamentocabecalho_busca(idespecificador: str):
    """
    Esta chamada retorna uma lista com dados de um cabecalho de orçamento filtrando pelo idespecificador.

    Exemplo para filtrar:
    idespecificador = 1188771
    """
    logging.info(f"Endpoint '/orcamentoespecificador/{idespecificador}' accessed successfully.")

    df = await execute_query("consultas/orcamentoespecificador.sql", {":IDESPECIFICADOR": idespecificador})

    columns = ["IDORCAMENTO", "IDCLIFOR", "NOME", "VALTOTLIQUIDO", "DTMOVIMENTO"]

    return await process_results(df, columns, return_with_data=True)


@app.get("/orcamentoprodutovendedor/{idvendedor}&{idproduto}")
async def orcamentocabecalho_busca(idvendedor: str, idproduto: str):
    """
    Esta chamada retorna uma lista com dados de orcamento/itens de orcamento filtrando um item e um vendedor.

    Exemplo para filtrar:
    idvendedor = 1137478
    idproduto = 1059453
    """
    logging.info(f"Endpoint '/orcamentoprodutovendedor/{idvendedor}&{idproduto}' accessed successfully.")

    df = await execute_query(
        "consultas/orcamentoprodutovendedor.sql", {":IDVENDEDOR": idvendedor, ":IDPRODUTO": idproduto}
    )

    columns = ["IDORCAMENTO", "IDVENDEDOR", "QTDPRODUTO", "VALTOTLIQUIDO", "VALUNITBRUTO", "VALFRETE"]

    return await process_results(df, columns, return_with_data=True)


@app.get("/especificadorvendedor/{idvendedor}")
async def especificadorvendedor_busca(idvendedor: str):
    """
    Esta chamada retorna uma lista com dados de especificadores filtrando pelo vendedor(idvendedor).

    Exemplo para filtrar:
    idvendedor = 1137478
    """
    logging.info(f"Endpoint '/especificadorvendedor/{idvendedor}' accessed successfully.")

    df = await execute_query("consultas/especificadorvendedor.sql", {":IDVENDEDOR": idvendedor})

    columns = [
        "IDINDICADOR",
        "NOMEINDICADOR",
        "FONECELULAR",
        "QTDVENDAS",
        "IDVENDEDOR",
        "NOMEVENDEDOR",
        "ULTIMACOMPRA",
        "DIASSEMCOMPRAS",
        "STATUS",
        "VALOR_VENDA_INDICADOR",
        "VALFRETE_VENDA_INDICADOR",
        "VALOR_DEV_INDICADOR",
        "VALOR_CAN_INDICADOR",
        "VALORLIQUIDOVENDAINDICADOR",
    ]

    return await process_results(df, columns, return_with_data=True)


@app.get("/orcamentoitens/{idorcamento}")
async def orcamentocabecalho_busca(idorcamento: str):
    """
    Esta chamada retorna uma lista com dados de um itens de  um determiando orçamento. O filtro é pelo idorcamento.

    Exemplo para filtrar:
    idorcamento = 3081473( este orçamento pode ter sido cancelado o efetivado. Para filtrar como teste sugiro consultar um novo idorcamento no CISS.)

    """
    logging.info(f"Endpoint '/orcamentoitens/{idorcamento}' accessed successfully.")

    df = await execute_query("consultas/orcamentoitens.sql", {":IDORCAMENTO": idorcamento})

    columns = [
        "IDORCAMENTO",
        "IDPRODUTO",
        "IDSUBPRODUTO",
        "DESCRAMBIENTE",
        "NUMSEQUENCIA",
        "IDVENDEDOR",
        "VALMULTIVENDAS",
        "EMBALAGEMSAIDA",
        "DESCRCOMPRODUTO",
        "VALDESCONTOPRO",
        "PERDESCONTOPRO",
        "VALLUCRO",
        "PERMARGEMLUCRO",
        "TIPOENTREGA",
        "NUMSEQUENCIA",
        "IDLOCALRETIRADA",
        "DESCRLOCALRETIRADA",
        "IDLOCALESTOQUE",
        "IDLOTE",
        "QTDPRODUTO",
        "VALUNITBRUTO",
        "VALFRETE",
        "VALTOTLIQUIDO",
    ]

    return await process_results(df, columns, return_with_data=True)


@app.get("/produtosmaisorcados/")
async def orcamentocabecalho_busca():
    """
    Esta chamada retorna uma lista com os 20 itens mais orçados nos últimos 30 dias em todo o Grupo Elevato.

    Não é aplicado nenhum filtro.

    """
    logging.info(f"Endpoint '/produtosmaisorcados/' accessed successfully.")

    df = await execute_query("consultas/produtosmaisorcados.sql", {})

    columns = ["IDPRODUTO", "QTDPRODUTO", "VALTOTLIQUIDO", "QTDORCAMENTO"]

    return await process_results(df, columns, return_with_data=True)


@app.get("/produtosmaisorcadosvendedor/{idvendedor}")
async def orcamentocabecalho_busca(idvendedor: str):
    """
    Esta chamada retorna uma lista com os 20 itens mais orçados de um vendedor nos últimos 30 dias.

    Exemplo para filtrar:
    idvendedor = 1137478

    """
    logging.info(f"Endpoint '/produtosmaisorcados/{idvendedor}' accessed successfully.")

    replace_dict = {":IDVENDEDOR": idvendedor}

    df = await execute_query("consultas/produtosmaisorcadosvendedor.sql", replace_dict)

    columns = ["IDPRODUTO", "QTDPRODUTO", "VALTOTLIQUIDO", "QTDORCAMENTO"]

    return await process_results(df, columns, return_with_data=True)


@app.get("/produtosmaisorcadosdepartamento/{iddepartamento}")
async def orcamentocabecalho_busca(iddepartamento: str):
    """
    Esta chamada retorna uma lista com os 20 itens mais orçados em um departamento nos últimos 30 dias

    Exemplo para filtrar:
    iddepartamento = 4

    """
    logging.info(f"Endpoint '/produtosmaisorcados/{iddepartamento}' accessed successfully.")

    replace_dict = {":IDDEPARTAMENTO": iddepartamento}

    df = await execute_query("consultas/produtosmaisorcadosdepartamento.sql", replace_dict)

    columns = ["IDPRODUTO", "QTDPRODUTO", "VALTOTLIQUIDO", "QTDORCAMENTO"]

    return await process_results(df, columns, return_with_data=True)


@app.get("/buscaestoquelote/{idsubproduto}")
async def orcamentocabecalho_busca(idsubproduto: str):
    """
    Esta chamada retorna uma lista com os dados de Lote de um determinado produto e sua quantidade disponível.

    Exemplo para filtrar:
    idsubproduto = 1059453

    """
    logging.info(f"Endpoint '/buscaestoquelote/{idsubproduto}' accessed successfully.")

    replace_dict = {":IDSUBPRODUTO": idsubproduto}

    df = await execute_query("consultas/buscaestoquelote.sql", replace_dict)

    columns = [
        "IDPRODUTO",
        "IDSUBPRODUTO",
        "DESCRLOTE",
        "IDLOCALESTOQUE",
        "DESCRLOCALESTOQUE",
        "QTDSALDODISPONIVEL",
    ]

    return await process_results(df, columns, return_with_data=True)


@app.post("/clientes/")
async def create_cliente(cliente: Cliente):
    cpfcnpj: Cliente.cpfcnpj
    nome: Cliente.nome
    rua: Cliente.rua
    numero: Cliente.numero
    complemento: Cliente.complemento
    bairro: Cliente.bairro
    cep: Cliente.cep
    # idcliente:Cliente.idcliente
    fonecelular: Cliente.fonecelular
    email: Cliente.email
    idusuariocadastro: Cliente.idusuariocadastro
    obsgeral: Cliente.obsgeral
    inscestadual: Cliente.inscestadual
    dtnascimento: Cliente.dtnascimento
    nomefantasia: Cliente.nomefantasia

    logging.info(
        f"Endpoint '/clientes/{cliente.cpfcnpj}&{cliente.nome}&{cliente.cep}&{cliente.rua}&{cliente.bairro}&{cliente.complemento}&{cliente.email}&{cliente.fonecelular}&{cliente.dtnascimento}&{cliente.inscestadual}&{cliente.nomefantasia}&{cliente.numero}' acessado com sucesso."
    )
    pyodbc.drivers()

    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()
    # conexao1 = pyodbc.connect(dados_conexao_pg)
    carrega_grava_clientes = pd.read_sql(
        f"""SELECT
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
        ) AS TBL""",
        conexao,
    )
    cursor.commit()
    cursor.close()

    # print(carrega_grava_clientes)
    for index, row in carrega_grava_clientes.iterrows():
        colunas = ["IDCLIENTE"]
        idclienteciss = {row[coluna] for coluna in colunas}
        print(idclienteciss)
        for idciss in idclienteciss:
            print(idciss)
        # Cliente.idcliente:idciss
        # print(Cliente.idcliente)
    # idcliente:"100"
    # idciss1 = int(idciss)
    # print(idciss)
    # idcliente:idciss
    # Cliente.idcliente:idciss
    # print(cliente.idcliente)

    # return carrega_grava_clientes.idcliente
    return idclienteciss


@app.post("/pedido/")
async def create_pedido(pedido: Pedido):
    idempresa: Pedido.idempresa
    idclifor: Pedido.idclifor
    logging.info(f"Endpoint '/pedido/{pedido.idempresa}&{pedido.idclifor}' acessado com sucesso.")
    pyodbc.drivers()

    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_grava_pedido = pd.read_sql(
        f"""SELECT
           cast(idpedido as varchar(20)) as idpedido
        FROM
        TABLE(  BIELEVATO.SET_PEDIDO_BUBBLE(
                                    '{pedido.idempresa}',
                                    '{pedido.idclifor}'
                )
        ) AS TBL""",
        conexao,
    )

    cursor.commit()
    cursor.close()

    # print(carrega_grava_clientes)
    for index, row in carrega_grava_pedido.iterrows():
        colunas = ["IDPEDIDO"]
        idpedidociss = {row[coluna] for coluna in colunas}
        print(idpedidociss)
        for idciss in idpedidociss:
            print(idciss)

    return idpedidociss


@app.post("/pedidoconsumidor/")
async def create_pedidoconsumidor(pedidoconsumidor: PedidoConsumidor):
    idempresa: PedidoConsumidor.idempresa
    idclifor: PedidoConsumidor.idclifor
    nome: PedidoConsumidor.nome
    logging.info(
        f"Endpoint '/pedidoconsumidor/{pedidoconsumidor.idempresa}&{pedidoconsumidor.idclifor}&{pedidoconsumidor.nome}' acessado com sucesso."
    )
    pyodbc.drivers()

    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_grava_pedido = pd.read_sql(
        f"""SELECT
           cast(idpedido as varchar(20)) as idpedido
        FROM
        TABLE(  BIELEVATO.SET_PEDIDO_BUBBLE_CONSUMIDOR(
                                    '{pedidoconsumidor.idempresa}',
                                    '{pedidoconsumidor.idclifor}',
                                    '{pedidoconsumidor.nome}'
                )
        ) AS TBL""",
        conexao,
    )

    cursor.commit()
    cursor.close()

    # print(carrega_grava_clientes)
    for index, row in carrega_grava_pedido.iterrows():
        colunas = ["IDPEDIDO"]
        idpedidociss = {row[coluna] for coluna in colunas}
        print(idpedidociss)
        for idciss in idpedidociss:
            print(idciss)

    return idpedidociss


@app.post("/orcamentocancelamento/")
async def create_orcamento(orcamentocancelamento: OrcamentoPerdido):
    idorcamento: OrcamentoPerdido.idorcamento
    logging.info(f"Endpoint '/orcamentocancelamento/{orcamentocancelamento.idorcamento}' acessado com sucesso.")
    pyodbc.drivers()

    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_cancela_orcamento = pd.read_sql(
        f"""SELECT
           STATUS 
        FROM
        TABLE(  BIELEVATO.CANCELA_ORCAMENTO_BUBBLE(
                                    '{orcamentocancelamento.idorcamento}'
                )
        ) AS TBL""",
        conexao,
    )

    cursor.commit()
    cursor.close()

    # print(carrega_grava_clientes)
    for index, row in carrega_cancela_orcamento.iterrows():
        colunas = ["STATUS"]
        status = {row[coluna] for coluna in colunas}
        print(status)
        for tstatus in status:
            print(tstatus)

    return status


@app.post("/itempedido/")
async def create_item_pedido(itempedido: ItemPedido):
    idpedido: ItemPedido.idpedido
    idempresa: ItemPedido.idempresa
    idvendedor: ItemPedido.idvendedor
    idproduto: ItemPedido.idproduto
    idsubproduto: ItemPedido.idsubproduto
    numsequencia: ItemPedido.numsequencia
    idlote: ItemPedido.idlote
    qtdproduto: ItemPedido.qtdproduto
    valunitbruto: ItemPedido.valunitbruto
    valtotliquido: ItemPedido.valtotliquido
    valdescontopro: ItemPedido.valdescontopro
    ambiente: ItemPedido.ambiente
    idlocalestoque: ItemPedido.idlocalestoque

    logging.info(
        f"Endpoint '/pedido/{itempedido.idempresa}&{itempedido.ambiente}&{itempedido.idpedido}&{itempedido.idvendedor}&{itempedido.idproduto}&{itempedido.idsubproduto}&{itempedido.qtdproduto}&{itempedido.valtotliquido}&{itempedido.idproduto}&{itempedido.numsequencia}&{itempedido.valdescontopro}&{itempedido.valunitbruto}' acessado com sucesso."
    )
    print(itempedido.valtotliquido)
    print(itempedido.valunitbruto)
    print(itempedido.qtdproduto)
    print(itempedido.idlote)

    pyodbc.drivers()

    conexao = pyodbc.connect(dados_conexao_db2)
    cursor = conexao.cursor()

    carrega_grava_item_pedido = pd.read_sql(
        f"""SELECT
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
        ) AS TBL""",
        conexao,
    )
    logging.info(
        f"Endpoint '/pedido/{itempedido.idpedido}&{itempedido.idvendedor}&{itempedido.idproduto}' passou no carregaitem e gravou com com sucesso."
    )
    cursor.commit()
    cursor.close()

    # print(carrega_grava_clientes)
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


# app = FastAPI()


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
