import unittest
from http import HTTPStatus
from fastapi.testclient import TestClient

from app import app


class TestClienteBusca(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_cliente_busca(self):
        cpfcnpj = "81839138068"
        response = self.client.get(f"/cliente/{cpfcnpj}")

        # self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertEqual(response.headers["content-type"], "application/json")

        data = response.json()
        self.assertIn("IDCLIFOR", data)
        self.assertIn("NOME", data)
        self.assertIn("NOMEFANTASIA", data)
        self.assertIn("CNPJCPF", data)
        self.assertIn("ENDERECO", data)
        self.assertIn("IDCEP", data)
        self.assertIn("NUMERO", data)
        self.assertIn("BAIRRO", data)
        self.assertIn("DESCRCIDADE", data)
        self.assertIn("DTNASCIMENTO", data)
        self.assertIn("COMPLEMENTO", data)
        self.assertIn("UF", data)
        self.assertIn("FONE2", data)
        self.assertIn("FONE1", data)
        self.assertIn("FONEFAX", data)
        self.assertIn("FONECELULAR", data)
        self.assertIn("EMAIL", data)
        self.assertIn("CODIGOIBGE", data)
        self.assertIn("GENERO", data)
        self.assertIn("IDVENDEDOR", data)
        self.assertIn("NOMEVENDEDOR", data)


class TestPrecoEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_preco_busca(self):
        idproduto = "1059453"
        idempresa = "1"
        response = self.client.get(f"/preco/{idproduto}&{idempresa}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertIn("VALPRECOVAREJO", data)
        self.assertIn("VALPROMVAREJO", data)


class TestProdutocompletoEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_produtocompleto_busca(self):
        idempresa = "1"
        idprodutos = "1059453,1081882,1056157"
        response = self.client.get(f"/produtocompleto/{idempresa}&{idprodutos}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertSetEqual(
            set(data.keys()),
            set(
                [
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
            ),
        )


class TestProdutocompletoFabricanteEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_produtocompletofabricante_busca(self):
        idempresa = "1"
        idfabricante = "PISO PORTINARI"
        response = self.client.get(f"/produtocompletofabricante/{idempresa}&{idfabricante}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertIn("data", data)
        results = data["data"]
        self.assertIsInstance(results, list)
        for result in results:
            self.assertSetEqual(set(result.keys()), set(["IDSUBPRODUTO"]))


class TestProdutocompletoEstoqueEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_produtocompletoestoque_busca(self):
        idempresa = "1"
        qtdmaior = "100"
        response = self.client.get(f"/produtocompletoestoque/{idempresa}&{qtdmaior}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertIn("data", data)
        results = data["data"]
        self.assertIsInstance(results, list)
        for result in results:
            self.assertSetEqual(set(result.keys()), set(["IDSUBPRODUTO"]))


class TestProdutocompletoEstoqueDescricaoEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_produtocompletoestoquedescricao_busca(self):
        idempresa = "1"
        fabricante = "PISO PORTINARI"
        qtdmaior = 100
        response = self.client.get(f"/produtocompletoestoquedescricao/{idempresa}&{fabricante}&{qtdmaior}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertIn("data", data)
        results = data["data"]
        self.assertIsInstance(results, list)
        if not results:
            self.fail("No products found matching the search criteria.")
        for result in results:
            self.assertSetEqual(
                set(result.keys()),
                set(
                    [
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
                ),
            )


class TestProdutocompletoReferenciaEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_produtocompletoreferencia_busca(self):
        idempresa = "1"
        fabricante = "1990"
        response = self.client.get(f"/produtocompletoreferencia/{idempresa}&{fabricante}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertIn("data", data)
        results = data["data"]
        self.assertIsInstance(results, list)
        if not results:
            self.fail("No products found matching the search criteria.")
        for result in results:
            self.assertSetEqual(
                set(result.keys()),
                set(
                    [
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
                ),
            )


class TestProdutocodbarEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_produtocodbar_busca(self):
        idproduto = "1059453"
        response = self.client.get(f"/produtocodbar/{idproduto}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertSetEqual(set(data.keys()), set(["IDCODBARPROD"]))


class TestCrossselEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_crosssel_busca(self):
        idsubproduto = "1059453"
        response = self.client.get(f"/crosssel/{idsubproduto}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertIn("data", data)
        result = data["data"]
        self.assertIsInstance(result, list)
        if not result:
            self.fail("No cross-sell products found for the given product ID.")
        for item in result:
            self.assertSetEqual(
                set(item.keys()),
                set(
                    [
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
                ),
            )


class TestVendasResumoEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_vendas_resumo(self):
        idvendedor = "1137478"
        response = self.client.get(f"/vendasresumo/{idvendedor}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertSetEqual(
            set(data.keys()),
            set(["VALORVENDADIA", "VALORMARGEMDIA", "VALORVENDAMES", "VALORMARGEMMES"]),
        )


class TestItensPedidoEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_itens_pedido(self):
        idpedido = "3083928"
        response = self.client.get(f"/itenspedido/{idpedido}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertIn("data", data)
        result = data["data"]
        self.assertIsInstance(result, list)
        if not result:
            self.fail("No items found for the given pedido ID.")
        for item in result:
            self.assertSetEqual(
                set(item.keys()),
                set(
                    [
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
                ),
            )


class TestEstoqueEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_estoque(self):
        idproduto = "1059453"
        response = self.client.get(f"/estoque/{idproduto}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertSetEqual(
            set(data.keys()),
            set(["QTDDISPONIVEL"]),
        )


class TestEspecificadoresEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_especificadores(self):
        cnpjcpf = "56581580015"
        response = self.client.get(f"/especificadores/{cnpjcpf}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertSetEqual(
            set(data.keys()),
            set(["IDCLIFOR", "NOME", "IDCLUBE", "DESCRCLUBE"]),
        )


class TestOrcamentoCabecalhoEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_orcamento_cabecalho(self):
        idvendedor = "1137478"
        response = self.client.get(f"/orcamentocabecalho/{idvendedor}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertIn("data", data)
        result = data["data"]
        self.assertIsInstance(result, list)
        if not result:
            self.fail("No orcamentocabecalho found for the given idvendedor.")
        for item in result:
            self.assertSetEqual(
                set(item.keys()),
                set(
                    [
                        "IDORCAMENTO",
                        "IDCLIFOR",
                        "NOME",
                        "VALTOTLIQUIDO",
                        "DTMOVIMENTO",
                        "FLAGCANCELADO",
                    ]
                ),
            )


class TestPedidoCabecalhoEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_pedidocabecalho(self):
        idvendedor = "1137478"
        response = self.client.get(f"/pedidocabecalho/{idvendedor}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertIn("data", data)
        result = data["data"]
        self.assertIsInstance(result, list)
        if not result:
            self.fail("No pedidocabecalho found for the given idvendedor.")
        for item in result:
            self.assertSetEqual(
                set(item.keys()),
                set(
                    [
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
                ),
            )


class TestOrcamentoCabecalhoOrcamentoEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_orcamentocabecalhoorcamento(self):
        idorcamento = "3083769"
        response = self.client.get(f"/orcamentocabecalhoorcamento/{idorcamento}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertSetEqual(
            set(data.keys()),
            set(
                [
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
            ),
        )


class TestOrcamentoEspecificadorEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_orcamentoespecificador(self):
        idespecificador = "1188771"
        response = self.client.get(f"/orcamentoespecificador/{idespecificador}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertIn("data", data)
        result = data["data"]
        self.assertIsInstance(result, list)
        self.assertTrue(result, "No orcamentoespecificador found for the given idespecificador.")
        for item in result:
            self.assertSetEqual(
                set(item.keys()),
                set(["IDORCAMENTO", "IDCLIFOR", "NOME", "VALTOTLIQUIDO", "DTMOVIMENTO"]),
            )


class TestOrcamentoProdutoVendedorEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_orcamentoprodutovendedor(self):
        idvendedor = "1137478"
        idproduto = "1059453"
        response = self.client.get(f"/orcamentoprodutovendedor/{idvendedor}&{idproduto}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertIn("data", data)
        result = data["data"]
        self.assertIsInstance(result, list)
        self.assertTrue(result, "No orcamentoprodutovendedor found for the given idvendedor and idproduto.")
        for item in result:
            self.assertSetEqual(
                set(item.keys()),
                set(
                    [
                        "IDORCAMENTO",
                        "IDVENDEDOR",
                        "QTDPRODUTO",
                        "VALTOTLIQUIDO",
                        "VALUNITBRUTO",
                        "VALFRETE",
                    ]
                ),
            )


class TestEspecificadorVendedorEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_especificadorvendedor(self):
        idvendedor = "1137478"
        response = self.client.get(f"/especificadorvendedor/{idvendedor}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertIn("data", data)
        result = data["data"]
        self.assertIsInstance(result, list)
        self.assertTrue(result, "No especificadorvendedor found for the given idvendedor.")
        for item in result:
            self.assertSetEqual(
                set(item.keys()),
                set(
                    [
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
                ),
            )


class TestOrcamentoItensEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_orcamentoitens(self):
        idorcamento = "3081473"
        response = self.client.get(f"/orcamentoitens/{idorcamento}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertIn("data", data)
        result = data["data"]
        self.assertIsInstance(result, list)
        if not result:
            self.fail("No orcamentoitens found for the given idorcamento.")
        for item in result:
            self.assertSetEqual(
                set(item.keys()),
                set(
                    [
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
                ),
            )


class TestProdutosMaisOrcadosEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_produtosmaisorcados(self):
        response = self.client.get("/produtosmaisorcados/")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertIn("data", data)
        result = data["data"]
        self.assertIsInstance(result, list)
        self.assertTrue(result, "No produtosmaisorcados found.")
        for item in result:
            self.assertSetEqual(
                set(item.keys()),
                set(["IDPRODUTO", "QTDPRODUTO", "VALTOTLIQUIDO", "QTDORCAMENTO"]),
            )

    def test_produtosmaisorcadosvendedor(self):
        idvendedor = "1137478"
        response = self.client.get(f"/produtosmaisorcadosvendedor/{idvendedor}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertIn("data", data)
        result = data["data"]
        self.assertIsInstance(result, list)
        self.assertTrue(result, "No produtosmaisorcadosvendedor found for the given idvendedor.")
        for item in result:
            self.assertSetEqual(
                set(item.keys()),
                set(["IDPRODUTO", "QTDPRODUTO", "VALTOTLIQUIDO", "QTDORCAMENTO"]),
            )

    def test_produtosmaisorcadosdepartamento(self):
        iddepartamento = "4"
        response = self.client.get(f"/produtosmaisorcadosdepartamento/{iddepartamento}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertIn("data", data)
        result = data["data"]
        self.assertIsInstance(result, list)
        self.assertTrue(result, "No produtosmaisorcadosdepartamento found for the given iddepartamento.")
        for item in result:
            self.assertSetEqual(
                set(item.keys()),
                set(["IDPRODUTO", "QTDPRODUTO", "VALTOTLIQUIDO", "QTDORCAMENTO"]),
            )


class TestBuscaEstoqueLoteEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_buscaestoquelote(self):
        idsubproduto = "1059453"
        response = self.client.get(f"/buscaestoquelote/{idsubproduto}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)
        self.assertIn("data", data)
        result = data["data"]
        self.assertIsInstance(result, list)
        self.assertTrue(result, "No buscaestoquelote found for the given idsubproduto.")
        for item in result:
            self.assertSetEqual(
                set(item.keys()),
                set(
                    [
                        "IDPRODUTO",
                        "IDSUBPRODUTO",
                        "DESCRLOTE",
                        "IDLOCALESTOQUE",
                        "DESCRLOCALESTOQUE",
                        "QTDSALDODISPONIVEL",
                    ]
                ),
            )


if __name__ == "__main__":
    unittest.main()
