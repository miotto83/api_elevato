from fastapi import FastAPI
import importlib
import pkgutil
from fastapi import APIRouter
from core import logger

app = FastAPI()

def load_api_modules(app, package_name, tags=[]):
    package = __import__(package_name, fromlist=[""])
    
    for loader, module_name, is_pkg in pkgutil.walk_packages(package.__path__):
        if is_pkg:
            load_api_modules(app, package_name + "." + module_name, tags)
        else:
            module = importlib.import_module("." + module_name, package_name)
            if hasattr(module, "router"):
                prefix = "/" + "/".join(package_name.split(".")[:-1]) + "/" + module_name
                router = APIRouter()
                router.include_router(module.router, prefix=prefix, tags=tags)
                app.include_router(router)
    return app

@app.on_event("startup")
async def startup_event():
    logger.setup()

@app.on_event("shutdown")
async def shutdown_event():
    pass

app = load_api_modules(app, package_name="api.v1.clientes", tags=["Clientes"])
app = load_api_modules(app, package_name="api.v1.especificadores", tags=["Especificadores"])
app = load_api_modules(app, package_name="api.v1.pedidos", tags=["Pedidos"])
app = load_api_modules(app, package_name="api.v1.produtos", tags=["Produtos"])
app = load_api_modules(app, package_name="api.v1.vendedores", tags=["Vendedores"])

# Bubble
app = load_api_modules(app, package_name="api.v1.bubble.venda_direta", tags=["Bubble - Venda Direta"])













# #  GET
# app.include_router(gets.clientes.router, prefix="/api/v1/get/clientes", tags=["Clientes"])
# app.include_router(gets.especificadores.router, prefix="/api/v1/get/especificadores", tags=["Especificadores"])
# app.include_router(gets.pedidos.router, prefix="/api/v1/get/pedidos", tags=["Pedidos"])
# app.include_router(gets.produtos.router, prefix="/api/v1/get/produtos", tags=["Produtos"])
# app.include_router(gets.vendas.router, prefix="/api/v1/get/vendas", tags=["Vendas"])
# app.include_router(gets.vendedores.router, prefix="/api/v1/get/vendedores", tags=["Vendedores"])

# # POST
# app.include_router(posts.clientes.router, prefix="/api/v1/get/clientes", tags=["Clientes"])
# app.include_router(posts.pedidos.router, prefix="/api/v1/get/pedidos", tags=["Pedidos"])