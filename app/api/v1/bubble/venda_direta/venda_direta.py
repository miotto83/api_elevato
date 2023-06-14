from fastapi import HTTPException, Depends, APIRouter, Request
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy import create_engine, MetaData, Table, select, and_
from core.database import Database
from core.config import get_connection_string
from sqlalchemy import text
from datetime import date

# Define your models
class SaleBase(BaseModel):
    DATA_CRIACAO: Optional[date] = None
    DATA_COMPRA: Optional[date] = None
    UNIDADE: Optional[str] = None
    VENDEDOR: Optional[str] = None
    METODO_PAY: Optional[str] = None
    NUMERO_PARCELA: Optional[int] = None
    FORNECEDOR: Optional[str] = None
    NOME_CLIENTE: Optional[str] = None
    VALOR_VENDA: Optional[float] = None
    MARGEM_PERC: Optional[float] = None
    MARGEM_VENDA: Optional[float] = None
    COMISSAO: Optional[float] = None
    IDPEDIDO: Optional[int] = None
    IDARQ: Optional[int] = None
    OBS: Optional[str] = None
    PREENCHEDOR: Optional[str] = None
    IDEMPRESA: Optional[int] = None
    NOME_FANTASIA: Optional[str] = None
    IDDEPARTAMENTO: Optional[int] = None
    DESCRDEPARTAMENTO: Optional[str] = None
    PARABI: Optional[str] = None
    COLUNAZERO: Optional[str] = None
    MARCA_CISS: Optional[str] = None
    IDSUBPRODUTO_CISS: Optional[int] = None

class SaleCreate(SaleBase):
    pass

class Sale(SaleBase):
    ID: int
    class Config:
        orm_mode = True

# Define your database and table
metadata = MetaData()
database = Database("dw_postgres")  # Specify your database
sales_data = Table("bubble__venda_direta", metadata, autoload_with=database.engine)

router = APIRouter()

@router.post("/", response_model=Sale)
def create_sale(sale: SaleCreate, db: Session = Depends(database.get_db)):
    result = db.execute(sales_data.insert().values(**sale.dict()))
    db.commit()
    print(sale.dict())
    return {"ID": result.inserted_primary_key[0], **sale.dict()}

@router.get("/", response_model=Sale)
def read_sales(ID: int = None, db: Session = Depends(database.get_db)):
    stmt = select(sales_data).where(sales_data.c.ID == ID)
    result = db.execute(stmt).fetchone()
    if not result:
        raise HTTPException(status_code=404, detail="Sale not found")
    return result._asdict()  # convert the record to dictionary before returning


@router.put("/{ID}", response_model=Sale)
def update_sale(ID: int, sale: SaleCreate, db: Session = Depends(database.get_db)):
    result = db.execute(select(sales_data).where(sales_data.c.ID == ID))
    record = result.fetchone()
    if not record:
        raise HTTPException(status_code=404, detail="Sale not found")
    
    # Create a dictionary of the updated values
    updated_values = {column: getattr(sale, column) for column in sale.dict() if getattr(sale, column) is not None}
    
    db.execute(
        sales_data.update().where(sales_data.c.ID == ID).values(**updated_values)
    )
    db.commit()
    
    # Fetch the updated record from the database
    updated_record = db.execute(select(sales_data).where(sales_data.c.ID == ID)).fetchone()
    
    return updated_record._asdict()  # convert the record to dictionary before returning



@router.delete("/{ID}", response_model=dict)
def delete_sale(ID: int, db: Session = Depends(database.get_db)):
    result = db.execute(
        select(sales_data).where(sales_data.c.ID == ID)
    )
    record = result.scalar()
    if not record:
        raise HTTPException(status_code=404, detail="Sale not found")
    db.execute(
        sales_data.delete().where(sales_data.c.ID == ID)
    )
    db.commit()
    return {"ID": ID}



