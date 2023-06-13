import pandas as pd
from fastapi import HTTPException
from typing import List
import logging

# import pyodbc
from sqlalchemy.orm import Session
from core.database import Database
import pandas as pd


dados_conexao_db2 = (
    # Your database connection string goes here
)


async def execute_query(database: str, file_path: str = None, replace_dict: dict = None, query: str = None):
    logging.info(f"Running query from '{file_path}'")

    db = Database(database)
    with Session(db.engine) as session:

        if query is None:
            query = open(file_path, "r").read()
        
        if replace_dict is not None:
            for key, value in replace_dict.items():
                query = query.replace(key, value)
                
        df = pd.read_sql(query, session.bind)
        df.columns = map(str.upper, df.columns)
        print(df.head())
    return df


async def process_results(df: pd.DataFrame, columns: List[str] = None, return_with_data: bool = False):
    data = []
    if columns is None:
        columns = df.columns

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
