# from dotenv import load_dotenv

# load_dotenv()  # take environment variables from .env.

# You can include other configurations as well, such as secret keys, environment specific variables, etc.
databases = {
    "ciss_db2": {
        "DRIVER": "{IBM DB2 ODBC DRIVER}",
        "HOSTNAME": "acesso.elevato.com.br",
        "PORT": "50000",
        "PROTOCOL": "TCPIP",
        "DATABASE": "CISSERP",
        "UID": "bielevato",
        "PWD": "BUQ8~5x?mOPLI18",
    },
    "dw_postgres": {
        "DRIVER": "{PostgreSQL Unicode}",
        "SERVER": "dwelevato.chco3y0yg2na.us-east-1.rds.amazonaws.com",
        "PORT": "5432",
        "DATABASE": "dwelevato",
        "UID": "postgres",
        "PWD": "suacasanossacausa22",
    },
}


def get_connection_string(database: str) -> str:
    if database in databases:
        if database == "ciss_db2":
            connection_string = f"ibm_db_sa://{databases[database]['UID']}:{databases[database]['PWD']}@{databases[database]['HOSTNAME']}:{databases[database]['PORT']}/{databases[database]['DATABASE']}"
            return connection_string
        elif database == "dw_postgres":
            connection_string = f"postgresql+psycopg2://{databases[database]['UID']}:{databases[database]['PWD']}@{databases[database]['SERVER']}:{databases[database]['PORT']}/{databases[database]['DATABASE']}"
            return connection_string
        
    else:
        raise ValueError(f"No configuration found for database: {database}")
