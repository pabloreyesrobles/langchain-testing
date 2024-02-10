from jinja_psycopg import JinjaPsycopg
import psycopg
from psycopg import sql
from dotenv import load_dotenv
import os
from markupsafe import Markup

class DBA():
    def __init__(self):
        pass
    
    def __del__(self):
        self.conn.close()
        
    def __enter__(self):
        load_dotenv()
        self.db_host = os.getenv("DB_HOST")
        self.db_port = os.getenv("DB_PORT")
        self.db_name = os.getenv("DB_NAME")
        self.db_user = os.getenv("DB_USERNAME")
        self.db_password = os.getenv("DB_PASSWORD")
        self.conn = psycopg.connect(f"dbname={self.db_name} user={self.db_user} password={self.db_password} host={self.db_host} port={self.db_port}")
        self.cur = self.conn.cursor()
        return self
    
    def __exit__(self,type,value,traceback):
        self.conn.close()
        
    def execute(self, template, params = None):
        try:
            self.conn
            self.cur
        except:
            self.conn = psycopg.connect(f"dbname={self.db_name} user={self.db_user} password={self.db_password} host={self.db_host} port={self.db_port}")
            self.cur = self.conn.cursor()
        
        # Check if first part of template exists: folder_name
        
        if "_" not in template:
            return False, "Invalid template name"
        
        template_split = template.split("_")
        
        folder_name = template_split[0]
        
        # Check if folder exists in postgresql dir
        
        if not os.path.isdir(f"./postgresql/{folder_name}"):
            return False, "Invalid template name"
        
        # Check if file exists in postgresql dir
        
        if not os.path.isfile(f"./postgresql/{folder_name}/{template}.sql"):
            return False, "Invalid template name"
        
        # Open and read file
        
        with open(f"./postgresql/{folder_name}/{template}.sql", "r") as f:
            query = f.read()
        
        try:
            _query = query
            if params is not None:
                for param in params:
                    if isinstance(params[param], str):
                        _query = _query.replace(f"@{param}", f"'{params[param]}'")
                    else:
                        _query = _query.replace(f"@{param}", str(params[param]))
            if "@" in _query:
                return False, "No se han incluido todos los parametros"
        except Exception as e:
            return False, "Error al generar la consulta: " + str(e)
            
        self.cur.execute(_query)
        self.conn.commit()
        fetch = self.cur.fetchall()
        col_names = [desc[0] for desc in self.cur.description]
        
        # Generate Json with column: value format
        fetch = [dict(zip(col_names, row)) for row in fetch]
        return True, fetch
    
    def execute_sql(self, query: str, req: bool = True):
        try:
            self.conn
            self.cur
        except:
            self.conn = psycopg.connect(f"dbname={self.db_name} user={self.db_user} password={self.db_password} host={self.db_host} port={self.db_port}")
            self.cur = self.conn.cursor()
            
        self.cur.execute(query)
        self.conn.commit()

        if not req:
            return True, None
        
        fetch = self.cur.fetchall()
        col_names = [desc[0] for desc in self.cur.description]
        
        # Generate Json with column: value format
        fetch = [dict(zip(col_names, row)) for row in fetch]
        return True, fetch
