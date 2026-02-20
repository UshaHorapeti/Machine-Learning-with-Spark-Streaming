import pyodbc
import pandas as pd

class myDenodo:
    
    def __init__(self, **kwargs):        
        self.cnxn = pyodbc.connect("DSN=DenodoODBC")


    def readfromDB(self, vql, **kwargs):
        print("Extracting Data from Denodo")
        return pd.read_sql_query(vql, self.cnxn)
        
class myDatabricks:
    
    def __init__(self, **kwargs):        
        self.cnxn = pyodbc.connect("DSN=DenodoODBC")


    def readfromDB(self, vql, **kwargs):
        print("Extracting Data from Denodo")
        return pd.read_sql_query(vql, self.cnxn)
        
