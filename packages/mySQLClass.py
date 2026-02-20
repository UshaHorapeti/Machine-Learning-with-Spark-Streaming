import pyodbc,time, pandas as pd
import traceback
from urllib.parse import quote_plus
from sqlalchemy import create_engine


try:
    from packages.myFileClass import myFile as myFile
except ModuleNotFoundError:
    
    from myFileClass import myFile as myFile
download=myFile()

class mySQLDB:
    log = ''
    def __init__(self,uid,pwd,**kwargs):
        self.download=myFile(debug='')

        uid_type = kwargs.get('uid_type', None)

        if uid_type == 'DB':
            trusted_conn = 'NO'
        else:
            trusted_conn = 'YES'
            
        
        conn_str = (
            r'Driver={ODBC Driver 17 for SQL Server};'
            r'Server=pw01tgsshsdb02.database.windows.net,1433;'
            r'Database=MS_Analytics;'
            f'UID={uid};'
            f'PWD={pwd};'
            f'TRUSTED_CONNECTION={trusted_conn}')
        
        # print(' ')
        # print(' conn_str : ',conn_str)
        
        # Integrated Security = False;
        print(' ')
        #print(f"String Connection: {conn_str}")
        print(' ')
        
        self.cnxn = pyodbc.connect(conn_str)
        self.quoted = quote_plus(conn_str)
        self.engine=create_engine('mssql+pyodbc:///?odbc_connect={}'.format(self.quoted), fast_executemany=True)
        
        # print(' engine : ',self.engine)
        
        # print('Database connection succesfully')
    
    def getCurrentTime(self,format='%H:%M:%S'):
        return time.strftime(format,time.localtime())

        from sqlalchemy import event
        @event.listens_for(self.engine, 'before_cursor_execute')
        def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            if executemany:
                cursor.fast_executemany = True    

    def writeToDB(self,df,table_name,sql,**kwargs):
        schema = kwargs.get('schema', 'MS_Analytics.SOE') # Optional input
        suffixTmp = kwargs.get('suffixTmp','tmp') # Optional input
        behaviour = kwargs.get('behaviour','replace')
        chunks = kwargs.get('chunks', None)
        dtype = kwargs.get('dtype', None)
        if sql != '':
            table_name=table_name+suffixTmp
            #print(f'Table_name: {table_name}')
            #Print(f"Executing SQL statement :\"{sql}\"")
        
        with self.cnxn.cursor() as self.cursor:
            self.cursor.fast_executemany = True  #Set fast_executemany  = True
            start = time.time() #Note start time
            mySQLDB.log = f"Writting records in table {table_name}, time: {self.getCurrentTime()}\n"
            print(f"Writting records in table {table_name}, time: {self.getCurrentTime()}...")
            df.to_sql(table_name, self.engine, index=False, if_exists=behaviour, schema=schema, chunksize=chunks)
           # resultDB3=self.readfromDB(f'SELECT * FROM {schema}.{table_name}')
           # print(f'DataFrame db(resultDB3):{resultDB3.head()}')
           # print('Database contains {x} records'.format(x=resultDB3.shape[0]))
            
            if sql != '':
                mySQLDB.log = f"Executing \"{sql}\", time: {self.getCurrentTime()}\n"
                print(f"Executing \"{sql}\", time: {self.getCurrentTime()}...")
                self.cursor.execute(sql)
            end = time.time() #Note end time
            self.cnxn.commit() #Close the cursor and connection
            result = f'{len(df)} rows inserted in table\n{(end - start)/60} minutes elapsed'
            mySQLDB.log = f'{result}, time: {self.getCurrentTime()}\n'
            print(f'{result}, time: {self.getCurrentTime()}\n')
            #return result

    def callProcedure(self,sql,**kwargs):
        with self.cnxn.cursor() as self.cursor:
            self.cursor.fast_executemany = True  #Set fast_executemany  = True
            start = time.time() #Note start time
            self.cursor.execute(sql)
            end = time.time() #Note end time
            self.cnxn.commit() #Close the cursor and connection
            print(f'{(end - start)/60} minutes elapsed')
            mySQLDB.log = f'It took {(end - start)/60} minutes the execution of {sql}\n'
        return f'{(end - start)/60} minutes elapsed'

    def readfromDB(self,query,**kwargs):
        print('Reading from Database...')
        return pd.read_sql(query, self.engine)

    def truncateDBTable(self,table_name,**kwargs):
        schema = kwargs.get('schema', 'MS_Analytics.SOE') # Optional input
        behaviour = kwargs.get('behaviour','replace')
        print(f'Table_name: {table_name}')
        
        with self.cnxn.cursor() as self.cursor:
            self.cursor.fast_executemany = True  #Set fast_executemany  = True
            self.cursor.execute("TRUNCATE TABLE table_name")
            self.cnxn.commit() #Close the cursor and connection

    def buildReadQuery(self,workspace,tableName,materialList,materialKeyList,countryList,snapshotList,snapshotTypeList,**kwargs):
        tableName=workspace+'.'+tableName
        query=''
        if materialList!=None:
            query=self.addWhereClause(query,tableName,materialList,'MaterialID')
        if materialKeyList!=None:
            query=self.addWhereClause(query,tableName,materialKeyList,'MaterialIDKey')
        if countryList!=None:
                query=self.addWhereClause(query,tableName,countryList,'Country')
        if snapshotList!=None:
                query=self.addWhereClause(query,tableName,snapshotList,'Snapshot')
        if snapshotTypeList!=None:
                query=self.addWhereClause(query,tableName,snapshotTypeList,'SnapshotType')
        if query=='':
            query = 'SELECT * FROM {table};'.format(table=tableName)
            
        return query
    
    def addWhereClause(self,query,tableName,inputList,key):
        if query=='':
            query='SELECT * FROM {table} WHERE '.format(table=tableName)+key
        else:
            query=query.replace(';','')
            query+=' AND '+key
        
        if len(inputList)==1:
            inputList.append(inputList[0])
        keys=tuple(inputList)
        query+=' IN {x};'.format(x=keys)
        return query
    
    def getDeltaRecords(self,df,db,col,pk,val,dateList,**kwargs):
        
        if db.shape[0] == 0:
            return df
        else :

            if dateList!=None:
                for c in dateList:
                    db[c]=pd.to_datetime(db[c]).dt.floor('d') # Set time to 00:00:00
                    df[c]=pd.to_datetime(df[c]).dt.floor('d') # Set time to 00:00:00
            
            print(f"Column types in df {list(df)}: {str(df.dtypes)}")
            print(f"Column types in db {list(db)}: {str(db.dtypes)}")
            resultDF=df.merge(db,on=pk,how='left',suffixes=('','_db'))
            deltaDF=pd.DataFrame(columns=df.columns)
            
            for v in val:
                temp=resultDF.loc[resultDF[v]!=resultDF[v+'_db']] #Database Update required
                if (temp.shape[0]>0)&(deltaDF.shape[0]<resultDF.shape[0]):
                    deltaDF=pd.concat([deltaDF,temp])
                    deltaDF=deltaDF.drop_duplicates()
                    #self.download.writeFile(deltaDF,'C:\e2esc\soe\data\output\DeltaTest.csv','ABS','CSV')
            
            if deltaDF.shape[0]>0: 
                deltaDF=deltaDF.drop_duplicates()[df.columns]  
                if dateList!=None:
                    for c in dateList:
                        deltaDF[c]=deltaDF[c].apply(lambda x: x.date())
                    
            return deltaDF

    def performDBUpdate(self,df,workspace,tableName,sql,col,pk,val,**kwargs):
        materialList = kwargs.get('matl', None) # Optional input
        materialKeyList = kwargs.get('matlKey', None) # Optional input
        countryList = kwargs.get('country', None) # Optional input
        snapshotList = kwargs.get('snapshot', None) # Optional input
        snapshotTypeList = kwargs.get('snapshotType', None) # Optional input
        dateList = kwargs.get('datefields', None) # Optional input
        chunks = kwargs.get('chunks', None) # Optional input
        bh = kwargs.get('behaviour', 'replace') # Optional input
        suffix = kwargs.get('suffixTmp','tmp') # Optional input

        query=self.buildReadQuery(workspace,tableName,materialList,materialKeyList,countryList,snapshotList,snapshotTypeList)

        print("Query to execute: " + str(query))
        print("")

        resultDB=self.readfromDB(query)
        print(f'DataFrame db(resultDB):{resultDB.head()}')
        print("")
        print('Database contains {x} records'.format(x=resultDB.shape[0]))

        if chunks == None:
            updateDF=self.getDeltaRecords(df,resultDB,col,pk,val,dateList)
        #    self.download.writeFile(updateDF,OUTPUT+'BO Failed Record.csv',REL,CSV)
            print('Update required for {x} records'.format(x=updateDF.shape[0]))
            print('Writing to Database...')
            if updateDF.shape[0]>0:
                self.writeToDB(updateDF,tableName,sql,schema=workspace, behaviour=bh,suffixTmp=suffix)
        else:
            updateDF=pd.DataFrame()
            for start in range(0, df.shape[0],chunks):
                dfSubset = df.iloc[start:start + chunks].copy()
                updateSubsetDF=self.getDeltaRecords(dfSubset,resultDB,col,pk,val,dateList)
                print('Update required for {x} records'.format(x=updateSubsetDF.shape[0]))
                print('Writing to Database...')
                if updateSubsetDF.shape[0]>0:
                    self.writeToDB(updateSubsetDF,tableName,sql,schema=workspace, behaviour = bh,suffixTmp=suffix)
                    updateDF=pd.concat([updateDF,updateSubsetDF])
        resultDB2=self.readfromDB(query)
        print(f'(After update) DataFrame db(resultDB):{resultDB2.head()}')
        print('(After update) Database contains {x} records'.format(x=resultDB2.shape[0]))
        return updateDF 

    def performReadDB(self,workspace,tableName,**kwargs):
        materialList = kwargs.get('matl', None) # Optional input
        materialKeyList = kwargs.get('matlKey', None) # Optional input
        countryList = kwargs.get('country', None) # Optional input
        snapshotList = kwargs.get('snapshot', None) # Optional input
        snapshotTypeList = kwargs.get('snapshotType', None) # Optional input
        
        query=self.buildReadQuery(workspace,tableName,materialList,materialKeyList,countryList,snapshotList,snapshotTypeList)
        resultDB=self.readfromDB(query)
        print('Database contains {x} records'.format(x=resultDB.shape[0]))       
        return resultDB

     # Function to drop records from a table.
     # WARNING: Please, use carefully this function, if you don't provide  a predicate value, 
     # you will delete all the records of the table.  - Jose Eduardo Garcia 10/10/2023
    def deleteRecords(self,tblName,**kwargs):
        predicate = kwargs.get('predicate',None) #Optional input dict with this syntax {"fieldName operator":"Value"}, Examples: {"ID>","102"},  {"Name=":"'Jhon'"," AND LastName!=":"'Dalton'"}
        sqlSentence = None
        try:
            if predicate != None:
                sqlSentence= 'DELETE FROM '+tblName+' WHERE '
                for key,value in predicate.items():
                    sqlSentence += key + value
            else:
                sqlSentence= 'DELETE FROM '+tblName
        
            with self.cnxn.cursor() as self.cursor:
                self.cursor.fast_executemany = True  #Set fast_executemany  = True
                self.cursor.execute(sqlSentence)
                self.cursor.commit()
        except Exception as ex:
            mySQLDB.log = f'There was an error executing sql sentence: "{sqlSentence}". Exception Details: {type(ex)}:{ex}\n{traceback.format_exc()}'
            raise RuntimeError(f'There was an error executing sql sentence: "{sqlSentence}". Exception Details: {type(ex)}:{ex}\n{traceback.format_exc()}') from ex
        else:
            mySQLDB.log = f'Query: "{sqlSentence}" successfully executed.'
            print(f'Query:\n{sqlSentence}\nsuccessfully executed.')
    
    def getDistinctRecords(self,tblName,columnName,paramsDict:dict=None):
        records = []
        sqlSentence = ''
        try:
            if paramsDict == None:
                sqlSentence = f'SELECT DISTINCT {columnName} FROM {tblName}'
            else:
                sqlSentence = 'SELECT DISTINCT ' +columnName+' FROM ' + tblName + ' WHERE '
                for key, value in paramsDict.items():
                    if not 'between' in key:
                        sqlSentence += key + "'" + value + "'"
                    else:
                        sqlSentence += key +  value

            with self.cnxn.cursor() as self.cursor:
                self.cursor.fast_executemany = True  #Set fast_executemany  = True
                self.cursor.execute(sqlSentence)
                records = self.cursor.fetchall()
                self.cursor.commit()
        except Exception as ex:
            #self._logInfo += f'There was an error executing the sql sentence:\n{sqlSentence}\nException Details: {type(ex)}:{ex}\n'
            raise RuntimeError(f'There was an error executing the sql sentence:\n{sqlSentence}\nException Details: {type(ex)}:{ex}') from ex
        else:
            print(f'Query:\n{sqlSentence}\nsuccessfully executed, it returned {len(records)} records')
           #self.logInfo += f'Query:\n{sqlSentence}\nsuccessfully executed, it returned {len(records)} records\n'
        return records

    def getRecords(self,query):
        records = []
        sqlSentence = query
        try:
            with self.cnxn.cursor() as self.cursor:
                self.cursor.fast_executemany = True  #Set fast_executemany  = True
                self.cursor.execute(sqlSentence)
                records = self.cursor.fetchall()
                self.cursor.commit()
        except Exception as ex:
            #self._logInfo += f'There was an error executing the sql sentence:\n{sqlSentence}\nException Details: {type(ex)}:{ex}\n'
            raise RuntimeError(f'There was an error executing the sql sentence:\n{sqlSentence}\nException Details: {type(ex)}:{ex}') from ex
        else:
            print(f'Query:\n{sqlSentence}\nsuccessfully executed, it returned {len(records)} records')
           #self.logInfo += f'Query:\n{sqlSentence}\nsuccessfully executed, it returned {len(records)} records\n'
        return records