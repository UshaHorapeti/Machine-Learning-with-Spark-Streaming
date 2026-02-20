import pandas as pd
try:
    from baselibrary.myLibraryConstants import myConstants as C
    from baselibrary.myFileClass import myFile as myFile
except:
    from myLibraryConstants import myConstants as C
    from myFileClass import myFile as myFile
#################################
#           git testing         #
#################################
def safeFloatConvert(x):
        try:
            float(x)
            return True # numeric, success!
        except ValueError:
            return False # not numeric
        except TypeError:
            return False # null type
    
class myDF:
    def __init__(self,path,pathType,dType,fType,cols,tgt,skip,**kwargs): 
       
# Added logic to initiate the object without a file - Vivek Jadhav 08/02/2022
        initWithoutFile=kwargs.get('noFile',None)
        
        if initWithoutFile==None:
#            print("x 1")
            self.path=path
            
#            print(str(self.path))

#            print("x 2")            
            self.pathType=pathType
#            print(str(self.pathType))

#            print("x 3")            
            self.dType=dType
#            print(str(self.dType))
        
#            print("x 4")
            self.fType=fType
#            print(str(self.fType))

#            print("x 5")            
            self.cols=cols
#            print(str(self.cols))

#            print("x 6")            
            self.tgt=tgt
#            print(str(self.tgt))            

#            print("x 7")            
            self.skip=skip
#            print(str(self.skip))

#            print("x 8")
            
            self.sheet = kwargs.get('sheet', None) # Optional input
#            print(str(self.sheet))

#            print("x 9")
            
            self.useCols = kwargs.get('usecols',None)  # Optional input
#            print(str(self.useCols))

#            print("x 10")
            
            self.useOriginalHeaders = kwargs.get('useOriginalHeaders', None) # Optional input
#            print(str(self.useOriginalHeaders))

#            print("x 11")
            
            self.addFileNameToDF = kwargs.get('addFileNameToDF', None) # Optional input
#            print(str(self.addFileNameToDF))

            #self.addFileNameToDF = kwargs.get('includeFileName',False) #Optional input 

#            print("x 12")
            
            self.dtype = kwargs.get('dtype', None)
#            print(str(self.dtype))

#            print("x 13")
            
            self.inputDF = pd.DataFrame()
#            print(self.inputDF)

#            print("x 14")
            
            self.outputDF = pd.DataFrame()
#            print(self.outputDF)

#            print("x 15")
            
            self.File=myFile(path=self.path,pathType=self.pathType,dType=self.dType)
#            print(self.File)

#            print("x 16")            

            self.path=self.File.path
#            print(str(self.path))

#            print("x 17")
            
            self.files=self.File.files
#            print(self.files)

#            print("x 18")
            
            self.inputDF=self.File.readData(self.fType,self.cols,self.skip,self.sheet,usecols=self.useCols,useOriginalHeaders=self.useOriginalHeaders,addFileNameToDF=self.addFileNameToDF,dtype=self.dtype)
#            print(self.inputDF)

#            print("x 19")            

            self.outputDF=self.File.buildDF(self.inputDF,self.tgt)
#            print(self.outputDF)

#            print("x 20")
            
            self.download=myFile()
#            print(self.download)

#            print("x 21")
                
    def getDF(self):
        return self.outputDF
    
    def buildKey(self,df,**kwargs):
        sku = kwargs.get('sku', None) # Optional input
        key = kwargs.get('key', 'MaterialID') # Optional input
        fill = kwargs.get('fill', 'SKU-') # Optional input
        newKey=key+'Key'
        dType=key+'Type'

        if sku != None:
            df[key]=df[sku]
        
        df.loc[df[key]=='',key]=C.BLANK.value
        strDF,floatDF=self.identifyFloat(df,key,dType)
        floatDF[newKey]=fill+floatDF[key].map(str).str.lstrip('0')
        strDF[newKey]=strDF[key].map(str)
        strDF[newKey]=strDF[key].str.upper()
        return pd.concat([floatDF, strDF])

    def buildUniqueKey(self,df,**kwargs):
        key = kwargs.get('key', 'MaterialID') # Optional input
        buKey = kwargs.get('buKey', 'BU') # Optional input

        newKey=key+'Key'
        
        df.loc[df[key]=='',key]=C.BLANK.value
        df.loc[df[buKey]=='',buKey]=C.BLANK.value

        df[buKey] = df[buKey].apply(self.mapBU)

        df[newKey] = df[buKey].map(str) + "|" + df[key].map(str)
        df[newKey] = df[newKey].str.upper()

        return df    
    
    def mapBU(self,val,**kwargs):
        val = str(val)
        if val == 'SRG':
            return 'SUR'
        elif val in (['Discontinued Operati', 'Discontinued Operations', 'DOP']):
            return 'DISC'
        else:
            return val

    def identifyFloat(self,df,key,dType):
        df.loc[:,dType]='STR'
        mask = df[key].map(safeFloatConvert)
        df.loc[mask,dType]='FLOAT'

        strDF=df.loc[df[dType]=='STR']
        floatDF=df.loc[df[dType]=='FLOAT']   
        return strDF,floatDF     

      
    def setFamily(self,df,familyDF,group,default,**kwargs):
        by = kwargs.get('by', None) # Optional input
        df[group]=default
        print("By: " + str(by))

        if by=='SKU':

            print('Product Family file had {x} records prior to dropping duplicates'.format(x=familyDF.shape[0]))
            skuDF=familyDF.drop_duplicates(subset='MaterialIDKey')
            print('Product Family file had {x} records after dropping duplicates'.format(x=skuDF.shape[0]))
            for index, row in skuDF.iterrows():
                #print(str(index))
                df.loc[(df['MaterialIDKey']==row['MaterialIDKey']),group]=row[group]
        
        elif by=='LXSKU':
            for index, row in familyDF.iterrows():
                if row['Key']=='L1':
                    df.loc[(df['ProductHierarchyL1']==row['ProductHierarchyL1']),group]=str(row[group])

                elif row['Key']=='L2':
                    df.loc[(df['ProductHierarchyL2']==row['ProductHierarchyL2']),group]=str(row[group])
            
                elif row['Key']=='L3': 
                    df.loc[(df['ProductHierarchyL3']==row['ProductHierarchyL3']),group]=str(row[group])
                    
                elif row['Key']=='SKU': 
                    df.loc[(df['MaterialIDKey']==row['MaterialIDKey']),group]=str(row[group])

        else:
        
            for index, row in familyDF.iterrows():
                
                if row['Key']=='L1':
                    df.loc[(df['ProductHierarchyL1']==row['ProductHierarchyL1']),group]=row[group]

                elif row['Key']=='L2':
                    df.loc[(df['ProductHierarchyL2']==row['ProductHierarchyL2']),group]=row[group]
            
                elif row['Key']=='L3': 
                    df.loc[(df['ProductHierarchyL3']==row['ProductHierarchyL3']),group]=row[group]
        return df

    def setMfgPlant(self,df,regionDF,output):
        temp=df.copy()
        regionList=regionDF.values.tolist()
        resultDF=pd.DataFrame()
        
        for rgn in regionList:
            temp.loc[(temp['RegionA']==''),'Region']=rgn[0]
            temp.loc[(temp['RegionA']==''),'MfgPlant']=temp['MfgPlantA']
            temp.loc[(temp['RegionA']==''),'MfgPlantName']=temp['MfgPlantNameA']
            resultDF=pd.concat([resultDF,temp.loc[(df['RegionA']=='')]])
        
        self.download.writeFile(resultDF,output+'RSTEP1.csv',C.REL.value,C.CSV.value)
        
        resultDF=resultDF.merge(df,left_on=['MaterialID','Region'],right_on=['MaterialID','RegionA'],how='left',suffixes=('','_r'))
        self.download.writeFile(resultDF,output+'RSTEP2.csv',C.REL.value,C.CSV.value)
        
        resultDF.loc[(resultDF['Region']==resultDF['RegionA_r']),'MfgPlant']=resultDF['MfgPlantA_r']
        resultDF.loc[(resultDF['Region']==resultDF['RegionA_r']),'MfgPlantName']=resultDF['MfgPlantNameA_r']
        resultDF=resultDF.loc[(resultDF['MfgPlant']!='')&(resultDF['MfgPlantName']!='')]
       # self.download.writeFile(resultDF,output+'RSTEP3.csv',C.REL.value,C.CSV.value)

        return resultDF
    