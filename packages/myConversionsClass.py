from cmath import isnan
#from curses import KEY_A1
from distutils import errors
import pandas as pd, numpy as np
import datetime as dt
from time import strptime
try:
    from baselibrary.myLibraryConstants import myConstants as C
    from baselibrary.myNotifyClass import myNotify as myNotify
    from baselibrary.myFileClass import myFile as myFile
    from baselibrary.myDFClass import myDF as myDF
except:
    from myLibraryConstants import myConstants as C
    from myNotifyClass import myNotify as myNotify
    from myFileClass import myFile as myFile
    from myDFClass import myDF as myDF

class myConversions:
    log = ''

    def __init__(self,report,outputFolder,recipientsFile,emailSettingsFile,pathType,**kwargs):
        self.report=report
        self.outputFolder=outputFolder+report
        self.pathType=pathType
        self.email=myNotify(recipientsFile,emailSettingsFile,pathType) #EMail Notifications object
        self.download=myFile() #File upload / download object

    @staticmethod
    def sortData(df,layout,sortKey,**kwargs):#df,layout,sortKey
        Asc = kwargs.get('Asc',True)
        return df.reindex(columns=layout).fillna(0).sort_values(by=sortKey,ascending=Asc)   
    
    @staticmethod
    def convertObjectToDatetime(df,colname,**kwargs): #colname
        onlyYear = kwargs.get('onlyYear',False) #Optional input
        getBDFiscalYear = kwargs.get('fiscalYear',False) #Optional input
        minValidYear = kwargs.get('minYear',0)
        if onlyYear == False:
            for c in colname:
                #df2=df[c]
                #df2.to_excel(f'C:\\e2esc\\soe\\log\\DateTimeColumnsValues.xlsx')
                df[c]=pd.to_datetime(df[c], errors = 'coerce').dt.floor('d') # Set time to 00:00:00
        else:
            for c in colname:
                df[c]=pd.to_datetime(df[c], errors = 'coerce')
                for i,row in df.iterrows():
                    try:
                        if isnan(row[c].year) or row[c].year < minValidYear:
                            row[c] = 0
                        else:
                            if getBDFiscalYear == False:
                                row[c] = row[c].year
                            else:
                                row[c] = myConversions.convertCYToFYDate(row[c].year,row[c].month).year
                    except Exception as ex:
                        print(f'There was an issue transforming {row[c]} to dateTime: {type(ex)}:{ex}')
                        myConversions.log+=f'There was an issue transforming \"{row[c]}\" to dateTime: {type(ex)}:{ex}\n'
                        row[c] = 0
                    df.at[i,c] = row[c]
        print(df.info())
        return df

    @staticmethod
    def convertStringToFloat(df,colname,**kwargs): 
        chars = kwargs.get('chars',[',','\\','$',')',' '])
        for c in colname:
            print('Converting...'+c)
            for i,row in  df.iterrows():
                for chr in chars:
                    row[c]=str(row[c]).replace(chr,'').replace('(', '-')
                #print(f'Row {i}: {row[c]}')
                df.at[i,c] = row[c]
            df.loc[df[c]=='',c]=0
            df[c]=df[c].astype(float)
            #df[c]=df[c].apply(lambda x: float(x))
        print(df.info())
        return df

    
    @staticmethod
    def convertStringToFloatV2(df,colname,**kwargs): 
        chars = kwargs.get('chars',[',','\\','$',')',' '])
        for c in colname:
            for i,row in  df.iterrows():
                for chr in chars:
                    row[c]=str(row[c]).replace(chr,'').replace('(', '-')
                #print(f'Row {i}: {row[c]}')
                try:
                    df.at[i,c] = float(row[c])
                except:
                    #print(f'\"{row[c]}\" in row {i} cannot be converted to float, therefore, its value will be zero.')
                    myConversions.log += f'\"{row[c]}" in row {i} cannot be converted to float, therefore, its value will be zero...\n'
                    df.at[i,c] = 0
        print(df.info())
        return df
    
    

    @staticmethod
    def cleanColumn(df, colname,**kwargs):
        characters = kwargs.get('chrs',' ')
        for c in colname:
            df[c]=df[c].apply(lambda x: x.strip(characters))
        print(df.info())
        return df
    
    @staticmethod
    def convertObjectToFloat(df,colname,**kwargs):
        for c in colname:
            df[c]=df[c].apply(lambda x: float(x))
        print(df.info())
        return df
    
    @staticmethod
    def convertObjectToInt(df,colname,**kwargs):
        for c in colname:
            df[c]=df[c].apply(lambda x: int(x))
        print(df.info())
        return df
    
    @staticmethod
    def convertIntToObject(df,colname,**kwargs):
        for c in colname:
            df[c]=df[c].apply(lambda x: str(x))
        print(df.info())
        return df
    
    @staticmethod
    def removeNegatives(df,colname,**kwargs):
        for c in colname:
            df=df.loc[df[c]>0]
        return df
    
    @staticmethod
    def convertIntToString(df,colname,**kwargs):
        leadingZeros = kwargs.get("leadingZeros",{"qty":0}) #Optional input
        for c in colname:
             for i,row in  df.iterrows():
                row[c] = str(row[c]).zfill(leadingZeros["qty"])
            #df[c]=df[c].map(str)
        print(df.info())
        return df
    
    #key=['MaterialIDKey','MaterialID','Country','Snapshot','SnapshotType','UOM']
    #val=['OrderQty','CancelledQty']    
    #print('Calculating Order qty...')
    #resultDF=convert.pivotSum(resultDF,key,val,'')
    @staticmethod
    def pivotSum(df,key,val,col,**kwargs):
        if col=='':
            resultDF=pd.pivot_table(df,index=key,
                                values=val,
                                aggfunc=np.sum).reset_index().fillna(0)
        else:
            resultDF=pd.pivot_table(df,index=key,
                                values=val, columns=col,
                                aggfunc=np.sum).reset_index().fillna(0)
        resultDF.columns = resultDF.columns.map(''.join)
        return(resultDF)

    @staticmethod
    def pivotMean(df,key,val,col,**kwargs):
        if col=='':
            resultDF=pd.pivot_table(df,index=key,
                                values=val,
                                aggfunc=np.mean).reset_index().fillna(0)
        else:
            resultDF=pd.pivot_table(df,index=key,
                                values=val, columns=col,
                                aggfunc=np.mean).reset_index().fillna(0)
        resultDF.columns = resultDF.columns.map(''.join)
        return(resultDF)
    
    @staticmethod
    def pivotCount(df,key,val,col,**kwargs):        
        if col=='':
            resultDF=pd.pivot_table(df,index=key,
                                values=val,
                                aggfunc='count').reset_index().fillna(0)
        else:
            resultDF=pd.pivot_table(df,index=key,
                                values=val, columns=col,
                                aggfunc='count').reset_index().fillna(0)
        resultDF.columns = resultDF.columns.map(''.join)
        return(resultDF)

    @staticmethod
    def mergeDF(df1,df2,key,joinType,**kwargs):
        Err=0
        col=key
        ignore_case = kwargs.get("ignore_case", False)
        
        if joinType=='left':
            dupsDF = df2[df2.duplicated(subset=col,keep=False)]
            if dupsDF.shape[0]>0:
                Err=99
                resultDF=pd.DataFrame()
            else:
                resultDF=df1.merge(df2,on=col,how=joinType,suffixes=('','_r'))
        else:
                resultDF=df1.merge(df2,on=col,how=joinType,suffixes=('','_r'))
        
        return(Err,resultDF)

    @staticmethod
    def replaceValues(df,colname,current,new):
        for c in colname:
            if current=='Null':
                df.loc[df[c].isnull(),c]=new
            else:
                df.loc[df[c]==current,c]=new
        return df

    @staticmethod
    def calcXYZ(df,col,**kwargs):
        X = kwargs.get('X',None)
        Y = kwargs.get('Y',None)
        df['Mean']=df.loc[:,col].mean(axis=1)
        df['StdDev']=df.loc[:,col].std(axis=1,ddof=0)
        df['COV']=9.00
        df.loc[df['Mean']>0,'COV']=df['StdDev']/df['Mean']
        df['XYZ']='Z'
        df.loc[(df['COV']>0)&(df['COV']<=X),'XYZ']='X'
        df.loc[(df['COV']>X)&(df['COV']<=Y),'XYZ']='Y'          
        return df

    @staticmethod
    def calcABC(df,fieldList,sortKey,val,groupBy,**kwargs):
        A = kwargs.get('A',None)
        B = kwargs.get('B',None)
       
        resultDF=df.reindex(columns=fieldList).fillna(0).sort_values(by=sortKey,ascending=False)
        resultDF.loc[resultDF[val]<0,val]=0

        if groupBy != None:
            resultDF['CumSum']=resultDF.groupby(groupBy)[val].cumsum()
            resultDF['Total']=resultDF.groupby(groupBy)[val].transform('sum')  
            resultDF['Percent']=resultDF[val]/resultDF['Total']
            resultDF['CumPercent']=resultDF['CumSum']/resultDF['Total']
        else:
            resultDF['CumSum']=resultDF[val].cumsum()
            resultDF['Total']=resultDF[val].sum()  
            resultDF['Percent']=resultDF[val]/resultDF['Total']
            resultDF['CumPercent']=resultDF['CumSum']/resultDF['Total']          
    
        resultDF['ABC']='C'
        resultDF.loc[(resultDF[val]>0)&(resultDF[val]==resultDF['CumSum']),'ABC']='A'
        resultDF.loc[(resultDF['CumPercent']>0)&(resultDF['CumPercent']<=A),'ABC']='A'
        resultDF.loc[(resultDF['ABC']!='A')&(resultDF['CumPercent']>A)&(resultDF['CumPercent']<=B),'ABC']='B'
        resultDF.loc[(resultDF['ABC']=='C')&(resultDF['Percent']>=(B-A)),'ABC']='B'           
        
        return resultDF

    @staticmethod
    def calcFCA(df,dmd,fcst,absErr,**kwargs):
        df['AbsErr %']=0
        df['FCA %']=0

        df['AbsErr %']=df[absErr]/df[dmd]
        df['FCA %']=1-df['AbsErr %']
        df.loc[df[dmd]==0,'FCA %']=0
        df.loc[df[dmd]==0,'AbsErr %']=0.99
        
        return df
    def mapSalesOrg(self,df,sorg,**kwargs):
        sorgPath = kwargs.get('sorgPath', None) # Optional input
        errorFile = self.outputFolder+'SalesOrgErrors.csv'
        df.loc[df[sorg]=='',sorg]=C.BLANK.value
        sorgDF=self.getSorgDF(sorgPath,self.pathType).drop_duplicates(subset='SalesOrg')
        resultDF=df.merge(sorgDF,left_on=sorg,right_on=['SalesOrg'],how='left',suffixes=('','_r'))

        resultDF.loc[resultDF['Region'].isnull(),'Region']=C.NOTMAPPED.value
        sorgErrorDF=resultDF.loc[resultDF['Region']==C.NOTMAPPED.value][sorg].drop_duplicates()
        if sorgErrorDF.shape[0]>0:
            self.download.writeFile(sorgErrorDF,errorFile,self.pathType,C.CSV.value)
            self.email.notify(2,self.report) 
        return resultDF

    def mapCountry(self,df,location,**kwargs):
        sitePath = kwargs.get('sitePath', None) # Optional input
        countryPath = kwargs.get('countryPath', None) # Optional input
        regionPath = kwargs.get('regionPath', None) # Optional input
        errorFile = self.outputFolder+'CountryErrors.csv'

        df.loc[df['Location']=='','Location']=C.BLANK.value
        
        if sitePath!=None:
            countryDF=self.getSiteDF(sitePath,self.pathType)
        elif countryPath!=None:
            countryDF=self.getCountryDF(countryPath,self.pathType)

    # Merge df and CountryDF    
        countryDF=countryDF.drop_duplicates(subset='Location')
        resultDF=df.merge(countryDF,left_on=[location],right_on=['Location'],how='left',suffixes=('','_r'))

        countryErrorDF=self.countryError(resultDF)
        if countryErrorDF.shape[0]>0:
            self.download.writeFile(countryErrorDF,errorFile,self.pathType,C.CSV.value)
            self.email.notify(2,self.report) 
        if regionPath!=None:
            resultDF=self.getRegion(resultDF,regionPath,self.pathType)
        return resultDF  

    def getCountryDF(self,countryPath,pathType,**kwargs):
        cols=['Location','Country']
        tgt=cols
        country=myDF(countryPath,pathType,C.FILE.value,C.CSV.value,cols,tgt,1)
        return country.getDF()
    
    def getSorgDF(self,sorgPath,pathType,**kwargs):
        cols=['SalesOrg','Region','RGN']
        tgt=cols
        sorg=myDF(sorgPath,pathType,C.FILE.value,C.CSV.value,cols,tgt,1)
        return sorg.getDF()
        
    def getSiteDF(self,sitePath,pathType,**kwargs):
        cols=['Location','Country','Type']
        tgt=cols
        country=myDF(sitePath,pathType,C.FILE.value,C.CSV.value,cols,tgt,1)
        return country.getDF()
    
    
    def countryError(self,df,**kwargs):
        df.loc[df['Country'].isnull(),'Country']=C.NOTMAPPED.value
        return df.loc[df['Country']==C.NOTMAPPED.value]['Location'].drop_duplicates()
    
    def getRegion(self,df,regionPath,pathType,**kwargs):
        cols=['Country','Region']
        tgt=cols
        region=myDF(regionPath,pathType,C.FILE.value,C.CSV.value,cols,tgt,1)
        regionDF=region.getDF()
        return df.merge(regionDF,on='Country',how='left',suffixes=('','_r'))   
    
    @staticmethod
    def searchSequentially(df,col,sequence,**kwargs):#df,year
        df[col]=0
        for c in sequence:
            df.loc[df[col]==0,col]=df[c]
        return df
    
    @staticmethod
    def filterMaxValue(df,col,**kwargs):
        matlLevel = kwargs.get('matlLevel', None) # Optional input
        if matlLevel!=None:
            filterDF=df[['MaterialIDKey',col]]
            maxValueDF=filterDF.groupby(by=['MaterialIDKey',col],as_index=False).max()            
            #self.download.writeFile(maxValueDF,'C:\e2esc\soe\data\output\HermesJob1TEST3.csv',C.ABS.value,C.CSV.value)
            resultDF=df.merge(maxValueDF,on='MaterialIDKey',how='left',suffixes=('','_max'))
            resultDF=resultDF.loc[resultDF[col]==resultDF[(col+'_max')]].drop([(col+'_max')],axis=1)
        else:
            maxValue=df[col].drop_duplicates().max()
            resultDF=df.loc[df[col]==maxValue]
        return resultDF
    
    @classmethod
    def convertListToString(cls,dataList:list,**kwargs):
        delimiter = kwargs.get("delimiter",",")
        enclosedChars = kwargs.get("enclosedChars",{"opener":"","closer":""}) #e.g {"opener":"[","closer":"]"}
        outputString = ""
        o =  enclosedChars.get("opener")
        c =  enclosedChars.get("closer")
        counter = 0
        for element in dataList:
            if counter < len(dataList) -1:
                    outputString += o + element + c + delimiter
            else:
                outputString += o + element + c
            counter += 1
        return outputString

    #Err=convert.validate(summaryDF,resultDF,['MaterialIDKey'])
    def validate(self,beforeDF,afterDF,key,**kwargs):
        tol = kwargs.get('tol', None) # Optional input
        totals = kwargs.get('totals', None) # Optional input
        if tol==None:
            tol=0.001 
        Err=0
        if afterDF.shape[0]>0:
            listVal=list(beforeDF.columns)
            print(f'List Val: {listVal}')
            for k in key:
                listVal.remove(k)
            pivotDF=pd.pivot_table(afterDF,index=key,values=listVal,aggfunc=np.sum).reset_index().fillna(0)
            print(f'pivotDF:\n {str(pivotDF)}')
            diffDF=beforeDF.merge(pivotDF,on=key,how='left',suffixes=('-Before','-After')).fillna(0)
            print(f'diffDF:\n {str(diffDF)}')
            i=0
            for col in listVal:
                if totals==None:
                    print(f'Totals ({totals}) was None')
                    diffDF[(col+'-Delta')]=(abs(diffDF[(col+'-After')]-diffDF[(col+'-Before')]))/diffDF[(col+'-Before')]
                    print(f'diffDF:\n {str(diffDF)}')
                    percentError=(diffDF[(col+'-Delta')].sum())/(diffDF[(col+'-Before')].sum())
                    print(f'percentError :{str(percentError)}\n')
                else:
                    print(f'Totals ({totals}) was not None')
                    percentError=abs((diffDF[(col+'-After')].sum()-diffDF[(col+'-Before')].sum())/diffDF[(col+'-Before')].sum())
                    print(f'percentError :{str(percentError)}\n')
                    print(f'tol :{str(tol)}\n')
                if percentError>tol:
                    i+=1
                    print(f'i: {i}')
                    download=myFile()
                    errDF=diffDF[[k,(col+'-Before'),(col+'-After'),(col+'-Delta')]]
                    print(f'errDF:\n {str(errDF)}')
                    download.writeFile(errDF,self.outputFolder+' ValidationErrors'+str(i)+'.csv',self.pathType,C.CSV.value)
                    Err+=99
                    print(f'Err: {Err}')                  
        return Err

    def validateTotals(self,beforeDF,afterDF,**kwargs):
        tol = kwargs.get('tol', None) # Optional input
        if tol==None:
            tol=0.001 
        Err=0
        if afterDF.shape[0]>0:
            for col in afterDF.index:
                if col in beforeDF.index:
                    Err=(beforeDF[col]-afterDF[col])/beforeDF[col]
                    if Err>tol:
                        Err+=99
                    else:
                        Err=0
        return Err

    def getTotalValue(self,df,yearDF,mthDF,**kwargs):
        FileTotal=0
        for index, row in yearDF.iterrows():
            tempDF=df.loc[df['Year']==row['Year']]
            months=(mthDF.loc[mthDF['Year']==row['Year']])['Month'].values.tolist()
            tempDF['Sum']=tempDF[months].sum(axis=1)
            FileTotal+=tempDF['Sum'].sum()
        return FileTotal
    
    def getBeforeTotal(self,df,val,**kwargs):
        beforeDF=df
        beforeDF['Sum']=beforeDF[val].sum(axis=1)
        return(beforeDF['Sum'].sum())

    def validateSum(self,df,col,total1,total2,**kwargs):
        tol = kwargs.get('tol', None) # Optional input
        if tol==None:
            tol=0.001 
        Err=0
        
        #Validate total1 and total2
        Err=(total1-total2)/total1
        if Err>tol:
            Err=99
        else:
            Err=0        
    
        #Validate df
        Err=(total1-df[col].sum())/total1
        if Err>tol:
            Err=99
        else:
            Err=0 
        
        return Err

    #weekMthPath=C.INPUT.value+'configuration\\HermesCalendarNew.csv'
    #hermesKey=['MaterialIDKey','Country']
    #MTH=['Oct','Nov','Dec','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep']
    #MONTHLY='MONTHLY'
    #getMonthlyHermesForecast(resultDF,hermesKey,C.MONTHLY.value,C.MTH.value,weekMthPath,
     #                       sumdf=summaryDF,min=9,max=12)
    def getMonthlyHermesForecast(self,df,key,SnapshotType,mthList,weekMthPath,**kwargs):
        sumDF = kwargs.get('sumdf', None) # Optional input      
        min = kwargs.get('min', None) # Optional input      
        max = kwargs.get('max', None) # Optional input      
        resultDF=pd.DataFrame()

        weekMthDF=self.getWeekMthDF(weekMthPath,self.pathType)
        print(f'weekMthDF:\n{weekMthDF.tail(10)}\n')
        calDF=weekMthDF[['Mth','Month','FiscalYear']].drop_duplicates()
        calDF=self.convertObjectToDatetime(calDF,['Month'])
        print(f'calDF:\n{calDF.tail(10)}\n')
        print(f'mthList\n: {mthList}\n')
        
        fyList = df['FiscalYear'].drop_duplicates()
        print(fyList.value_counts())
        for fy in fyList:
            for mth in mthList:
                print(f'df[{mth}]:{df[mth].sum()}\n')
                #if df[mth].sum()>0:
                if df.loc[df['FiscalYear'] == fy][mth].sum()>0:
                    temp=df.loc[df['FiscalYear'] == fy][key]
                    print(f'temp:\n{temp.tail(10)}\n')
                    print(f"calDF[Mth]:\n{calDF['Mth'].tail(10)}\n")
                    print(f"calDF[Mth={mth}]:\n{calDF.loc[calDF['Mth']==mth].tail(10)}\n")
                    temp['Snapshot']=calDF.loc[(calDF['Mth']==mth) & (calDF['FiscalYear'] == fy)]['Month'].item()
                    temp['SnapshotType']=SnapshotType
                    # temp['HermesForecast']=df[mth] 
                    temp['HermesForecast']=df.loc[df['FiscalYear'] == fy][mth]
                    print('Sum of Fcst for ',mth,' = ',df[mth].sum())
                    resultDF=pd.concat([resultDF,temp])
        
        print("CHECK HERE: /n")
        print(resultDF.FiscalYear.value_counts())
        #self.download.writeFile(resultDF,self.outputFolder+' BEFORE FILTER' + '.csv',self.pathType,C.CSV.value)

        resultDF,summaryDF=self.filterSnapshots(resultDF,sumDF,key,calDF,min,max)
        return resultDF,summaryDF
    
    def filterSnapshots(self,df,summaryDF,key,calDF,min,max,**kwargs):
        resultDF=pd.DataFrame(columns=df.columns)
        mthList=[]
        if min!=None:
            filterDF=df[['Snapshot']].drop_duplicates().sort_values(by='Snapshot',ascending=True)
            i=0
            for index,row in filterDF.iterrows():
                i+=1
                if (i>=min) & (i<=max):
                    temp=df.loc[df['Snapshot']==row['Snapshot']]
                    mthList.append(calDF.loc[calDF['Month']==row['Snapshot']]['Mth'].item())
                    resultDF=pd.concat([resultDF,temp])
                    
            
            sumDF=pd.DataFrame(columns=summaryDF.columns)
            for k in key:
                if (k in summaryDF.columns):
                    sumDF[k]=summaryDF[k]

            for m in mthList:
                sumDF[m]=summaryDF[m]
                
            summaryDF=sumDF.fillna(0)    

            return resultDF,summaryDF

    def getWeeklyHermesForecast(self,df,SnapshotType,weekMthPath,pathType,**kwargs):
        resultDF=pd.DataFrame()

        weekMthDF=self.getWeekMthDF(weekMthPath,pathType)
        calDF=weekMthDF[['Week','Factor','Month']].drop_duplicates()
        calDF=self.convertObjectToDatetime(calDF,['Month','Week'])

        for index, row in calDF.iterrows():
            temp=df.loc[df['Snapshot']==row['Month']]
            if temp['HermesForecast'].sum()>0:
                temp['Snapshot']=row['Week']
                temp['SnapshotType']=SnapshotType
                temp['HermesForecast']=temp['HermesForecast']/row['Factor']
                resultDF=pd.concat([resultDF,temp])
        resultDF=pd.concat([df,resultDF])
        return resultDF

    def getWeekMthDF(self,weekMthPath,pathType,**kwargs):
        cols=['Week','Month','Factor','Mth', 'FiscalYear']
        tgt=cols
        weekMth=myDF(weekMthPath,pathType,C.FILE.value,C.CSV.value,cols,tgt,1)
        return weekMth.getDF()

    def addPeriods(self,df,mth,**kwargs):
        calPath = kwargs.get('calPath', None) # Optional input   
        cols=[mth,'Period']
        tgt=cols
        periods=myDF(calPath,self.pathType,C.FILE.value,C.CSV.value,cols,tgt,1)
        periodsDF=periods.getDF().drop_duplicates()
        periodsDF=self.convertObjectToDatetime(periodsDF,[mth])
        return df.merge(periodsDF,on=mth,how='left',suffixes=('','_r'))

    def sumColumns(self,df,col1,col2,**kwargs):
        for c1 in col1:
            df[c1]=0 #Initialize value
            for c2 in col2:
                c=c1+c2
                df[c1]+=df[c] 
        return df
    
    def totalRevenueFigures(self,df,uom,key,periods,proj):
        for k in key:
            colSum=k+uom
            df[colSum]=df[('PRJ'+uom)]=0
            for p in periods:
                if k=='PRJ':
                    df['PRJ'+uom]+=df[(proj+uom+p)]
                else:
                    df[colSum]+=df[(colSum+p)]
        return df

    def HermesBeforeTotal(self,df,key,col,**kwargs):
        resultDF=df[key]
        resultDF['Total']=0
        for c in col:
            resultDF['Total']+=df[c]
        return resultDF

    def HermesAfterTotal(self,df,key,val,snapshotType,**kwargs):
        resultDF=df.loc[df['SnapshotType']==snapshotType]
        resultDF=self.pivotSum(resultDF,key,val,'').rename(columns={val:'Total'})
        return resultDF

    def addSnapshotType(self,df,snapshotType,**kwargs): 
        df['SnapshotType']=snapshotType
        return df

    def boAgeCategory(self,df,age,agedText,currentText,**kwargs):
        #Populate AgeCategory
        resultDF=df
        resultDF.loc[resultDF['AvgAgeInDays']>=age,'AgeCategory']=agedText
        resultDF.loc[resultDF['AvgAgeInDays']<age,'AgeCategory']=currentText
        return resultDF    
 
    def sumKey(self,df,val,col,**kwargs):
        resultDF=df
        for v in val:
            resultDF[v]=0
            for c in col:
                resultDF[v]+=resultDF[v+c]
        return resultDF

    def addSnapshot(self,df,period,date,**kwargs):
        resultDF=df
        if period==C.MONTHLY.value:
            resultDF['Snapshot']= resultDF[date].to_numpy().astype('datetime64[M]')
        elif period==C.WEEKLY.value:
            # Handle nulls
            nonullDF = resultDF.loc[~(resultDF[date].isnull())]
            nullDF = resultDF.loc[(resultDF[date].isnull())]
            nonullDF['Snapshot']= nonullDF[date] - nonullDF[date].dt.weekday.astype('timedelta64[D]')

            resultDF = pd.concat([nonullDF, nullDF])
        return resultDF

    def convertUOM(self,df,uomDF,key,**kwargs):
        #convertKey=['MaterialIDKey','Country','Snapshot','SnapshotType','UOM','FinalFactor']
         # Optional parameter to use a different field for joining to UOM data

         #convertKey=['MaterialID','Country','Snapshot','SnapshotType','UOM','FinalFactor']
        #uomConversionDF = convert.convertUOM(resultDF,uomDF,convertKey,keyField='MaterialID')
        keyField = kwargs.get("keyField","MaterialIDKey")
        
                
        uomDF=self.prepareUOM(uomDF)

        #Set uppercase to the keyfiels of the dataframes to merge.
        uomDF[keyField] = uomDF[keyField].str.upper()
        df[keyField] = df[keyField].str.upper()

        print("DataFrame UOM:")
        print(uomDF.head())
        print("DataFrame to join with UOM: ")
        print(df.head())
        print("Starting merging...")
        
        resultDF=df.merge(uomDF,left_on=[keyField,'UOM'],
            right_on=[keyField,'from'],how='left',suffixes=('','_step1'))

        resultDF.loc[resultDF['UOM']=='EA','FinalFactor']=1
        
        resultDF.loc[(resultDF['FinalFactor'].isnull())&(resultDF['UOM']==resultDF['from']),
                    'FinalFactor']=resultDF['factor']    
                
        resultDF=resultDF.merge(uomDF,left_on=[keyField,'UOM'],
            right_on=[keyField,'to'],how='left',suffixes=('','_step2'))

        resultDF.loc[(resultDF['FinalFactor'].isnull())&(resultDF['UOM']==resultDF['to_step2'])
                    ,'FinalFactor']=1/resultDF['factor_step2']  

        resultDF.loc[resultDF['FinalFactor'].isnull(),'FinalFactor']=1

        resultDF=resultDF.loc[resultDF['FinalFactor']!=1]
        print(resultDF.head()) 
        return resultDF[key]
    
    def prepareUOM(self,uomDF):
        uomDF=uomDF[(uomDF['factor']!=1)]
        uom1DF=uomDF[(uomDF['from']=='EA')&(uomDF['to']!='EA')]
        uom2DF=uomDF[(uomDF['from']!='EA')&(uomDF['to']=='EA')]
        return pd.concat([uom1DF,uom2DF])

    def calculateIncomingOrders(self,df,uomDF,key):
        resultDF=df.merge(uomDF,on=key,how='left')
        resultDF.loc[resultDF['FinalFactor'].isnull(),'FinalFactor']=1
        resultDF['IncomingOrders']=(resultDF['OrderQty']-resultDF['CancelledQty'])*resultDF['FinalFactor']
        return resultDF
    
    def setNullDefaults(self,df,col,value):
        for c in col:
            df.loc[(df[c].isnull()),c]=value
        return df

    def convertFYToCYDate(self, year, month):
        vYear = int(year)
        vMonthNumber = strptime(month, "%b").tm_mon
        if vMonthNumber > 9 :
            return dt.date(vYear - 1, vMonthNumber,1)
        else:
            return dt.date(vYear, vMonthNumber,1)

    @staticmethod
    def convertCYToFYDate(year, month, day = 1):
        vYear = int(year)
        vMonthNumber = month#strptime(month, "%b").tm_mon
        if vMonthNumber > 9 :
            return dt.date(vYear + 1, vMonthNumber,day)
        else:
            return dt.date(vYear, vMonthNumber, day)
    

    def dateToFiscalYear(self, date):
        vYear = int(date.year)
        vMonth = int(date.month)
        if vMonth > 9 :
            return vYear + 1
        else:
            return vYear
