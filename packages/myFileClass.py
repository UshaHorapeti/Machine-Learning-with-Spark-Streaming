from multiprocessing.sharedctypes import Value
from posixpath import abspath
import os,pandas as pd, datetime, shutil
from pathlib import Path as Path
from os import path
import traceback
try:
    from myLibraryConstants import myConstants as C
except:
     from myLibraryConstants import myConstants as C

class myFile:
    def __init__(self,**kwargs): #path,pathType,dType
       
        self.path = kwargs.get('path', None) # Optional input
        self.pathType = kwargs.get('pathType', None) # Optional input
        self.dType = kwargs.get('dType', None) # Optional input
        self.debug = kwargs.get('debug', None) # Optional input
        self.debugCount=0
        
        if self.path!=None:
            self.path=self.setPath(self.path,self.pathType)
            self.files=self.getFiles(self.dType)
    
    
    def getFileName(self):
        return self.path.split(os.sep)[-1]

    def writeTxtFile(self,content:str,mode='w',encoding='utf8',overwrite=True):
        try:
            if overwrite:
                if(os.path.exists(self.path)):
                    os.remove(self.path)
            with open(self.path,mode=mode,encoding=encoding) as file:
                file.write(content)
        except Exception as ex:
            print(f'There was an issue trying to write the file: {self.path}\n{type(ex)}:{ex}\n{traceback.format_exc()}')
        else:
            print(f'The file {self.path} was succesfully created.')

        
    def setPath(self,path,pathType,**kwargs):
        if pathType==C.REL.value:
            root = os.getcwd().split(os.sep)[:-1]
            fileName=os.sep.join(root)+path
        elif pathType==C.ABS.value:
            fileName=path
        return fileName
    

    def getFiles(self,dType,**kwargs):
        if dType==C.FOLDER.value:
            files=os.listdir(self.path)
        elif dType==C.FILE.value:
            files=[self.path]
        else:
            files=[]
        return files
    
    def move(self,toPath,pathType,dType,**kwargs):
        toPath=self.setPath(toPath,pathType)

        self.selectFiles=kwargs.get('selectFiles', None) # Optional input
        self.keyword=kwargs.get('keyword',None) # Optional input

        if self.selectFiles!=None:
            set1 = set(self.files)
            intersection = set1.intersection(self.selectFiles)
            self.moveFiles = list(intersection)
            print(self.moveFiles)
        
        if self.keyword!=None:
            set1=set(self.files)
            self.moveFiles = [s for s in self.files if self.keyword in s]
            print(self.moveFiles)
                
        if dType==C.FOLDER.value:
            if (self.selectFiles==None)&(self.keyword==None): #User wants to move all files
                self.moveFiles=self.files
            for file in self.moveFiles:
                fromFile=self.path+file
                toFile=toPath+file
                os.replace(fromFile,toFile)
        elif dType==C.FILE.value:
            fromFile=self.path
            toFile=toPath
            os.replace(fromFile,toFile)
    
    def readData(self,fType,cols,skip,sheet,**kwargs):
        total=0
        resultDF=pd.DataFrame(columns=cols)
        useCols = kwargs.get('usecols',None) #Optional input
        useOriginalHeaders = kwargs.get('useOriginalHeaders',None)
        addFileNameToDF = kwargs.get('addFileNameToDF', None)

        if useOriginalHeaders == True:
            cols=None
            header=0
        else:
            header=None         

        for file in self.files:
            f=''
            if self.dType==C.FOLDER.value:
                if os.path.isfile(os.path.join(self.path,file)):
                    f=os.path.join(self.path,file)
            elif self.dType==C.FILE.value:
                if os.path.isfile(file):
                    f=file

            if f != '': 
                print('Reading file ', f)
                if fType == C.CSV.value:
                    temp=pd.read_csv(f,skiprows=skip,
                    header=header,names=cols,keep_default_na=False,encoding=C.ISO.value,usecols=useCols)
                elif fType == C.EXCEL.value:
                    if sheet==None:
                        temp=pd.read_excel(f,skiprows=skip,header=header,names=cols,keep_default_na=False,usecols=useCols)
                    else:
                        temp=pd.read_excel(f,skiprows=skip,header=header,names=cols,sheet_name=sheet,keep_default_na=False,usecols=useCols)
                print('File Read Rows =', temp.shape[0])
                total=total+temp.shape[0]

                if addFileNameToDF is True:
                    temp["FileName"] = file
                
                resultDF=pd.concat([resultDF, temp],ignore_index=True)
               # resultDF = resultDF.append(temp,ignore_index=True)
        
        print('Total Rows =', total)
        return resultDF

    def getFileCount(self,**kwargs):#path,pathType
        folderName=self.setPath(self.path,self.pathType)
        files=os.listdir(folderName)
        return len(files)
    
    @staticmethod
    def checkErrorFile(errorFile,**kwargs):
        root = os.getcwd().split(os.sep)[:-1]
        file=os.sep.join(root)+errorFile
        if os.path.isfile(file):
            return True
        else:
            return False

    @staticmethod
    def buildDF(inputDF,tgt,**kwargs):
        dropDuplicates = kwargs.get('dropDuplicates', None) # Optional input
        if inputDF.shape[0]>0:
            if dropDuplicates!=None:
                outputDF=inputDF[tgt].drop_duplicates()
            else:
                outputDF=inputDF[tgt]            
        else:
            outputDF=pd.DataFrame()
        return outputDF
    
    def writeFile(self,df,file,pathType,fType,**kwargs):
        sheet = kwargs.get('sheet', None) # Optional input
        id = kwargs.get('id', None) # Optional input
        chunks = kwargs.get('chunks', None) # Optional input
        
        if chunks == None:
            fileName=self.setPath(file,pathType)
            if id==None:
                id=False
            if sheet==None:
                sheet='Sheet1'
            if fType==C.CSV.value:
                df.to_csv(fileName, index=id)
            elif fType==C.EXCEL.value:
                df.to_excel(fileName, sheet_name=sheet,engine='xlsxwriter',index=id)
            print('Total rows downloaded to file =',df.shape[0])
        
        else:
            for start in range(0, df.shape[0], chunks):
                dfSubset = df.iloc[start:start + chunks].copy()
                fileSubset=file.split('.')[-2]+'_chunk_'+str(start)+'.'+file.split('.')[-1]
                fileName=self.setPath(fileSubset,pathType)
                if id==None:
                    id=False
                if sheet==None:
                    sheet='Sheet1'
                if fType==C.CSV.value:
                    dfSubset.to_csv(fileName, index=id)
                elif fType==C.EXCEL.value:
                    dfSubset.to_excel(fileName, sheet_name=sheet,engine='xlsxwriter',index=id)
                print('Total rows downloaded to file =',dfSubset.shape[0])
                #print(start,dfSubset.shape[0])
                #print(file.split('.')[-2]+'_chunk_'+str(start)+'.'+file.split('.')[-1])


    def debugWrite(self,df,text,debugDates,debugFolder,**kwargs):
        if self.debug=='X':
            if ('Snapshot' in df.columns):
                debugSnapshots=[]
                for d in debugDates: 
                    debugSnapshots=pd.concat([debugSnapshots,pd.to_datetime(d)])
                   # debugSnapshots.append(pd.to_datetime(d))
                debugDF=df.loc[df['Snapshot'].isin(debugSnapshots)]
            else:
                debugDF=df
        
            self.debugCount+=1
            file=debugFolder+'DEBUG '+text+str(self.debugCount)+'.csv'
            print('Writing file',file)
            self.writeFile(debugDF,file,C.REL.value,C.CSV.value)

    def getLatestSubDirByCreateDate(self,**kwargs):
        returnType = kwargs.get('returnType', None) # Optional Input
        print(returnType)
        subfolder = max(Path(self.path).glob('*/'), key=os.path.getmtime)

        if returnType == C.REL.value:
            return subfolder.name
        else:
            return subfolder
    
    def createDirectory(self,path,directoryName,**kwargs):
        #path = kwargs.get('path', None) # Optional input
        pathType = kwargs.get('pathType', None) # Optional input
        if os.path.isdir(path):
            if os.path.exists(path.strip('\\')+'\\'+directoryName.strip('\\')):
                print("Folder already exists: "+path.strip('\\')+"\\"+directoryName.strip('\\'))
            else:
                os.mkdir(path.strip('\\')+'\\'+directoryName.strip('\\'))
        else:
            print("Failure-Invalid directory: "+path.strip('\\')+"\\"+directoryName.strip('\\'))
    
    def cleanup(self,cleanupList):
        for c in cleanupList:
            report=c[0]
            src=c[1]
            dst=c[2]
            cleanupDays=c[3]
            keep=c[4]
            if keep<0:
                self.performCleanupDays(report,src,dst,cleanupDays)
            else:
                self.performCleanupKeep(report,src,dst,keep)

    def performCleanupDays(self,report,src,dst,cleanupDays):   
        if not os.path.exists(dst):
                os.makedirs(dst)
        files = [i for i in os.listdir(src) if path.isfile(path.join(src, i))]
        for f in files:
            srcname=path.join(src, f)
            dstname=path.join(dst, f)
            d=os.path.getmtime(srcname)
            t=datetime.datetime.now().timestamp()
            delta=(t-d)/86400
            if delta > cleanupDays:
                shutil.move(srcname, dstname)
        print('Files older than {o} days for {x} report have been successfully moved'.format(
                    o=cleanupDays, x=report)) 

    def performCleanupKeep(self,report,src,dst,keep):   
        if not os.path.exists(dst):
                os.makedirs(dst)
        files = [i for i in os.listdir(src) if path.isfile(path.join(src, i))]
        files = [path.join(src, i) for i in files]
        files = sorted(files, key = os.path.getmtime,reverse=True)
        counter=0
        for f in files:
            filename = os.path.basename(f)
            srcname=f
            dstname=path.join(dst, filename)
            if counter >= keep:
                shutil.move(srcname, dstname)
            counter+=1
        print('{o} files retained for {x} report'.format(o=keep, x=report))
    
    def readExcelFile(self,fType,**kwargs):
        vSheet = kwargs.get('sheet', None) # Optional input
        vSkiprows = kwargs.get('skiprows', None) # Optional input
        headerRow = kwargs.get('header',0)
        df = pd.DataFrame()
        try:
            if self.dType == C.FILE.value:
                if fType == C.CSV.value:
                    df = pd.read_csv(self.path,skiprows=vSkiprows,keep_default_na=False,encoding=C.ISO.value)
                elif fType == C.EXCEL.value:
                    if vSheet==None:
                        df = pd.read_excel(self.path,skiprows=vSkiprows,header=headerRow,keep_default_na=False)
                    else:
                        df = pd.read_excel(self.path,skiprows=vSkiprows,header=headerRow,keep_default_na=False,sheet_name=vSheet)
                else:
                    print(f'Unsupported file type, it should be a csv or excel file')
            else:
                print(f'The file path should be point to a file, not to a folder.')
        except Exception as ex:
            print(f'There was an error: {type(ex)}:{ex}')
        else:
            print(f'Successful reading of {self.path}')
        return df