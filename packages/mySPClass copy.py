
import pandas as pd,io,os
from office365.sharepoint.files.file import File
from shareplum import Site
from shareplum import Office365
from shareplum.site import Version
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
try:
    from baselibrary.myFileClass import myFile as myFile
    from baselibrary.myDFClass import myDF as myDF
    from baselibrary.myLibraryConstants import myConstants as C
except:
    from myFileClass import myFile as myFile
    from myDFClass import myDF as myDF
    from myLibraryConstants import myConstants as C

class mySPDF:
    def __init__(self,recipientsFile,**kwargs):
        self.user=kwargs.get('user', C.FROM.value) # Optional input
        self.pathType = kwargs.get('pathType',C.REL.value) # Optional input
        self.getUser(recipientsFile)

        self.errorDF = pd.DataFrame(columns=['File'])
        self.errorSharepointWrite=myFile()
        
   #     d = {'Success': ['File loaded to Sharepoint']}
        self.successDF = pd.DataFrame(columns=['File'])
        self.successSharepointWrite=myFile()
    
    def getUser(self,recipientsFile,**kwargs):
        cols=['Role','Email','PWD','Report','msgType']
        tgt=cols
        recipients=myDF(recipientsFile,self.pathType,C.FILE.value,C.EXCEL.value,cols,tgt,1)
        recipientsDF=recipients.getDF()
        senderList=recipientsDF.loc[recipientsDF['Role']==self.user].values.tolist()
        self.user=senderList[0][1]
        self.pwd=senderList[0][2]

    def readFromSP(self,SP,spSite,spFolder,spFile,localFolder,**kwargs):
        prefix = kwargs.get('prefix', None) #Optional Input
        self.setCookies(SP,spSite)
        self.spFolder = self.site.Folder(spFolder)
        print('Sharepoint Folder for read = ',self.spFolder)
        self.readUrl=spSite+spFolder
        
        # print("Full URL: " + str(self.readUrl))
        #print(self.spFolder.files)

        for fileName in self.spFolder.files : 
            print(fileName)
            f = fileName['Name']
            if self.readFile(spFile,f)=='X':
                print('Reading file ',f)
                self.url = self.readUrl + fileName['Name'] + '?cid=' + fileName['UniqueId']
                self.authenticate()
                self.response = File.open_binary(self.ctx, self.url)
                if prefix != None:
                    localFile= localFolder + prefix + f 
                    print(localFile)
                else:
                    print(localFolder + f)
                    localFile= localFolder + f 
                self.moveToLocalFolder(localFile)

    def readAllFromSP(self,SP,spSite,spFolder,localFolder,**kwargs):
        prefix = kwargs.get('prefix', None) #Optional Input
        self.setCookies(SP,spSite)
        self.spFolder = self.site.Folder(spFolder)
        print('Sharepoint Folder for read = ',self.spFolder)
        self.readUrl=spSite+spFolder
         
        
        print("Full URL: " + str(self.readUrl))
        #print(self.spFolder.files)

        for fileName in self.spFolder.files : 
            print(fileName)
            f = fileName['Name']
            print('Reading file ',f)
            self.url = self.readUrl + fileName['Name'] + '?cid=' + fileName['UniqueId']
            self.authenticate()
            self.response = File.open_binary(self.ctx, self.url)
            if prefix != None:
                localFile= localFolder + prefix + f 
            else:
                localFile= localFolder + f 
            self.moveToLocalFolder(localFile)

    def readFile(self,file,f,**kwargs):
        if (file==f)|(file==''):
            return 'X'

    def authenticate(self,**kwargs):
        ctx_auth = AuthenticationContext(self.url)
        if ctx_auth.acquire_token_for_user(self.user, self.pwd):
            self.ctx = ClientContext(self.url, ctx_auth)
            web = self.ctx.web
            self.ctx.load(web)
            self.ctx.execute_query()
            print("Authentication successful")
        else:
            print("Authentication failed")
    
    def moveToLocalFolder(self,localFile,**kwargs):
        print('Moving File Name = ',localFile)
        with open(localFile, "wb") as local_file:
            local_file.write(self.response.content)
        #save data to BytesIO stream
        bytes_file_obj = io.BytesIO()
        bytes_file_obj.write(self.response.content)
        bytes_file_obj.seek(0) #set file object to start
        print('Move Completed')

        
    def setCookies(self,SP,spSite,**kwargs):#spSite
        #site = Site(sharepoint_site, version = Version.v365, authcookie = authcookie)
        #print("user")
        #user
        #print(self.user)
        #print("password")
        #password
        #print(self.pwd)
        #self.authcookie= Office365(SP, username=self.user, password=self.pwd).GetCookies()
        self.authcookie= Office365(SP, username="ISCBI001@bd.com", password=r"52SQ6I>*P+*63*Z").GetCookies()
        self.site= Site(spSite,version=Version.v365, authcookie=self.authcookie,timeout=999999)
        print(self.authcookie)
        #self.site = Site(spSite, authcookie=self.authcookie)


    def writeToSPFolder(self,SP,spSite,folder,file,localFolder,successFile,errorFile,**kwargs):#spSite,folder,file,localFolder
        self.setCookies(SP,spSite)
        self.spFolder = self.site.Folder(folder)
        pathKind = kwargs.get('pathType',C.REL.value) #OptionalInput
        fKind = kwargs.get('fType',C.CSV.value) #OptionalInput
        print('Sharepoint Folder for write = ',str(self.spFolder))

        Err=0
        if file == '':
            files = os.listdir(localFolder)
            for fileName in files :  
                print('Uploading File',fileName)
                f=localFolder+fileName
                with open(f, mode='rb') as rowFile:
                    fileContent = rowFile.read()
                    try:
                        
                        self.spFolder.upload_file(fileContent, os.path.basename(f))

                        print("Write to Sharepoint Completed")
                        
#                        print('z 3')                        
                        tempDF = pd.DataFrame(columns=['File'])                                 

                        tempDF['File']=[fileName]
                        
                        self.successDF=self.successDF.append(tempDF)

                        fileName=successFile        

                        self.successSharepointWrite.writeFile(self.successDF,fileName,pathKind,fKind)    

                    
                    except Exception as e:
                        Err=1
                        tempDF = pd.DataFrame(columns=['File'])
                        tempDF['File']=[fileName]
                        self.errorDF=self.errorDF.append(tempDF)  
                        fileName=errorFile      
                        self.errorSharepointWrite.writeFile(self.errorDF,fileName,pathKind,fKind)
                        print(e)
        else:
            print('Uploading file ', file)
            with open(file, mode='rb') as rowFile:
                fileContent = rowFile.read()
                try:
                    self.spFolder.upload_file(fileContent, os.path.basename(file))
                    print("Write to Sharepoint Completed")
                    tempDF = pd.DataFrame(columns=['File'])
                    print('tempDF='+tempDF)
                    tempDF['File']=[file]
                    print('tempDF2='+tempDF)
                    self.successDF=self.successDF.append(tempDF)  
                    fileName=successFile
                    fileName=fileName.replace('.xlsx','.csv')   
                    print(fileName) 
                    self.successSharepointWrite.writeFile(self.successDF,fileName,pathKind,fKind)    
                    
                except Exception as e:
                    Err=1
                    tempDF = pd.DataFrame(columns=['File'])
                    tempDF['File']=[file]
                    self.errorDF=self.errorDF.append(tempDF)  
                    fileName=errorFile     
                    fileName=fileName.replace('.xlsx','.csv') 
                    self.errorSharepointWrite.writeFile(self.errorDF,fileName,pathKind,fKind)    
                    print(e)       
        return Err
        
    def getFolderFilesList(self, SP, spSite, spFolder, columns):


        self.setCookies(SP,spSite)
        self.spFolder = self.site.Folder(spFolder)
        print('Sharepoint Folder for read = ',self.spFolder)
        self.readUrl=spSite+spFolder

        try:
            allfiles = self.spFolder.files
            df_SPfiles = pd.DataFrame(allfiles, columns = columns)
            return df_SPfiles
        except Exception as e:
            print("ERROR in retrieving folder files list. Error Message: " + str(e))
            exit()        

    def readDataFromExcelInSharePoint(self, SP, spSite, spLibrary, file):

        self.setCookies(SP,spSite)        
        
        try:
            self.spFolder = self.site.Folder(spLibrary)

            print("Getting " + file + " from SharePoint folder...")
            SPFile = self.spFolder.get_file(file)

            print("Reading Excel file...")
            data = io.BytesIO(SPFile) 

            return data
            
        except Exception as e:
            print("ERROR reading Excel file from SharePoint folder. Error Message: " + str(e))
            exit()
    
    def downloadFileFromSharepoint(self, SP, spSite, spLibrary, file):

        self.setCookies(SP,spSite)        
        
        try:
            self.spFolder = self.site.Folder(spLibrary)

            print("Getting " + file + " from SharePoint folder...")
            SPFile = self.spFolder.get_file(file)

            print("Reading file...")
            data = io.BytesIO(SPFile) 

            return data
            
        except Exception as e:
            print("ERROR downloading file from SharePoint folder. Error Message: " + str(e))
            exit()


    def readExcelFileFromFolder(self, SP, spSite, spLibrary, file, **kwargs):

        self.setCookies(SP,spSite)        

        vSheet    = kwargs.get('sheet', None) # Optional input
        vSkiprows = kwargs.get('skiprows', None) # Optional input
        vUseCols  = kwargs.get('usecols', None) # Optional input

        try:
            folder = self.site.Folder(spLibrary)

            print("Getting " + file + " from SharePoint folder...")
            SPFile = folder.get_file(file)

            print("Reading Excel file...")
            data = io.BytesIO(SPFile) 

            if vSheet != None:
                df_SPFile = pd.read_excel(data, sheet_name=vSheet,skiprows=vSkiprows,usecols=vUseCols)
            else:
                df_SPFile = pd.read_excel(data,skiprows=vSkiprows,usecols=vUseCols)
            df_SPFile = df_SPFile.fillna('')

            return df_SPFile

        except Exception as e:
            print("ERROR reading Excel file from SharePoint folder. Error Message: " + str(e))
            exit()
