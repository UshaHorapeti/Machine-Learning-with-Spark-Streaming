import pandas as pd
import io
import os
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
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
    def __init__(self, recipientsFile, **kwargs):
        # Use client credentials instead of user credentials
        self.client_id = "24bc3c08-2bb5-4433-8e98-65f9cf82f76c"
        self.client_secret = "UwM8Q~Hh85AZxOzulpoVMkf66bp52KpWplA5Fdrk"
        self.tenant_id = "94c3e67c-9e2d-4800-a6b7-635d97882165"
        
        # Keep original properties
        self.user = kwargs.get('user', C.FROM.value)
        self.pathType = kwargs.get('pathType', C.REL.value)
        self.ctx = None
        self.url = None
        self.response = None
        self.site = None
        self.spFolder = None
        self.readUrl = None
        
        # Original error handling objects
        self.errorDF = pd.DataFrame(columns=['File'])
        self.errorSharepointWrite = myFile()
        
        self.successDF = pd.DataFrame(columns=['File'])
        self.successSharepointWrite = myFile()
        
        # Call getUser to maintain original flow
        self.getUser(recipientsFile)

    def getUser(self, recipientsFile, **kwargs):
        """Maintain original function signature but use client credentials instead"""
        try:
            cols = ['Role', 'Email', 'PWD', 'Report', 'msgType']
            tgt = cols
            recipients = myDF(recipientsFile, self.pathType, C.FILE.value, C.EXCEL.value, cols, tgt, 1)
            recipientsDF = recipients.getDF()
            senderList = recipientsDF.loc[recipientsDF['Role'] == self.user].values.tolist()
            self.user = senderList[0][1]  # Keep for compatibility
            self.pwd = senderList[0][2]   # Keep for compatibility
        except Exception as e:
            print(f"Note: Using client credentials instead of user credentials. Error reading recipients: {str(e)}")
            self.user = "client_credentials"
            self.pwd = "client_credentials"

    # def readFromSP(self, SP, spSite, spFolder, spFile, localFolder, **kwargs):
    #     """Read files from SharePoint folder - exact original signature and logic"""
    #     prefix = kwargs.get('prefix', None)
    #     self.setCookies(SP, spSite)
    #     self.spFolder = self.site.Folder(spFolder)
    #     print('Sharepoint Folder for read = ', self.spFolder)
    #     self.readUrl = spSite + spFolder
        
    #     for fileName in self.spFolder.files:
    #         print(fileName)
    #         f = fileName['Name']
    #         if self.readFile(spFile, f) == 'X':
    #             print('Reading file ', f)
    #             self.url = self.readUrl + fileName['Name'] + '?cid=' + fileName['UniqueId']
    #             self.authenticate()
    #             self.response = File.open_binary(self.ctx, self.url)
    #             if prefix != None:
    #                 localFile = localFolder + prefix + f
    #                 print(localFile)
    #             else:
    #                 print(localFolder + f)
    #                 localFile = localFolder + f
    #             self.moveToLocalFolder(localFile)
    def readFromSP(self, SP, spSite, spFolder, spFile, localFolder, **kwargs):
        """Read files from SharePoint folder - with automatic path conversion"""
        delete_files_after_read = kwargs.get('delete_files_after_read', 'N')
        prefix = kwargs.get('prefix', None) #Optional Input
        # AUTO-CONVERT PATH FORMATS - UNIVERSAL FIX
        if spFolder.startswith('http'):
            # Convert full URL to server-relative path
            spFolder = spFolder.replace(spSite, '').replace(SP, '')
            if not spFolder.startswith('/'):
                spFolder = '/' + spFolder
        elif not spFolder.startswith('/'):
            # Convert relative path to server-relative path
            site_path = spSite.replace(SP, '')
            spFolder = f"{site_path}/{spFolder}"
            if not spFolder.startswith('/'):
                spFolder = '/' + spFolder
        
        self.setCookies(SP, spSite)
        self.spFolder = self.site.Folder(spFolder)  # Now uses converted path

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

    def readAllFromSP(self, SP, spSite, spFolder, localFolder, **kwargs):
        """Read all files from SharePoint folder - exact original signature and logic"""
        prefix = kwargs.get('prefix', None)
        self.setCookies(SP, spSite)
        self.spFolder = self.site.Folder(spFolder)
        print('Sharepoint Folder for read = ', self.spFolder)
        self.readUrl = spSite + spFolder
        
        print("Full URL: " + str(self.readUrl))
        
        for fileName in self.spFolder.files:
            print(fileName)
            f = fileName['Name']
            print('Reading file ', f)
            self.url = self.readUrl + fileName['Name'] + '?cid=' + fileName['UniqueId']
            self.authenticate()
            self.response = File.open_binary(self.ctx, self.url)
            if prefix != None:
                localFile = localFolder + prefix + f
            else:
                localFile = localFolder + f
            self.moveToLocalFolder(localFile)

    def readFile(self, file, f, **kwargs):
        """Check if file should be read - exact original logic"""
        if (file == f) | (file == ''):
            return 'X'

    def authenticate(self, **kwargs):
        """Authenticate - exact original signature but using client credentials"""
        try:
            credentials = ClientCredential(self.client_id, self.client_secret)
            ctx_auth = credentials
            self.ctx = ClientContext(self.url.split('?')[0]).with_credentials(ctx_auth)
            web = self.ctx.web
            self.ctx.load(web)
            self.ctx.execute_query()
            print("Authentication successful")
        except Exception as e:
            print("Authentication failed")

    def moveToLocalFolder(self, localFile, **kwargs):
        """Move file to local folder - exact original signature and logic"""
        print('Moving File Name = ', localFile)
        with open(localFile, "wb") as local_file:
            local_file.write(self.response.content)
        # save data to BytesIO stream
        bytes_file_obj = io.BytesIO()
        bytes_file_obj.write(self.response.content)
        bytes_file_obj.seek(0)  # set file object to start
        print('Move Completed')

    def setCookies(self, SP, spSite, **kwargs):
        """Set authentication - exact original signature but using client credentials"""
        try:
            credentials = ClientCredential(self.client_id, self.client_secret)
            self.ctx = ClientContext(spSite).with_credentials(credentials)
            
            # Test the connection
            web = self.ctx.web
            self.ctx.load(web)
            self.ctx.execute_query()
            print(self.ctx)  # Similar to original print(self.authcookie)
            
            # Create compatibility wrapper
            self.site = SharePointSiteWrapper(self.ctx, spSite)
            
        except Exception as e:
            print(f"Authentication failed: {str(e)}")
            raise e

    def writeToSPFolder(self, SP, spSite, folder, file, localFolder, successFile, errorFile, **kwargs):
        """Write files to SharePoint folder - exact original signature and logic"""
        self.setCookies(SP, spSite)
        self.spFolder = self.site.Folder(folder)
        pathKind = kwargs.get('pathType', C.REL.value)
        fKind = kwargs.get('fType', C.CSV.value)
        print('Sharepoint Folder for write = ', str(self.spFolder))

        Err = 0
        if file == '':
            files = os.listdir(localFolder)
            for fileName in files:
                print('Uploading File', fileName)
                f = localFolder + fileName
                with open(f, mode='rb') as rowFile:
                    fileContent = rowFile.read()
                    try:
                        self.spFolder.upload_file(fileContent, os.path.basename(f))
                        print("Write to Sharepoint Completed")
                        
                        tempDF = pd.DataFrame(columns=['File'])
                        tempDF['File'] = [fileName]
                        self.successDF = self.successDF.append(tempDF)

                        fileName = successFile
                        self.successSharepointWrite.writeFile(self.successDF, fileName, pathKind, fKind)

                    except Exception as e:
                        Err = 1
                        tempDF = pd.DataFrame(columns=['File'])
                        tempDF['File'] = [fileName]
                        self.errorDF = self.errorDF.append(tempDF)
                        fileName = errorFile
                        self.errorSharepointWrite.writeFile(self.errorDF, fileName, pathKind, fKind)
                        print(e)
        else:
            print('Uploading file ', file)
            with open(file, mode='rb') as rowFile:
                fileContent = rowFile.read()
                try:
                    self.spFolder.upload_file(fileContent, os.path.basename(file))
                    print("Write to Sharepoint Completed")
                    tempDF = pd.DataFrame(columns=['File'])
                    print('tempDF=' + str(tempDF))
                    tempDF['File'] = [file]
                    print('tempDF2=' + str(tempDF))
                    self.successDF = self.successDF.append(tempDF)
                    fileName = successFile
                    fileName = fileName.replace('.xlsx', '.csv')
                    print(fileName)
                    self.successSharepointWrite.writeFile(self.successDF, fileName, pathKind, fKind)

                except Exception as e:
                    Err = 1
                    tempDF = pd.DataFrame(columns=['File'])
                    tempDF['File'] = [file]
                    self.errorDF = self.errorDF.append(tempDF)
                    fileName = errorFile
                    fileName = fileName.replace('.xlsx', '.csv')
                    self.errorSharepointWrite.writeFile(self.errorDF, fileName, pathKind, fKind)
                    print(e)
        return Err

    def getFolderFilesList(self, SP, spSite, spFolder, columns):
        """Get folder files list - exact original signature"""
        self.setCookies(SP, spSite)
        self.spFolder = self.site.Folder(spFolder)
        print('Sharepoint Folder for read = ', self.spFolder)
        self.readUrl = spSite + spFolder

        try:
            allfiles = self.spFolder.files
            df_SPfiles = pd.DataFrame(allfiles, columns=columns)
            return df_SPfiles
        except Exception as e:
            print("ERROR in retrieving folder files list. Error Message: " + str(e))
            exit()

    def readDataFromExcelInSharePoint(self, SP, spSite, spLibrary, file):
        """Read Excel data from SharePoint - exact original signature"""
        self.setCookies(SP, spSite)
        
        try:
            self.spFolder = self.site.Folder(spLibrary)

            print("Getting " + file + " from SharePoint folder...")
            SPFile = self.spFolder.get_file(file)

            print("Reading Excel file...")
            data = io.BytesIO(SPFile)
            data.seek(0)

            return data

        except Exception as e:
            print("ERROR reading Excel file from SharePoint folder. Error Message: " + str(e))
            exit()

    def downloadFileFromSharepoint(self, SP, spSite, spLibrary, file):
        """Download file from SharePoint - exact original signature"""
        self.setCookies(SP, spSite)

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
        """Read Excel file from folder - exact original signature"""
        self.setCookies(SP, spSite)

        vSheet = kwargs.get('sheet', None)
        vSkiprows = kwargs.get('skiprows', None)
        vUseCols = kwargs.get('usecols', None)

        try:
            folder = self.site.Folder(spLibrary)

            print("Getting " + file + " from SharePoint folder...")
            SPFile = folder.get_file(file)

            print("Reading Excel file...")
            data = io.BytesIO(SPFile)

            if vSheet != None:
                df_SPFile = pd.read_excel(data, sheet_name=vSheet, skiprows=vSkiprows, usecols=vUseCols)
            else:
                df_SPFile = pd.read_excel(data, skiprows=vSkiprows, usecols=vUseCols)
            df_SPFile = df_SPFile.fillna('')

            return df_SPFile

        except Exception as e:
            print("ERROR reading Excel file from SharePoint folder. Error Message: " + str(e))
            exit()


class SharePointSiteWrapper:
    """Wrapper to maintain compatibility with original site object interface"""
    
    def __init__(self, ctx, site_url):
        self.ctx = ctx
        self.site_url = site_url
    
    def Folder(self, folder_path):
        """Return folder wrapper - matches original interface"""
        return SharePointFolderWrapper(self.ctx, self.site_url, folder_path)


class SharePointFolderWrapper:
    """Wrapper to maintain compatibility with original folder object interface"""
    
    def __init__(self, ctx, site_url, folder_path):
        self.ctx = ctx
        self.site_url = site_url
        self.folder_path = folder_path
        self.folder = None
        self._load_folder()
    
    def _load_folder(self):
        """Load the SharePoint folder"""
        try:
            self.folder = self.ctx.web.get_folder_by_server_relative_url(self.folder_path)
            self.ctx.load(self.folder)
            self.ctx.execute_query()
        except Exception as e:
            print(f"Error loading folder {self.folder_path}: {str(e)}")
    
    @property
    def files(self):
        """Return files list in original format"""
        try:
            files = self.folder.files
            self.ctx.load(files)
            self.ctx.execute_query()
            
            files_list = []
            for file in files:
                files_list.append({
                    'Name': file.properties['Name'],
                    'UniqueId': file.properties.get('UniqueId', ''),
                })
            return files_list
        except Exception as e:
            print(f"Error getting files list: {str(e)}")
            return []
    
    def upload_file(self, file_content, file_name):
        """Upload file to folder"""
        try:
            self.folder.upload_file(file_name, file_content)
            self.ctx.execute_query()
        except Exception as e:
            print(f"Error uploading file {file_name}: {str(e)}")
            raise e
    
    # def get_file(self, file_name):
    #     """Get file content as bytes - matches original interface"""
    #     try:
    #         file_url = f"{self.folder_path.rstrip('/')}/{file_name}"
    #         file_obj = self.ctx.web.get_file_by_server_relative_url(file_url)
            
    #         file_content = file_obj.read()
    #         self.ctx.execute_query()
            
    #         return file_content.value
    #     except Exception as e:
    #         print(f"Error reading file {file_name}: {str(e)}")
    #         return b''
    # def get_file(self, file_name):
    #     try:
    #         file_url = f"{self.folder_path.rstrip('/')}/{file_name}"
    #         file_obj = self.ctx.web.get_file_by_server_relative_url(file_url)
        
    #         file_content = file_obj.read()
    #         self.ctx.execute_query()
        
    #         return file_content  # ✅ Return bytes directly
    #     except Exception as e:
    #         print(f"Error reading file {file_name}: {str(e)}")
    #         return b''
    def get_file(self, file_name):
        try:
            file_url = f"{self.folder_path.rstrip('/')}/{file_name}"
            print(f"DEBUG: Trying to read file_url: {file_url}")
            
            # Use the same approach as the working debug script
            file_obj = self.ctx.web.get_file_by_server_relative_url(file_url)
            self.ctx.load(file_obj)  # Load the file object first
            self.ctx.execute_query()
            
            # Get file content using the working method
            file_content = file_obj.read()
            self.ctx.execute_query()
            
            print(f"DEBUG: Successfully read {len(file_content)} bytes")
            return file_content
            
        except Exception as e:
            print(f"DEBUG: Full error details: {str(e)}")
            print(f"DEBUG: Error type: {type(e).__name__}")
            import traceback
            traceback.print_exc()
            return b''
    
    def __str__(self):
        """String representation for compatibility with original print statements"""
        return f"SharePoint Folder: {self.folder_path}"