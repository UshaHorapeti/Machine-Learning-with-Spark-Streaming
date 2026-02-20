from genericpath import isdir
from threading import local
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
import smtplib
import pandas as pd
import os


try:
    from myLibraryConstants import myConstants as C
    from myDFClass import myDF as myDF
except:
    from myLibraryConstants import myConstants as C
    from myDFClass import myDF as myDF

class myNotify:
    def __init__(self,recipientsFile,emailSettingsFile,pathType,**kwargs): 
       self.recipientsFile=recipientsFile
       self.emailSettingsFile=emailSettingsFile
       self.pathType=pathType
       print('Notify object set up')
    
    def getLogin(self, **kwargs):
        user = kwargs.get('user', C.FROM.value)
        cols=['Role','Email','PWD','Report','msgType']
        tgt=cols
        recipients=myDF(self.recipientsFile,self.pathType,C.FILE.value,C.EXCEL.value,cols,tgt,1)
        recipientsDF=recipients.getDF()
        #print(recipientsDF)
        senderList=recipientsDF.loc[recipientsDF['Role']==user].values.tolist()
        #print(f'Sender List: {senderList}')
        uid=senderList[0][1]
        pwd=senderList[0][2]
        return uid,pwd
  
    def notify(self,msgType,report,**kwargs):
        attachments = kwargs.get('attachments', None) # Optional input
        html = kwargs.get('html', None) # Optional input
        attachmentsDir = kwargs.get('attachmentsDir',os.getcwd()) #Optional input
        subject =kwargs.get('subject',None)

        cols=['Role','Email','PWD','Report','msgType']
        tgt=cols
        recipients=myDF(self.recipientsFile,self.pathType,C.FILE.value,C.EXCEL.value,cols,tgt,1)
        recipientsDF=recipients.getDF()
        recipientsDF=self.filterRecipients(recipientsDF,msgType,report)

        cols=['msgType','message','send']
        tgt=cols
        emailSettings=myDF(self.emailSettingsFile,self.pathType,C.FILE.value,C.EXCEL.value,cols,tgt,1)
        emailDF=emailSettings.getDF()

        settingsList=emailDF.loc[emailDF['msgType']==msgType].values.tolist()
        print(settingsList)

        print(str(settingsList))
        if len(settingsList)>0:
            message=settingsList[0][1]
            #print(f'Message:{message}')
            send=settingsList[0][2]
            #print(f'Send: {send}')
            print(f'recipientsDF rows: {recipientsDF.shape[0]}')
            
            if recipientsDF.shape[0]>1:
                if(send=='X'):
                    recList=recipientsDF.loc[recipientsDF['Role']==C.TO.value]['Email'].values.tolist()
                    senderList=recipientsDF.loc[recipientsDF['Role']==C.FROM.value].values.tolist()
                    sender=senderList[0][1]
                    pwd=senderList[0][2]
                    ccList=recipientsDF.loc[recipientsDF['Role']==C.CC.value]['Email'].values.tolist()

                    msg = MIMEMultipart()
                    dateEST=pd.to_datetime('today')-pd.Timedelta('6 hours')
                    if subject == None:
                        msg['Subject'] = report+message+dateEST.strftime("%m/%d/%Y")
                    else:
                        msg['Subject'] = subject
                    msg['From'] = sender
                    msg['To'] = ', '.join(recList)
                    msg['Cc'] = ', '.join(ccList)

                    #print(' Sender : ',msg['From'])
                    #print(' To : ',msg['To'])
                    #print(' Cc : ',msg['Cc'])

                    msg=self.buildAttachments(msg,attachments,folder=attachmentsDir)
                    msg=self.buildHtml(msg,html)

                    server = smtplib.SMTP(C.HOST.value,C.PORT.value)
                #    server.starttls()            05/02 VJ Commented the logic because administrator of the SMTP server has disabled smtp login feature
                #    server.login(sender, pwd)    05/02 VJ Commented the logic because administrator of the SMTP server has disabled smtp login feature
                    #print(f'Sender: {str(sender)}')
                    #print(f'Recipients: {str(recList)}')
                    server.sendmail(sender, recList, msg.as_string())
                    server.close()       

    def filterRecipients(self,df,msgType,report):
        resultDF=df.loc[df['Role']==C.FROM.value]
        searchDF=df.loc[df['Report']==report]

        for index,row in searchDF.iterrows():
            if row['msgType']=='':
                tempDF=pd.DataFrame([row])
                resultDF=pd.concat([resultDF,tempDF],ignore_index = True, axis = 0)
            else:
                msgTypeList=str(row['msgType']).split(',')
                for msg in msgTypeList:
                    if int(msg)==int(msgType):
                        tempDF=pd.DataFrame([row])
                        resultDF=pd.concat([resultDF,tempDF],ignore_index = True, axis = 0)
        return resultDF  

    def buildAttachments(self,msg,attachments,**kwargs):
        if attachments!=None:
            localFolder = kwargs.get('folder',os.getcwd())#Optional Input
            root=localFolder.split(os.sep)[:-1]
            print(f'RootFolder:{root}')
            inputFolder=os.sep.join(root)
            if os.path.isdir(os.path.join(inputFolder,attachments)):
                inputFolder = os.path.join(inputFolder,attachments)
                files=os.listdir(inputFolder)
            else:
                files= [attachments]
            #print(f'Input folder: {inputFolder}')
            #print(f'Files to send: {str(files)}')

            for file in files or []:
                if os.path.isfile(os.path.join(inputFolder, file)):
                    f= os.path.join(inputFolder,file)
                    #print(f'f: {f}')
                    with open(f, "rb") as fil:
                        part = MIMEApplication(fil.read(),Name=os.path.basename(f))
            # After the file is closed
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
                    msg.attach(part)
        return msg   

    def buildHtml(self,msg,html):
        if html!=None:
            partHTML = MIMEText(html, "html")
            msg.attach(partHTML)
        return msg

#print(f'Module name:{__name__}')