""" ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
Description: This script will perform an audit of Replicated Feature Datasets
 
Beginning with: ReplicaAudit.py
 
Created on: 8/25/2017
 
Purpose: This Script will perform an audit of Replicated Feature 
    Datasets by Querying the Replica Dataset ID in the GDB Items
    Table. Then we loop through the returned records and capture
    the Physical Name and query the GDB Items table for the Physical
    Name and the data type not equal to the Replica Dataset ID.
    Then we capture the Item Type, and Item type name. Then we
    write the results to the excel. If a GDB has no replicated
    datasets nothing is written.

    query the GDB Items table for the Physical Name and the data type not equal to the Replica Dataset ID.
 
Authored by: Joe Guzi
 
Previous Production Date:      Production Date: 8/29/2017
 
### ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------"""
 
# Import modules
import arcpy, time, sys, string, os, traceback, datetime, shutil, httplib, urllib, json, getpass, arcserver, subprocess
import xml.dom.minidom as DOM
import xml.etree.ElementTree as ET
from subprocess import Popen
import smtplib
from email.MIMEText import MIMEText
# End Import
 
# Setting the arc py environment
ENV= arcpy.env
# End Arcpy Environment
 
# Setting the overwrite of existing features
ENV.overwriteOutput = True
# End Overwrite Setting
 
# Set Email List
SetEmail([])
# End Set Email List
  
# Write Log code
logFile, root = setLog(sys.argv[0], True) #True=Time stamp in log file name; False=No time stamp in log file name
 
'''
---  These are log examples  ---
message += "Write log message here" + "\n"
exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
formatted_lines = traceback.format_exc().splitlines()
writelog(logFile,message + "\n" + formatted_lines[-1])
writelog(logFile, "Write log message here" + "\n")
---  End log examples  ---
'''
# End Write Log code
 
 
# Functions
message = ""
scriptName = ""
logFile = ""
EmailList = ['jsguzi@starkcountyohio.gov']

def setLog(SysArgv, Timestamp):
    global scriptName, logFile
    dateTimeStamp = time.strftime('%Y%m%d%H%M%S')
    root = os.path.dirname(SysArgv) #"C:\\Users\\jsguzi\\Desktop"
    if not os.path.exists(root + "\\log"): # Check existence of a log folder within the root, if it does not exist it creates one.
        os.mkdir(root + "\\log")
    scriptName = SysArgv.split("\\")[len(SysArgv.split("\\")) - 1][0:-3] #Gets the name of the script without the .py extension

    if Timestamp == True:
        logFile = root + "\\log\\" + scriptName + "_" + dateTimeStamp[:14] + ".log" #Creates the logFile variable
    elif Timestamp == False:
        logFile = root + "\\log\\" + scriptName + ".log" #Creates the logFile variable
        
    if os.path.exists(logFile):
        os.remove(logFile)
    return logFile, root

def SetEmail(AdditionalEmailList):
    global EmailList
    EmailList = EmailList + AdditionalEmailList

def writelog(logfile,msg):
    global message
    message += msg
    print msg
    f = open(logfile,'a')
    f.write(msg)
    f.close()

def sendEmail(subject, emailMessage):
    #This function is for general success or error emails, sent to SCGIS
    global message, scriptName, EmailList, logFile
    message += emailMessage
    messages = arcpy.GetMessages()
    message += messages
    exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
    formatted_lines = traceback.format_exc().splitlines()
    writelog(logFile, formatted_lines[-1] + "\n")
    # Send Email
    # This is the email notification piece [%]
    #email error notification
    smtpserver = 'mailrelay.co.stark.oh.us'
    AUTHREQUIRED = 0 # if you need to use SMTP AUTH set to 1
    smtpuser = ''  # for SMTP AUTH, set SMTP username here
    smtppass = ''  # for SMTP AUTH, set SMTP password here
 
    RECIPIENTS = EmailList
    SENDER = 'gissas@starkcountyohio.gov'
    msg = MIMEText(message) #***i pointed this mime thing at the message
    msg['Subject'] = subject + ' with Script: ' + str(scriptName) ### this is the subject line of the email
    # Following headers are useful to show the email correctly
    # in your recipient's email box, and to avoid being marked
    # as spam. They are NOT essential to the sendmail call later
    msg['From'] = "ArcGIS on GISSAS "
    msg['Reply-to'] = "Joe Guzi "
    msg['To'] = "jsguzi@starkcountyohio.gov"
 
    session = smtplib.SMTP(smtpserver)
    if AUTHREQUIRED:
        session.login(smtpuser, smtppass)
    session.sendmail(SENDER, RECIPIENTS, msg.as_string())
    session.close()

def readTextFile(File, Time):
    DayAgo = datetime.timedelta(days = 1)
    DayAgoDateTime = Time - DayAgo
    PreviousDateTime = str(DayAgoDateTime)[0:19]
    if not os.path.exists(File):
        TextFile = open(File, "w")
        TextFile.write("")
        TextFile.close()
    else:
        TextFile = open(File, "r")
        Lines = TextFile.read()
        if Lines == "":
            pass
        else:
            PreviousDateTime = Lines
        TextFile.close()
    return PreviousDateTime

def writeTextFile(File, Time):
    ShortTime = str(Time)[0:19]
    TextFile = open(File, "w")
    TextFile.write(ShortTime)
    TextFile.close()

    # Replica Audit Function
def ReplicaAudit (GDB, GDBItems, GDBName):
    global GeodatabasefileWrite
    t = ""
    SQLReplicaDataset = "TYPE = '{D98421EB-D582-4713-9484-43304D0810F6}'"

    writelog(logFile, "Process: Table Search Cursor" + "\n")
    GDBReplicaDatasetCursor = arcpy.da.SearchCursor(GDBItems, "*", SQLReplicaDataset)
    writelog(logFile, "Process: Table Search Cursor Complete!" + "\n")
    
    for Feature in GDBReplicaDatasetCursor:
        PhysicalName = Feature[4]
        writelog(logFile, "Physical Name: " + str(PhysicalName) + "\n")
        SQLItemName = "PHYSICALNAME = '" + str(PhysicalName) + "'" + "AND TYPE <> '{D98421EB-D582-4713-9484-43304D0810F6}'"

        writelog(logFile, "Process: GDB Item Name Search Cursor" + "\n")
        GDBItemsCursor = arcpy.da.SearchCursor(GDBItems, "*", SQLItemName)
        writelog(logFile, "Process: GDB Item Name Search Cursor Complete!" + "\n")

        for Item in GDBItemsCursor:
            Path = str(Item[5])
            writelog(logFile, "-------: " + str(Path) + "\n")
            TypeID = str(Item[2])
            writelog(logFile, "-------: " + str(TypeID) + "\n")

            if TypeID == "{70737809-852C-4A03-9E22-2CECEA5B9BFA}":
                TypeName = "Feature Class"
            elif TypeID == "{74737149-DCB5-4257-8904-B9724E32A530}":
                TypeName = "Feature Dataset"
            elif TypeID == "{767152D3-ED66-4325-8774-420D46674E07}":
                TypeName = "Topology"
            elif TypeID == "{B606A7E1-FA5B-439C-849C-6E9C2481537B}":
                TypeName = "Relationship Class"
            elif TypeID == "{73718A66-AFB9-4B88-A551-CFFA0AE12620}":
                TypeName = "Geometric Network"
            elif TypeID == "{CD06BC3B-789D-4C51-AAFA-A467912B8965}":
                TypeName = "Table"
            else:
                TypeName = "Error"

            t += GDBName + ","
            t += Path + ","
            t += PhysicalName + ","
            t += TypeID + ","
            t += TypeName + ","
            t += "\n"

        writelog(logFile, "Process: Delete Cursors" + "\n")
        del GDBItemsCursor
        writelog(logFile, "Process: Delete Cursors Complete!" + "\n")
        
    writelog(logFile, "Write to CSV"+ "\n")
    ReplicaAuditfileWrite.writelines(t)
    writelog(logFile, "Write to CSV Complete"+ "\n")
     
    writelog(logFile, "Process: Delete Cursors" + "\n")
    del GDBReplicaDatasetCursor
    writelog(logFile, "Process: Delete Cursors Complete!" + "\n")
# End Function Section
 
# Variables
#Create Audit Folder
writelog(logFile, "Check Folder Existence" + "\n")

folder_Path = "G:\\SCGIS\\ArcGISServerAudit\\SCGIS\\" #Change path to the folder of your choice
date = datetime.date.today()
date =str(date)
newDate = date[5:7] + date[8:] + date[0:4]
folder = folder_Path + "Audit" + newDate
if not os.path.exists(folder):
    os.mkdir(folder)

writelog(logFile, "Check Folder Existence Complete!" + "\n")

dateTimeStamp = time.strftime('%Y%m%d%H%M%S')
ConnectionFile = root + "\\Connection Files\\"
GDB = ConnectionFile + "ConnectionFile.sde" # Create a connection file and change the variable
GDBItems = GDB + "\\***.dbo.GDB_ITEMS" #change the stars to the gdb name
t = ""
# End Variable Section 
 
try:
    # Process
    writelog(logFile, "Process:" + "\n")
    writelog(logFile, "STARTING TIME: " + str(datetime.datetime.now()) + "\n")   

    writelog(logFile, "Process: Create Replica Audit File" + "\n")
    ReplicaAuditfile = folder + "\\ReplicaAudit" + dateTimeStamp +".csv"
    ReplicaAuditfileWrite = open(ReplicaAuditfile, 'w')
    ReplicaAuditfileWrite.write("Database Name,Path,PhysicalName,Type,Typename\n")
    writelog(logFile, "Process: Create Replica Audit File Complete" + "\n")

    writelog(logFile, "Process: Run Replica Audit CCGDB" + "\n")
    ReplicaAudit (GDB, GDBItems, "***") #change the stars to the gdb name
    writelog(logFile, "Process: Run Replica Audit CCGDB Complete" + "\n")

    # Close CSV File
    writelog(logFile, "Process: Close CSV File" + "\n")
    ReplicaAuditfileWrite.close()
    writelog(logFile, "Process: Close CSV File Complete!" + "\n")
     
    writelog(logFile, "ENDING TIME: " + str(datetime.datetime.now()) + "\n")   
    writelog(logFile, "Success!" + "\n")
except:
    writelog(logFile, "Error:" + "\n")
    writelog(logFile, "ERROR TIME: " + str(datetime.datetime.now()) + "\n")
    sendEmail("Error", "Error" + "\n")
