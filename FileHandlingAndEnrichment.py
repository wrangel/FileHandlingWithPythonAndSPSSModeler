## load libraries
import os
import sys
import datetime
import time
import modeler.api
import shutil
import platform
from subprocess import Popen, PIPE
from email.mime.text import MIMEText
import smtplib
from csv import reader, writer

## set working environment
OPS_MODE = "normal" ## full or normal
SCHEDULEDEXECUTIONHOURS = [<int>, <int>]
WORK_DIR = os.path.normpath("<path>")
IN_PUTTY_COM = r'<path>\PuTTY\pscp.exe -sftp -r -l 1658 -pw <pw>  -P 22 <origin_dir>/*.* <target_dir>'
## do not use this command directly in shell, it contains placeholders
OUT_WINSCP_COM = r'<path>\WinSCP\WinSCP.exe /log=<path>\winscp.log /command "option batch abort" "option confirm off" "option include *.csv" "open sftp://<host>" "synchronize remote <origin_path>\[YYYYMM]\[DD] /<target_path>" "exit"'
CUSTOM_CSV_HEADERS = [<list of headers>]

print("\n # start job\n")

print("\n # script runs on Jython local version "+str(platform.python_version())+"\n")


### preparations

## get db credentials
db_dir = os.path.join("<path>")
db_filepath = os.path.join(db_dir, "db.csv")
db_file_read = open(db_filepath)
for line in db_file_read: 
    pw = line
db_file_read.close()
DWH_CREDENTIALS = ["<server>", "<user>", "<pw>"]

## instantiate process log
process_log = []

## set date and time
timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") #time.strftime("%Y-%m-%d %H:%M:%S")
today = datetime.date.today()
fourdaysbefore = today - datetime.timedelta(4)
executionHour = datetime.datetime.now().hour
currYearMonth = str(int(today.year)*100+int(today.month))
lastYearMonth = str((datetime.date(day=1, month=today.month, year=today.year) - datetime.timedelta(days=1)).strftime("%Y%m"))
checkedYearMonths = [lastYearMonth, currYearMonth]

## set paths
repo_dir = os.path.join(WORK_DIR, os.path.normpath("<subdirectory>"))
dest_dir = os.path.join(WORK_DIR, os.path.normpath("<subdirectory>"))
root_dir = os.path.join(WORK_DIR, os.path.normpath("<subdirectory>"))
cym_path = os.path.join(dest_dir, os.path.normpath(str(currYearMonth)))

server_supp_dir = os.path.normpath("<path>")
server_temp_dir = os.path.normpath("<path>")

mailTextName = os.path.join(server_supp_dir, "mailFailureText.txt")
recipientsListName = os.path.join(server_supp_dir,"eMailRecipientList.txt") 
tempXLDesc = "Check.xlsx"
tempXLName = os.path.join(server_temp_dir, tempXLDesc) 

## exit for unexpected execution hour 
if executionHour not in SCHEDULEDEXECUTIONHOURS:
    print("\n EXIT PROCESS: No execution schedule forseen at this hour of the day")


### file management, morning task
if executionHour == SCHEDULEDEXECUTIONHOURS[0]:
    ## pull data from remote server
    print("\n # pull data from remote server\n")
    ## execute pull command
    try:
        p = Popen(IN_PUTTY_COM, stdout=PIPE)
        for line in p.stdout:
            print(line)
        p.wait()
    except Exception:
        process_log.append("ERROR: PuTTY was not able to pull data from remote")
        raise # reraises the exception
        sys.exit(1)
       
    ## distribute the files from the intermediary container root_dir to the appropriate dest_dir
    print("\n # distribute files")
    ## create a current year month folder, if not yet existing
    #os.makedirs(dest_dir, exist_ok=True) ## python 3.4 upwards
    try: 
        os.mkdir(cym_path)
    except (OSError, IOError):
        if not os.path.isdir(cym_path):
            process_log.append("ERROR: Problems when creating the month directory")
            raise
            sys.exit(1)
    
    ## walk through root_dir, create day folder in dest_dir if not yet existing, copy paste (overwrite) files to appropriate day folder
    for root, dirs, files in os.walk(root_dir):
        for filename in files:
            if len(filename) <= 25:
                fileDay = str(filename[9:11])
                fileYearMonth = "20"+str(filename[5:9])
            else:
                fileDay = str(filename[37:39])
                fileYearMonth = str(filename[31:37])
            # archive only the last four calendar days, including
            if fourdaysbefore <= datetime.datetime.strptime(fileYearMonth+fileDay, '%Y%m%d').date(): 
                pathNow = os.path.normpath(os.path.join(dest_dir, os.path.normpath(fileYearMonth), os.path.normpath(fileDay)))
                try: 
                    os.mkdir(pathNow)
                except (OSError, IOError):
                    if not os.path.isdir(pathNow):
                        process_log.append("ERROR: Problems when creating the day directory")
                        raise
                        sys.exit(1)
                try:
                    shutil.copyfile(os.path.join(root_dir, filename), os.path.join(pathNow, filename))
                except (OSError, IOError):
                    process_log.append("WARNING: Could not archive "+filename)
                    print("\n --> NOT archived: "+filename)
                    sys.exit(1)
                print("\n --> archived: "+filename)
    
    ## push data to remote server
    print("\n # push data to remote server")
    ## get only the latest YMD folder to push to remote
    latestYMfolder = 0
    for folderYM in os.listdir(dest_dir):
        if os.listdir(os.path.join(dest_dir, folderYM)):
            intfolderYM = int(folderYM)
            if intfolderYM > latestYMfolder:
                latestYMfolder = intfolderYM
    latestYMfolderPath = os.path.normpath(os.path.join(dest_dir, str(latestYMfolder)))
    latestDfolder = 0
    for folderD in os.listdir(latestYMfolderPath):
        intfolderD = int(folderD)
        if intfolderD > latestDfolder:
            latestDfolder = intfolderD
    
    if latestDfolder < 10:
        latestDDfolder = '0'+str(latestDfolder)
    else:
        latestDDfolder = str(latestDfolder)
    
    ## assemble the push command 
    OUT_WINSCP_COM = OUT_WINSCP_COM.replace("[YYYYMM]", str(latestYMfolder)).replace("[DD]", latestDDfolder)
    ## execute push command
    try:
        p = Popen(OUT_WINSCP_COM, stdout=PIPE)
        p.wait()
        print("\n --> pushed latest folder YM "+str(latestYMfolder)+" D "+latestDDfolder+" to remote\n")
    except Exception:
        process_log.append("ERROR: WinSCP was not able to push data to remote")
        raise
        sys.exit(1)

    print("\n # job is done\n")


### enrichment task, afternoon task
if executionHour == SCHEDULEDEXECUTIONHOURS[1]:
    ## extract the reference list of files already loaded in PR_MASTER
    print("\n # get list of already imported files")
    referenceListFileName = os.path.join(server_supp_dir, "referenceList.txt")
    referenceListFile = open(referenceListFileName)
    data = referenceListFile.readlines()
    referenceList = []
    for d in data:
        referenceList.append(d.rstrip())
    referenceListFile.close()
    print("\n --> "+str(len(referenceList))+" files screened so far")
    
    ## screen though all files in specified folders and determine all files not yet loaded in PR_MASTER. 2 months, rolling
    print("\n # screen through directories\n")
    
    ## get stream object
    stream = modeler.script.stream()
    
    ## set grid
    hpos = 200
    vpos = 200
    
    ## get 201
    outputDbNode201 = stream.findByID("idC8RCXI27KI")
    outputDbNode201.setPropertyValue("datasource", DWH_CREDENTIALS)
    
    # get 001
    inputNode001 = stream.findByID("id6F2SI2RZFPE")
    inputNode001.setPropertyValue("datasource", DWH_CREDENTIALS)
    
    ## get 102
    intermediateNode102 = stream.findByID("id6L5S1FR8F35")
    
    # get 002
    inputNode002 = stream.findByID("id6P6SU1RWFYD")
    inputNode002.setPropertyValue("datasource", DWH_CREDENTIALS)
    
    ## get 203, 204, 205
    outputNode203 = stream.findByID("id4WIRQXIDSDS")
    outputNode203.setPropertyValue("full_filename", tempXLName)
    
    warnings = []
    processedFileCount = 0
    insertedFileCount = 0
    
    if OPS_MODE == "full":
        checkedYearMonths = [1]
    
    for cM in checkedYearMonths:
        if OPS_MODE == "full":
            checked_dd = dest_dir
        else:
            checked_dd = os.path.normpath(os.path.join(dest_dir, os.path.normpath(cM)))
        for root, dirs, files in os.walk(checked_dd):
            for filename in files:
                if os.path.splitext(filename)[-1].lower() == ".csv":
                    if filename not in referenceList:
                        processedFileCount += 1
                        # get the number of rows in the file
                        filepath = os.path.join(root, filename)
                        file_read = open(filepath)
                        row_count = sum(1 for row in file_read)
                        file_read.close()
                        if row_count > 2:
                            insertedFileCount += 1
                            print(filepath+"\n")
                            ## copy files temporarily to server local path
                            server_temp_dirpath = os.path.join(server_temp_dir, filename)
                            modifiedserver_temp_dirpath = os.path.join(server_temp_dir, os.path.splitext(filename)[0]+"modified.txt")
                            shutil.copy(filepath, server_temp_dirpath)
                                
                            ## get rid of NULL byte rows 
                            ## http://taombo.com/taombo-blog/reading-null-values-with-csv.reader-giving-line-contains-null-byte
                            inputFile = open(filepath, "rb") 
                            r = reader((line.replace('\0', '') for line in inputFile), delimiter='\t')
                            r.next() # skip the headers
                            #regular_rows = [['SP']+[filename]+[timestamp]+row[:14] for row in r if row and len(row[8]) == 54]      
                            regular_rows = [['SP']+[filename]+[timestamp]+row[:14] for row in r if row and len(''.join(row[8:9]).strip("'")) == 52]  
                
                            outputFile = open(modifiedserver_temp_dirpath, "wb") 
                            w = writer(outputFile, delimiter=';')
                            w.writerow(CUSTOM_CSV_HEADERS) # replace standard headers hy custom headers
                            w.writerows(regular_rows)
                          
                            outputFile.close()
                            inputFile.close()
                            
                			## create import node for newest available file in current month folder
                            inputNode = stream.createAt("variablefile", "CURRENT_PR_IMPORT", hpos, vpos)
                            inputNode.setPropertyValue("full_filename", modifiedserver_temp_dirpath)
                            inputNode.setPropertyValue("read_field_names", True)
                            inputNode.setPropertyValue("skip_header", 0)
                            inputNode.setPropertyValue("delimit_space", False)
                            inputNode.setPropertyValue("delimit_comma", False)
                            inputNode.setPropertyValue("delimit_tab", False)
                            inputNode.setPropertyValue("delimit_other", True)
                            inputNode.setPropertyValue("other", ";")
                            inputNode.setPropertyValue("strip_spaces", "Both")
                            stream.link(inputNode, outputDbNode201) 
                			
                            try:
                                nodes = [ outputDbNode201 ]
                                results = []   
                                stream.runSelected(nodes, results)
                            except Exception:
                                print(traceback.format_exception(*sys.exc_info()))
                                process_log.append("ERROR: Problems when inserting into PR_LOAD")
                                raise
                                sys.exit(1)
    
                            ## clean up
                            os.remove(server_temp_dirpath)
                            os.remove(modifiedserver_temp_dirpath)
                            stream.delete(inputNode)
                        
                        ## append filename to referenceList.txt, in any case
                        referenceListFile = open(referenceListFileName, 'a')
                        print>>referenceListFile, filename
                        referenceListFile.close()
    
                        ## csv file counter and warnings
                        numberOfFiles = len([filename for filename in os.listdir(root) if os.path.splitext(filename)[-1].lower() == ".csv"])
                        if numberOfFiles != 10:
                            if numberOfFiles > 1:
                                warningString = root+" contains "+ str(numberOfFiles)+" csv files"
                            else:
                                warningString = root+" contains "+ str(numberOfFiles)+" csv file"
                            warnings.append(warningString) 
        		   
    print(" --> "+str(processedFileCount) +" file(s) processed")
    print(" --> Insert "+str(insertedFileCount) +" file(s) into PRD_FIN.PR_MASTER")
    
    ## do insert and statistics only in case of need
    if insertedFileCount > 0:
        ## write out daily warning statistics
        ## deduplicate warnings list
        warnings_unique = set(warnings)
        ## create warnings ouput
        curr_date = str(datetime.datetime.now().date())
        warningsListFileName = os.path.join(server_supp_dir, "warningsList.txt")
        ## "w" creates the file if the file does not exist, but it will truncate the existing file. "a" appends to existing file
        try:
            warningsListFile = open(warningsListFileName, 'a')
        except Exception:
            process_log.append("ERROR: Problems when handling warnings.txt")
            raise
            sys.exit(1)
        for w_u in warnings_unique:
            print>>warningsListFile, curr_date+" -- "+w_u
        warningsListFile.close()    
    
        ## run insert / table node 2    	
        nodes = [ intermediateNode102 ]
        results = []   
        try:
            stream.runSelected(nodes, results)  
        except Exception:
            process_log.append("ERROR: Problems when inserting into PR_MASTER")
            raise
            sys.exit(1)
      
        print("\n # produce statistics\n")
        ## run daily statistics 1
        nodes = [ outputNode203 ]
        results = []   
        try:
            stream.runSelected(nodes, results) 
        except Exception:
            process_log.append("ERROR: Problems when handling dailyProcedureCheck.xlsx")
            raise
            sys.exit(1)
    
        shutil.copy(referenceListFileName, os.path.join(repo_dir, "referenceList.txt"))  
        shutil.copy(warningsListFileName, os.path.join(repo_dir, "warningsList.txt"))
        shutil.copy(tempXLName, os.path.join(repo_dir, tempXLDesc))
    
    ## notify
    print("\n # send notification (if failed)\n")
    if(len(process_log)) > 0:
        print(", ".join(process_log))
        mailText = open(mailTextName, "r")
        msg = MIMEText(mailText.read())
        recipientslist = []
        recipientsList = open(recipientsListName, "r")
        for recipient in recipientsList:
            recipientslist.append(recipient.rstrip())
        recipientsList.close()
        sender = "<e-mail address>"
        recipients = recipientslist
        msg['Subject'] = "<Subject> - Error or Warning Notification"
        msg['From'] = sender
        msg['To'] = ", ".join(recipients)
        s = smtplib.SMTP('<url>') #send_message for python3
        full_msg = msg.as_string()+"\n "+", ".join(process_log)
        print(full_msg)
        s.sendmail(sender, recipients, full_msg)
        s.quit()
    
    print("\n # job is done\n")
