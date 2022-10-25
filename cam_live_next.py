import datetime
import os
import numpy as np
import time
import pickle
import re
import glob
from sqlalchemy import create_engine

vision_path = "/home/pi/sfn/"
machine_name = "MC42"
server_name ='10.221.151.14:1433'
database = 'SFN'
username = 'sfn'
password = 'Molex12#'
pickle_file = 'store_stamping.pckl'

engine = create_engine("mssql+pyodbc://"+username+":"+password+"@"+server_name+"/"+database+"?driver=FreeTDS")
print(engine.connect())

def SaveLineCount(filename, count):
    var = open(pickle_file, 'wb')
    n = [filename,count]
    pickle.dump(n, var)
    var.close()

def LoadLineCount():
    var = open(pickle_file, 'rb')
    n = pickle.load(var)
    var.close()
    return(n)

def GetNextFile(vision_path, old_file):
    old_modified_time = datetime.datetime.fromtimestamp(os.path.getmtime(vision_path+old_file))
    modified_time_list = []
    for file in os.listdir(vision_path):
        if "cam1" in file:
            file_modified_time = datetime.datetime.fromtimestamp(os.path.getmtime(vision_path+file))
            if file_modified_time>old_modified_time:
                modified_time_list.append(file_modified_time)
                if file_modified_time == np.min(modified_time_list):
                    next = file
    if len(modified_time_list)<1:
        return(0)
    return(next)

def WriteDB(file_to_read,line,engine,count):
   line_list = line.split(",")
   day = datetime.datetime.strptime(line_list[0].strip(), '%d/%b/%y')
   time = datetime.datetime.strptime(line_list[1].strip(), '%H:%M:%S').time()
   dt_str = (datetime.datetime.combine(day, time)).strftime('%Y-%m-%d %H:%M:%S')
   LG1 = line_list[2].split(":")[1].strip()
   LG2 = line_list[3].split(":")[1].strip()

   strSqlTemplate ='INSERT INTO [dbo].[stamping_vision]([Machine],[Count],[RecordDate],[LG1],[LG2],[CreateDate],[Source]) VALUES (\'<Machine>\',<Count>,\'<RecordDate>\',<LG1>,<LG2>,getdate(),\'<Source>\');'
   strSqlTemplate = strSqlTemplate.replace('<Machine>',machine_name)
   strSqlTemplate = strSqlTemplate.replace('<Source>',file_to_read)
   strSqlTemplate = strSqlTemplate.replace('<Count>',str(count))
   strSqlTemplate = strSqlTemplate.replace('<RecordDate>',str(dt_str))
   strSqlTemplate = strSqlTemplate.replace('<LG1>',LG1)
   strSqlTemplate = strSqlTemplate.replace('<LG2>',LG2)
   print(strSqlTemplate)
   engine.execute(strSqlTemplate)

while(True):
    try:
        day = datetime.datetime.now() - datetime.timedelta(days=0)
        date_formatted = day.strftime("%Y%m%d")
        print(date_formatted)
        #SaveLineCount(latest,str(0))
        old = LoadLineCount()
        old_file = old[0]
        old_count = int(old[1])
        new_file = GetNextFile(vision_path, old_file)
        print(new_file)
    except:
        continue

    #keep reading old file if latest line count > previous line count
    try:
        with open(vision_path+old_file, 'r') as f:
            lines = f.read().splitlines()
            #print(len(lines))
            if len(lines)>old_count:
                file_to_read = old_file
            else:
                file_to_read = new_file
        
        with open(vision_path+file_to_read, 'r') as g:
            lines = g.read().splitlines()
            new_count = len(lines)
            print(old_file+":"+str(old_count))
            print(file_to_read+":"+str(new_count))
        
            if file_to_read == old_file:
                count = new_count
                lines_to_read = lines[count:]
            else:
                count = 1
                lines_to_read = lines

            if(file_to_read!=old_file or new_count>old_count):
                print("***new data***")
                for line in lines_to_read:
                    WriteDB(file_to_read,line,engine,count)
                    count += 1
                print("done")
                SaveLineCount(file_to_read, new_count)
                time.sleep(5)
                continue
            else:
                print("***old data***")
                time.sleep(2)
                continue
    except:
        time.sleep(2)
        continue