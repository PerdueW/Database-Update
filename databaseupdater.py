#!/usr/bin/python3

import mysql.connector
from mysql.connector import Error 
import time
from datetime import datetime, date
from tkinter import filedialog
from tkinter import *
import tkinter as tk
import os
import shutil
from pathlib import Path
import sys

host = ""
#host = ""
#host = ""
user = ""
password = ""
database = ""
cleaneddbColumns = []
cleantables = []
distinctFileColumns =  []
testcases = []
SpecCases = []

if not os.path.exists('Processed_Files'):
        os.makedirs('Processed_Files')
        
def exitProgram():
        root.destroy()

def insertData():
        status.config(text=" ")
        SpecCases = []
        root.filePath =  filedialog.askopenfilename(initialdir = "Desktop",title = "Select Saved Insert")
        if not root.filePath:
            root.destroy()
            exit()
        else:
            pass
        pathAct.config(text=root.filePath)
        insert = open(root.filePath, 'r')
        columnString = insert.read()
        # Creates the list of tests in the insert statement
        spectable = columnString[columnString.find('into ') + 5: columnString.find('(')] # gets and sets the model for insert statement
        table = columnString[columnString.find('into ') + 5: columnString.find('(')] # gets and sets the table for insert statement
        spectable = spectable.strip()
        table = table.strip()
        SpecCasesResult = ""
        with open(filepath, "r") as fp:
                for line in fp:
                        line = line.replace('-', '_')
                        speccasesTable = line.split('=')[0]
                        specCases = line.split('=')[1]
                        if(spectable == speccasesTable):
                                SpecCasesResult = specCases
                                break
                        elif(table == speccasesTable):
                                SpecCasesResult = specCases
                                break
                        else:
                                SpecCasesResult="" 
        status.config(text="Processing..") 
        SpecCases = SpecCasesResult.split(",")                     
        fileColumns = columnString[columnString.find(' (') + 2: columnString.find(') values')]
        fileColumns = fileColumns.split(",")
        filevalues = columnString[columnString.find('values(') + 7: columnString.rfind(')')]
        filevalues = filevalues.split("',")
        # Connects to database and retrieves the columns in
        # the table and then converts it in to a list
        conn = mysql.connector.connect(host=host,user=user,password=password,database=database) 
        cursor = conn.cursor()
        cursor.execute("SELECT `COLUMN_NAME`FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_NAME`='" + table + "'")
        dbColumns = cursor.fetchall ()
        for decolumn in dbColumns:
            cleaneddbColumns.append(decolumn[0])

        # Querys the database and gets the list of current tables
        # in the database.
        savedTable = columnString[columnString.find('into ') + 5: columnString.find(' (')]
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall ()
        for table in tables:
            cleantables.append(table[0])
        if savedTable not in cleantables:
            cursor.execute("create table " + savedTable + " (mac  varchar(20), testdatetime datetime) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8")
            cursor.execute("set global innodb_file_format = barracuda")
            cursor.execute("set global innodb_file_per_table = 1")
            modelssavedTable = ''.join(savedTable.split('m', 1))
            cursor.execute("select model from models")
            models = cursor.fetchall ()
            if (modelssavedTable,) not in models:
                cursor.execute("insert into models (model) values ('" + modelssavedTable +"')")
            else:
                pass
        else:
            pass

        # Isolates the testsuite and builds a list of specific testsuites
        for column in fileColumns:
            testcases.append(column.split('__')[0])
        for test in testcases: 
            if test not in distinctFileColumns: 
                distinctFileColumns.append(test) 

        for testsuite in distinctFileColumns:
            if testsuite in SpecCases:
                specTable = spectable + "__" + str(testsuite)
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall ()
                for table in tables:
                    cleantables.append(table[0])
                if specTable not in cleantables:
                    cursor.execute("create table " + specTable + " (mac  varchar(20), testdatetime datetime) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8")
                    cursor.execute("set global innodb_file_format = barracuda")
                    cursor.execute("set global innodb_file_per_table = 1")
                    cursor.execute("SHOW TABLES")
                    tables = cursor.fetchall ()
                    for table in tables:
                        cleantables.append(table[0])
                else:
                    pass
            else:
                pass
        for column in fileColumns:
            testsuiteTest = column.split('__')[0]
            if testsuiteTest in SpecCases:
                queryString = ("SELECT column_name FROM information_schema.columns WHERE table_schema = 'TestStandDB' AND table_name = '" + spectable + "__" + testsuiteTest + "'")
                cursor.execute(queryString)
                tablecolumns = cursor.fetchall ()
                tablecolumns = list(sum(tablecolumns, ())) 
                count = len(re.findall("__", column))
                if (count <= 1):
                        name = column
                else:
                        name="__".join(column.split("__")[1:])
                if column not in tablecolumns:
                    queryString = ("ALTER TABLE " + spectable + "__" + testsuiteTest + " ADD " + column + " text NULL")
                    try:
                            #print(queryString)
                            cursor.execute(queryString)
                    except mysql.connector.Error as e:
                            print(e)
                            print (testsuiteTest + " Please check the insert statement for any special characgters")
                            status.config(text="Error in insert statment")
                            sys.exit(1)
                else:
                    pass
            else:
                queryString = ("SELECT column_name FROM information_schema.columns WHERE table_schema = 'TestStandDB' AND table_name = '" + savedTable + "'")
                cursor.execute(queryString)
                tablecolumns = cursor.fetchall ()
                tablecolumns = list(sum(tablecolumns, ()))
                if column.upper() not in (name.upper() for name in tablecolumns):
                    queryString = ("ALTER TABLE " + savedTable + " ADD " + column + " text NULL")
                    try:
                            #print(queryString)
                            cursor.execute(queryString)
                    except mysql.connector.Error as e:
                            print(e)
                            print (testsuiteTest + " Please check the insert statement for any special characgters")
                            status.config(text="Error in insert statment")
                            sys.exit(1)
                else:
                    pass
        status.config(text="Processing....") 
        dbinfo = {}
        def merge(list1, list2):       
            merged_list = [(list1[i], list2[i]) for i in range(0, len(list1))] 
            return merged_list
        columnvaluepair = merge(fileColumns, filevalues)
        for pair in columnvaluepair:
            print(pair)
            testsuite = pair[0].split('__')[0]
            if testsuite in SpecCases:
                if testsuite not in dbinfo:
                    dbinfo[testsuite] = []
                    dbinfo[testsuite].append(pair)
                else:
                    dbinfo[testsuite].append(pair)
            else:
                if savedTable not in dbinfo:
                    dbinfo[savedTable] = []
                    dbinfo[savedTable].append(pair)
                else:
                    dbinfo[savedTable].append(pair)
            
        columns = ""
        values = ""        
        for table in dbinfo:
            string =  ""
            if table == savedTable:
                string += "insert into " + savedTable + " ("
            else:
                savedTable = savedTable.replace('__OLD', '')
                string += "insert into " + savedTable + "__" + table + " (mac,testdatetime," 
            for pairs in dbinfo[table]:
                columns += pairs[0] + ','
                values += pairs[1] + "',"
                if pairs[0] == "mac":
                    mac = pairs[1]
                else:
                    pass
                if pairs[0] == "testdatetime":
                    testdatetime = pairs[1]
                else:
                    pass 
            columns = columns.rstrip(',')
            newcolumns = ""
            for column in columns.split(','):
                    count = len(re.findall("__", column))
                    if count <= 1:
                            column = column
                            newcolumns += column + ','
                    else:
                            column = column.replace(table + '__', '')
                            newcolumns += column + ','
            newcolumns = newcolumns.rstrip(',')
            values = values.rstrip(',')
            values = values.rstrip("'")
            values += "'"
            if table == savedTable:
                string += newcolumns + ") values (" + values + ")"
            else:
                string += newcolumns + ") values (" + mac + "'," + testdatetime + "'," + values + ")"
            if table == savedTable:
                testString = "select mac, testdatetime from " + table + " where mac = " + mac + "'"                
            else:
                testString = "select mac, testdatetime from " + savedTable + "__" + table + " where mac = " + mac + "'"
            cursor.execute(testString)
            results2 = cursor.fetchall()
            if results2:
                tableMac  = results2[0][0]
                tableDT   = results2[0][1]
                testmac            = mac.replace(" '", "")
                testTestdatetime   = testdatetime.replace(" '", "")
                tableDT = str(tableDT)
                if (testmac == tableMac and testTestdatetime == tableDT):
                        status.config(text="Mac and Date/Time already exists")
                else:
                        #print(string)
                        #cursor.execute(string)
                        status.config(text="Insert Complete")   
            else:
                print("inserting...")
                #cursor.execute(string)
                status.config(text="Insert Complete")        
            conn.commit()
            columns = ""
            values = ""
            time.sleep(.1)
        insert.close()
        src = root.filePath
        dst = "Processed_Files"
        try:
            #shutil.move(src, dst)
            pass
        except OSError:
            pass
        status.config(text="Select Next File")
        print("Select Next File")    
root = tk.Tk(className= " Insert Statement Application " )
root.geometry("800x300")
pathLbl = tk.Label(root, width=10,height=1, anchor='w', text="Path")
pathLbl.grid(row=0, column=0)
pathAct = tk.Label(root, width=120,height=1, anchor='w', text="------")
pathAct.grid(row=0, column=1)
statusLbl = tk.Label(root, width=10,height=1,  anchor='w', text="Status")
statusLbl.grid(row=1, column=0)
status = tk.Label(root, width=20,height=1,  anchor='w', text="")
status.grid(row=1, column=1)
exitBtn = tk.Button(root, width=20,height=1,  anchor='center', command = exitProgram, text ="Exit")
exitBtn.grid(row=2, column=0)
nextBtn = tk.Button(root, width=20,height=1,  anchor='center', command =insertData, text ="Next Insert")
nextBtn.grid(row=2, column=1)

filepath =  filedialog.askopenfilename(initialdir = "Desktop",title = "Select Special Test Cases")
if not filepath:
        root.destroy()
        exit()
else:
        pass
insertData()
root.mainloop()
    
    
    

    


























