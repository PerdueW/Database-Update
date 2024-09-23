#!/usr/bin/python

import mysql.connector
from mysql.connector import Error 
import time
from tkinter import messagebox
from tkinter import filedialog
from tkinter import *
import tkinter as tk
import os
import shutil

host = ""
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

def Diff(li1, li2):
    return (list(list(set(li1)-set(li2)) + list(set(li2)-set(li1))))

def exitProgram():
    root.destroy()

def insertData():        
    # This portion of insertData pulls data from the saved insert statement text file to process its information for use with
    # determining it data to the database.
    root.filePath =  filedialog.askopenfilename(initialdir = "Desktop",title = "Select Saved Insert")
    if not root.filePath:
        root.destroy()
        exit()
    else:
        pass
    pathAct.config(text=root.filePath)
    insertStatement = open(root.filePath, 'r')
    insert = insertStatement.read()
    insertmodel = insert[insert.find('into ') + 5: insert.find('(')]
    insertcolumns = insert[insert.find(' (') + 2: insert.find(') values')]
    insertvalues = insert[insert.find('values(') + 7: insert.rfind(')')]
    insertmodel = insertmodel.strip()
    insertcolumns = insertcolumns.split(",")
    insertvalues = insertvalues.split(",")
    
    dbinfo = {}
    def merge(list1, list2):       
        merged_list = [(list1[i], list2[i]) for i in range(0, len(list1))] 
        return merged_list
    columnvaluepair = merge(insertcolumns, insertvalues)
    
    if insertmodel in modelscases:
        print(insertmodel + " is in modelsases")
    else:
        print(insertmodel + " is not in modelsases")
        query1 = "SELECT `COLUMN_NAME`FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_NAME`='" + insertmodel + "'"
        cursor.execute(query1)
        dbtableColumns = cursor.fetchall()
        for dbcolumn in dbtableColumns:
            dbcolumn = dbcolumn[0]
            dbcolumn = dbcolumn.strip()
            cleaneddbColumns.append(dbcolumn) 
    
    print("The different columns are ")
    additionalColumns = (Diff(cleaneddbColumns, insertcolumns))
    print(additionalColumns)
    print(type(additionalColumns))
    print("")
    

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
nextBtn = tk.Button(root, width=20,height=1,  anchor='center', command =insertData, text ="Next Insert")
nextBtn.grid(row=2, column=0)
exitBtn = tk.Button(root, width=20,height=1,  anchor='center', command = exitProgram, text ="Exit")
exitBtn.grid(row=2, column=1)

connected = False
# This portion of insertdata establishes an connection to the database and if no connection is present alerts
# the user there is no connection and to check connection to databse.
try:
    conn = mysql.connector.connect(host=host,user=user,password=password,database=database)
    connected  = True
except mysql.connector.Error as error:
    print(error)
    print('Please check your connection to the database')
    messagebox.showerror("DB Connection Error", "Check DB Connection")
    root.destroy()
    exit()
if connected:
    cursor = conn.cursor()
    
# This portion of insertData pulls data from the SpecCases text file to process its information for use with
# determining special cases for inserting data to the database.
modelscases = {} 
speccasespath =  filedialog.askopenfilename(initialdir = "Desktop",title = "Select Special Test Cases")
if not speccasespath:
    root.destroy()
    exit()
else:
    pass
with open(speccasespath, "r") as fp:
    for line in fp:
        line = line.replace('-', '_')
        speccasesmodel = line.split('=')[0]
        specCases = line.split('=')[1]
        modelscases[speccasesmodel] = specCases
insertData()
root.mainloop()
    
