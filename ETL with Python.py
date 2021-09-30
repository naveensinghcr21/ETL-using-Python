import petl as etl, sys

from platform import python_version

print(python_version())


print(sys.getdefaultencoding())

import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(host='localhost',

                                         user='root',
                                         password='Iamwhatiam21@')

    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("show databases")
        record = cursor.fetchall()

        records_list = []
        for i in record:
            x = str(list(i))

            records_list.append(x.replace("[", "").replace("]", "").replace("'", ""))
        print("database lists", records_list)




except Error as e:
    print("Error while connecting to MySQL", e)

user_selected_sourcedatabase = str(input("enter your source database"))
print(user_selected_sourcedatabase)


if user_selected_sourcedatabase in records_list:
    cursor.execute("use %s" %user_selected_sourcedatabase)
    cursor.execute("show tables")
    tables=cursor.fetchall()
    #filtering out unwanted characters from list items:
    tables_list = []
    for i in tables:
        x = str(list(i))

        tables_list.append(x.replace("[", "").replace("]", "").replace("'", ""))
    print("Tables Lists : ", tables_list)
    #select table you want to work with:

else:
    print("no such database %s" %user_selected_sourcedatabase)

#converting tables from tuple format to str
number_of_sourcetables = int(input("how many tables you want to extract:"))
extracted_sourcetables_lists = []
for i in range(number_of_sourcetables):
    extracted_sourcetables_lists.append(input("tables you want to work with:"))
print("list of tables you selected : ",extracted_sourcetables_lists)

for i in extracted_sourcetables_lists:

        cursor.execute("select * from %s limit 100" %i)
        print("tables : " ,cursor.fetchall())

#working with source database
user_selected_targetdatabase = str(input("enter your Target database"))
print(user_selected_targetdatabase)

#ETL:

if user_selected_sourcedatabase in records_list:

    for t in extracted_sourcetables_lists:
        cursor.execute("use %s" %user_selected_targetdatabase)
        cursor.execute("show tables")
        print(cursor.fetchall())
        print(t)
        cursor.execute("drop table if exists %s" %t)
        sourceDs=etl.fromdb(user_selected_sourcedatabase,"select * from %s" %t)
        etl.todb(sourceDs,user_selected_targetdatabase,t,create=True,sample=1000)
cursor.execute("show tables")
y=cursor.fetchall()
print(y)





