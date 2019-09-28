import pyodbc
import csv

# ESTABLISH CONNECTION PARAMETERS

conn = pyodbc.connect(
    'Driver={ODBC Driver 17 for SQL Server};'
    'Server=tcp:[server].database.windows.net,1433;'
    'Database=[db name];'
    'Uid=[username];'
    'Pwd=[{password}];'
    'Encrypt=yes;'
    'TrustServerCertificate=no;'
    'Connection Timeout=30;')


    # CHECK DB

cursor = conn.cursor()
cursor.execute('SELECT * FROM INFORMATION_SCHEMA.TABLES')

for row in cursor:
    print (f'row={row}')


# CREATE TABLE COMMAND
cursor.execute('CREATE TABLE demo_table(testcolumn1 int, testcolumn2 int)')



#INSERT ROW COMMAND

cursor.execute('insert into demo_table(testcolumn1,testcolumn2) values (?,?);', (4157,4076))

# SELECT COMMAND

cursor = conn.cursor()
cursor.execute("SELECT TOP (10) * FROM  demo_table")
row = cursor.fetchone()
while row:
    print (row)
    row = cursor.fetchone()


#RETRY LOGIC

import time

retry_flag = True
retry_count = 0
cursor = conn.cursor()
while retry_flag and retry_count < 5:
    try:
        cursor.execute("SELECT TOP (10) * FROM  demo_table", [args['type'], args['id']])
        retry_flag = False
    except:
        print ("Retry after 1 sec")
        retry_count = retry_count + 1
        time.sleep(1)


#BULK insert

with open ('test.csv', 'r') as f:
    reader = csv.reader(f)
    columns = next(reader)
    print(columns)
    query = 'insert into SalesLT.DemoTable2({0}) values ({1});'
    print(query)
    query = query.format(','.join(columns), ','.join('?' * len(columns)))
    print(query)

    cursor = conn.cursor()
    for data in reader:
        print(query,data)
        cursor.execute(query, data)
    cursor.commit()
