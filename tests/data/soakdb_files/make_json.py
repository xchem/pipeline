import sqlite3
import json
 

def dict_factory(cur, row):
    d = {}
    for idx, col in enumerate(cur.description):
        d[col[0]] = row[idx]
    return d
 

connection = sqlite3.connect("soakDBDataFile.sqlite")
connection.row_factory = dict_factory
 
cursor = connection.cursor()
 
cursor.execute("select * from mainTable")
 
# fetch all or one we'll go for all.
 
results = cursor.fetchall()
 
with open('soakDBDataFile.json', 'w') as f:
    json.dump(results, f)
 
connection.close()
