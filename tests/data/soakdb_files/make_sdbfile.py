import sqlite3
import pandas
import json
import os

cwd = os.path.dirname(os.path.realpath(__file__))

json_file = os.path.join(cwd, 'soakDBDataFile.json')
db = os.path.join(cwd, 'soakDBDataFile.sqlite')

json = json.load(open(json_file))

# write json to sqlite file
conn = sqlite3.connect(db)
df = pandas.DataFrame.from_dict(json)
df.to_sql("mainTable", conn, if_exists='replace')
conn.close()