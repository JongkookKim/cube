import phoenixdb
import phoenixdb.cursor

import pandas as pd
import math as mh

# convert Json type data format to PostgreSQL insert SQL
import sqlalchemy
from sqlalchemy import create_engine


### Specify explicitily Variables ###
database_url = 'http://210.179.60.129:8765/'
conn = phoenixdb.connect(database_url, autocommit=True)
l_strSQL = ""

#create postgresql engine
pg_engine = create_engine("postgresql://ese:smart1196!!@210.179.66.88:5432/RINO_SMARTCON")


l_strSQL = l_strSQL + "select resourceid, content, timeofoccurrencelong, labels "
l_strSQL = l_strSQL + "from contentinstance "
l_strSQL = l_strSQL + "where parentid like 'origin%' "
#l_strSQL = l_strSQL + "order by timeofoccurrencelong desc limit 10"
l_strSQL = l_strSQL + "limit 10"

cursor = conn.cursor()
#cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username VARCHAR)")
#cursor.execute("UPSERT INTO users VALUES (?, ?)", (1, 'admin'))
#cursor.execute("select resourceid, content, timeofoccurrencelong, labels from contentinstance where parentid like '%origin' order by timeofoccurrencelong desc limit 10")
cursor.execute(l_strSQL)

#print(cursor.fetchall())
getData = cursor.fetchall()

### fetch to Pandas Dataframe ###
df = pd.DataFrame(getData)
df.columns = ['resourceid','content','timeofoccurrencelong','labels']

l_intRow = 0

for rdata in df['labels']:
    print(df.at[l_intRow, 'labels'])
    if mh.ceil(l_intRow % 2) == 1 :
        df.at[l_intRow, 'labels'] = 'A'
    l_intRow = l_intRow + 1

# print(df[df["labels"]=='A'])

l_label_a = df[df["labels"]=='A']
l_label_sharp = df[df["labels"]=='#']
# print(df)
# print(l_label_a)
# print(l_label_sharp)
# print(df['content'], df['labels'])
# l_split_data = df['content'].str.split(",")
# l_content = l_split_data.to_list()

l_split_cont_a = l_label_a['content'].str.split(",")
l_content_a = l_split_cont_a.to_list()
print(l_split_cont_a)
#l_content_colname = [""]
# print(l_split_data)

l_maxTime = df['timeofoccurrencelong'].max()
print(l_maxTime)

##########################################################################################################################################
# Type : Function
# Name : f_Insert_
def 
