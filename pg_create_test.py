import pandas as pd

# convert Json type data format to PostgreSQL insert SQL
import sqlalchemy as pgdb
from sqlalchemy import create_engine
#import psycopg2

#create postgresql engine
#pg_engine = create_engine("postgresql://RINO:rinoadmin@121.163.83.4:5432/RINO_SMARTCON")
pg_engine = create_engine("postgresql://rino:rinoadmin@show.ziumks.com:5432/RINO_SMARTCON")

score = pd.DataFrame({'id':['aaaa'],'date':['20210714'],'name':['kim'], 'age':['21'],'math_score':[99],'pass_yn':[True]})

pg_engine.execute("DROP TABLE IF EXISTS zium.score_temp;") # drop table if exists
score.to_sql(name='score_temp', 
            con=pg_engine,
            schema='zium',
            if_exists='append', # {'fail', 'replace', 'append'), default 'fail'
             index=False,
             #index_label='id',
             chunksize=2,
             dtype={'id':pgdb.types.VARCHAR(100),
                    'date':pgdb.DateTime(),
                    'name':pgdb.types.VARCHAR(100),
                    'age':pgdb.types.INTEGER(),
                    'math_score':pgdb.types.Float(precision=3),
                    'pass_yn':pgdb.types.Boolean()})