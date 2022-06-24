import time
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import sqlalchemy
import configparser

# 행: 100,000, 열: 40, 파일 크기: 27.9MB
df1 = pd.read_csv("./era2022.csv", encoding='utf-8', usecols=["name", "team", "WAR"])
df1.columns = ['name', 'team', 'war']
df2 = pd.read_csv("./ops2022.csv", encoding='utf-8', usecols=["name", "team", "WAR+"])
df2.columns = ['name', 'team', 'war']
df = pd.concat([df1,df2], ignore_index=True)
df['war_id'] = df.index

# params
user = "root"
password = "12341234"
host = "ybo-phase1.cgkn3au7spxb.ap-northeast-2.rds.amazonaws.com"
port = 3306
database = "ybo_db"


# DB 접속 엔진 객체 생성
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', encoding='utf-8')

# DB 테이블 명
table_name = "war"

dtypesql = {'war_id': sqlalchemy.types.Integer, 
            'name': sqlalchemy.types.VARCHAR(255), 
            'team': sqlalchemy.types.VARCHAR(255), 
            'war': sqlalchemy.types.Float
}

# DB에 DataFrame 적재
df.to_sql(index = False,
          name = table_name,
          con = engine,
          if_exists = 'replace',
          method = 'multi', 
          chunksize = 10000,
          dtype=dtypesql)

with engine.connect() as con:
    con.execute('ALTER TABLE `war` ADD PRIMARY KEY (`war_id`);')