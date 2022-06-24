import time
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import sqlalchemy
import configparser

# 행: 100,000, 열: 40, 파일 크기: 27.9MB
df = pd.read_csv("./predict_ops.csv", encoding='utf-8', usecols=['name', 'team', 'OPS', 'prediction_OPS'])
df.columns = ['name', 'team' , 'ops', 'predict_ops']
df['ops_predict_id'] = df.index

# params
user = "root"
password = "12341234"
host = "ybo-phase1.cgkn3au7spxb.ap-northeast-2.rds.amazonaws.com"
port = 3306
database = "ybo_db"


# DB 접속 엔진 객체 생성
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', encoding='utf-8')

# DB 테이블 명
table_name = "ops_predict"

dtypesql = {'ops_predict_id': sqlalchemy.types.Integer, 
            'name': sqlalchemy.types.VARCHAR(255), 
            'team': sqlalchemy.types.VARCHAR(255), 
            'ops': sqlalchemy.types.Float,
            'predict_ops': sqlalchemy.types.Float
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
    con.execute('ALTER TABLE `ops_predict` ADD PRIMARY KEY (`ops_predict_id`);')