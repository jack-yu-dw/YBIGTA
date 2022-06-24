import time
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import sqlalchemy
import configparser

# 행: 100,000, 열: 40, 파일 크기: 27.9MB
df = pd.read_csv("./era2022.csv", encoding='utf-8', usecols=["name", "team", "WAR", "승", "패", "이닝", "실점", "자책", "피안타", "홈런", "볼넷", "삼진", "ERA", "WPA"])
df.columns = ['name', 'team', 'war', 'win', 'lose', 'inning', 'runs', 'earned_run', 'hit', 'homerun', 'bb', 'strikeout', 'era', 'wpa']
df['pitcher_id'] = df.index

# params
user = "root"
password = "12341234"
host = "ybo-phase1.cgkn3au7spxb.ap-northeast-2.rds.amazonaws.com"
port = 3306
database = "ybo_db"


# DB 접속 엔진 객체 생성
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', encoding='utf-8')

# DB 테이블 명
table_name = "pitcher"

dtypesql = {'pitcher_id': sqlalchemy.types.Integer, 
            'name': sqlalchemy.types.VARCHAR(255), 
            'team': sqlalchemy.types.VARCHAR(255), 
            'war': sqlalchemy.types.Float,
            'win': sqlalchemy.types.Integer,
            'lose': sqlalchemy.types.Integer,
            'inning': sqlalchemy.types.Float,
            'runs': sqlalchemy.types.Integer,
            'earned_run': sqlalchemy.types.Integer,
            'hit': sqlalchemy.types.Integer,
            'homerun': sqlalchemy.types.Integer,
            'bb': sqlalchemy.types.Integer,
            'strikeout': sqlalchemy.types.Integer,
            'era': sqlalchemy.types.Float,
            'wpa': sqlalchemy.types.Float
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
    con.execute('ALTER TABLE `pitcher` ADD PRIMARY KEY (`pitcher_id`);')