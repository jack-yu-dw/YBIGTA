import time
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import sqlalchemy
import configparser

# 행: 100,000, 열: 40, 파일 크기: 27.9MB
# WAR+ 를 WAR 대신 써도 되는건지?
df = pd.read_csv("./ops2022.csv", encoding='utf-8', usecols=["name", "team", "position", "WAR+", "game수", "타석", "안타", "홈런", "타점", "도루", "삼진", "병살", "타율", "출루", "장타", "OPS"])
df.columns = ['name', 'team', 'position', 'war', 'games', 'pa', 'hit', 'homerun', 'rbi', 'steal', 'strikeout', 'dp', 'ba', 'obp', 'slg', 'ops']
df['hitter_id'] = df.index

# params
user = "root"
password = "12341234"
host = "ybo-phase1.cgkn3au7spxb.ap-northeast-2.rds.amazonaws.com"
port = 3306
database = "ybo_db"


# DB 접속 엔진 객체 생성
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', encoding='utf-8')

# DB 테이블 명
table_name = "hitter"

dtypesql = {'hitter_id': sqlalchemy.types.Integer, 
            'name': sqlalchemy.types.VARCHAR(255), 
            'team': sqlalchemy.types.VARCHAR(255),
            'position': sqlalchemy.types.VARCHAR(20), 
            'war': sqlalchemy.types.Float,
            'games': sqlalchemy.types.Integer,
            'pa': sqlalchemy.types.Integer, #타석
            'hit': sqlalchemy.types.Integer,  #안타
            'homerun': sqlalchemy.types.Integer,
            'rbi': sqlalchemy.types.Integer,    #타점
            'steal': sqlalchemy.types.Integer,  #도루
            'strikeout': sqlalchemy.types.Integer,  #삼진
            'dp': sqlalchemy.types.Integer, #병살
            'ba': sqlalchemy.types.Float, #타율
            'obp': sqlalchemy.types.Float, #출루율
            'slg': sqlalchemy.types.Float, #장타율
            'ops': sqlalchemy.types.Float   #ops
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
    con.execute('ALTER TABLE `hitter` ADD PRIMARY KEY (`hitter_id`);')