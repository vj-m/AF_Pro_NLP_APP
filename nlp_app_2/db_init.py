import mysql.connector
from mysql.connector import connect, Error
from db_config import config
import logging
import pandas as pd
import time
from sqlalchemy import create_engine, inspect

logging.basicConfig(filename='dev.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)

try:
    db = connect(**config)
except Error as e:
    logging.debug(" ********* Exception in connecting to db in db_init{} ********** ".format(e))
    print(e)

try:
    cursor = db.cursor()
    cursor.execute("DROP TABLE test")
    #cursor.execute("CREATE DATABASE nlpdb;")
    cursor.execute("CREATE TABLE test"
    "(id INT, name VARCHAR(100), summary VARCHAR(150),description VARCHAR(300),position VARCHAR(100), category VARCHAR(100), url VARCHAR(500), video_thumbnail INT, videoThumbnailUrl VARCHAR(300), support VARCHAR(400));")
    #print("Created Table 'test' ")
    #cursor.execute("SELECT * FROM test;")

except Error as e:
    logging.debug(" ********* Exception in Creating in DB db_init {} ********** ".format(e))


try:
    st = time.time()
    data_df = pd.read_excel('convertcsv.xlsx', sheet_name='Sheet 1')
    data_df.drop(columns=[
        'deleted',
        'sport',
        'creator',
        'creatorId',
        'dateCreated',
        'dateDeleted',
        'fileAttachment',
        'lastUpdated',
        'visibility',
        'durationSec',
        'evaluationId'
    ], axis=1, inplace=True)

    # Create new columns for Embedding and Similarity Score
    data_df['embedding'] = ''
    data_df['score'] = 0
    data_df['support'] = ''

    print(data_df.columns)
    print('Time taken to load pd from excel, drop and add new columns(secs) = ', time.time() - st)

    st = time.time()
    engine = create_engine("mysql+mysqlconnector://yuri:mysql_dev123@34.231.134.0/nlpdb")
    print('Time taken to create engine(secs) = ', time.time() - st)

    st = time.time()
    data_df.to_sql('test', engine, schema=None, if_exists='replace', index=True, index_label=None, chunksize=None,
                   dtype=None, method=None)

    print('Time taken to load entire data into table(secs) = ', time.time() - st)

    inspector = inspect(engine)
    for table_name in inspector.get_table_names():
        print(table_name)
        logging.debug(" ********* Table Names in DB db_init {} ********** ".format(table_name))

except Exception as e:
    logging.debug(" ********* Exception in Loading to DF in DB db_init {} ********** ".format(e))