# -*- coding: utf-8 -*-
"""
Created on Wed Apr 29 00:49:39 2020

@author: marcia.cunha
"""

import pandas as pd
import glob #read files
import seaborn as sns
import numpy as np
import psycopg2
from datetime import datetime
from datetime import timedelta
from dateutil.parser import parse
import re

conn=''
path = r'C:\Users\marcia.cunha\Desktop\Acessos\gitdir\data-science-personal\Data-Science\ml-visual-sales-potential' # use your path
for filename in glob.glob(path + "/*.csv"):
    conn = pd.read_csv(filename, index_col=None, header=None, sep=';')
   # print(filename)

# DEFINE FUNCTION TO CONECT TO REDSHIFT
def get_data(sql):
    """connects to RSift and returns dataframe"""
    global conn
    connection_parameters = {
            'host': conn[0][1],
            'port': conn[1][1],
            'database': conn[2][1],
            'user': conn[3][1],
            'password': conn[4][1]
            }
    conn = psycopg2.connect(**connection_parameters)
    cur = conn.cursor()
    cur.execute(sql)
    columns = [item.name for item in cur.description]
    results = cur.fetchall()
    return pd.DataFrame(results, columns=columns);

# READ THE QUERY FILE AND TRANSFORM THE TEXT TO AVOID ERROR WHEN RUNNING
def get_query_from_file(file_name):
    # READ SQL QUERY FILE
    file = open(file_name)
    query = file.read();
    file.close();
    # REMOVE COMMENTS IN THE QUERY
    comment = re.compile('\-{2}(.+?)\\n')
    query = comment.sub(' ', query).replace('\n', ' ').replace('  ', ' ')
    return query;
# READ SQL QUERY FILE
query = get_query_from_file(path +"\product_database.sql")
# LOAD DATAFRAME FROM QUERY RESULT
product_base = get_data(query)

# -*- coding: utf-8 -*-

