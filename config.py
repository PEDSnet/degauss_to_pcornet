import configparser
import pandas as pd
import psycopg2
import psycopg2.extras

def config(filename):
    # create a parser
    parser = configparser.ConfigParser()
    # read config file
    parser.read(filename)
    # get the section, sefault to postgresql
    db = {}
    #check .ini files for section input, if true, returns credential info in form of a dictionary
    if parser.has_section('postgresql'):
        params = parser.items('postgresql')
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('file {} not found'.format(filename))
    return db

def get_db_connection(filename):
    #get database config info and connect
    params = config(filename)
    db_name = params['database']
    print('Connecting to the PostgreSQL database...')
    conn = psycopg2.connect(**params)
    return conn
