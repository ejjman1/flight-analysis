# author: Emanuele Salonico, 2023


import psycopg2 #postgre
import pyodbc #mssql
import pandas as pd
import numpy as np
import ast
import psycopg2.extras as extras
import os
import logging

# logging
logger_name = os.path.basename(__file__)
logger = logging.getLogger(logger_name)


class Database:
    def __init__(self, db_host, db_name, db_user, db_pw, db_table, db_sql):
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_table = db_table
        self.__db_pw = db_pw
        self.db_sql = db_sql
        if(db_sql.lower() == 'postgre'):
            self.conn = self.connect_to_postgresql()
        elif(db_sql.lower() == 'mssql'):
            self.conn = self.connect_to_mssql()
        else:
            raise ValueError("db_sql field incorrect. Please use 'postgre' or 'mssql'.")
        self.conn.autocommit = True

    def __repr__(self):
        return f"Database: {self.db_name}"

    def connect_to_postgresql(self):
        """
        Connect to Postgresql and return a connection object.
        """
        try:
            conn = psycopg2.connect(host=self.db_host,
                                    database=self.db_name,
                                    user=self.db_user,
                                    password=self.__db_pw)
            return conn
        except Exception as e:
            raise ConnectionError(e)
    
    def connect_to_mssql(self):
        """
        Connect to Microsoft SQL and return a connection object.
        """
        try:
            conn = pyodbc.connect('DRIVER={SQL SERVER};User ID='+self.db_user+';Password='+self.__db_pw+';Server='+self.db_host+';Database='+self.db_name)
            return conn
        except Exception as e:
            raise ConnectionError(e)

    def list_all_databases(self):
        cursor = self.conn.cursor()
        if self.db_sql == "postgre":
            cursor.execute(
                "SELECT datname FROM pg_database WHERE datistemplate = false;")
        else:
            cursor.execute(
                "SELECT name FROM sys.databases WHERE database_id > 4;")

        result = cursor.fetchall()
        cursor.close()

        return [x[0] for x in result]

    def list_all_tables(self):
        cursor = self.conn.cursor()
        if(self.db_sql == 'postgre'):
            cursor.execute(
                "SELECT * FROM information_schema.tables WHERE table_schema = 'public';")
            result = cursor.fetchall()
            cursor.close()
            return [x[2] for x in result]
        else:
            cursor.execute(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';")
            result = cursor.fetchall()
            cursor.close()
            return [x[0] for x in result]


    def create_db(self):
        """
        Creates a new database for flight_analysis data.
        """
        cursor = self.conn.cursor()
        if self.db_sql == 'postgre':
            query = """CREATE DATABASE flight_analysis WITH OWNER = postgres ENCODING = 'UTF8' CONNECTION LIMIT = -1 IS_TEMPLATE = False;"""
        else:
            query = 'CREATE DATABASE flight_analysis'
        cursor.execute(query)
        cursor.close()

        logger.info("Database [flight_analysis] created.")

    def create_scraped_table(self, overwrite):
        query = ""
        if overwrite:
            if self.db_sql == 'postgre':
                query += "DROP TABLE IF EXISTS public.scraped;\n"
            else:
                query += "USE flight_analysis; IF OBJECT_ID('scraped', 'U') IS NOT NULL DROP TABLE scraped;\n"

        if self.db_sql == 'postgre':
            # TODO this query needs tested.
            query += """
                CREATE TABLE IF NOT EXISTS public.scraped
                (
                    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
                    depart_departure_datetime timestamp with time zone,
                    depart_departure_day text COLLATE pg_catalog."default",
                    depart_arrival_datetime timestamp with time zone,
                    depart_arrival_day text COLLATE pg_catalog."default",
                    return_departure_datetime timestamp with time zone,
                    return_departure_day text COLLATE pg_catalog."default",
                    return_arrival_datetime timestamp with time zone,
                    return_arrival_day text COLLATE pg_catalog."default",
                    airlines text[] COLLATE pg_catalog."default",
                    travel_time smallint NOT NULL,
                    origin character(3) COLLATE pg_catalog."default"  NOT NULL,
                    destination character(3) COLLATE pg_catalog."default"  NOT NULL,
                    layover_n smallint NOT NULL,
                    layover_time numeric,
                    layover_location text COLLATE pg_catalog."default",
                    price smallint,
                    price_currency text COLLATE pg_catalog."default",
                    price_trend text COLLATE pg_catalog."default",
                    price_value text COLLATE pg_catalog."default",
                    access_date timestamp with time zone NOT NULL,
                    one_way boolean NOT NULL,
                    has_train boolean NOT NULL,
                    days_advance smallint NOT NULL
                )

                TABLESPACE pg_default;

                ALTER TABLE IF EXISTS public.scraped OWNER to postgres;
                """
        else:
            query += """
                USE flight_analysis;
                CREATE TABLE scraped
                (
                    id uniqueidentifier DEFAULT NEWID() PRIMARY KEY,
                    depart_departure_datetime datetime2(0),
                    depart_departure_day varchar(max),
                    depart_arrival_datetime datetime2(0),
                    depart_arrival_day varchar(max),
                    return_departure_datetime datetime2(0),
                    return_departure_day varchar(max),
                    return_arrival_datetime datetime2(0),
                    return_arrival_day varchar(max),
                    airlines varchar(max),
                    travel_time smallint NOT NULL,
                    origin char(3) NOT NULL,
                    destination char(3) NOT NULL,
                    layover_n smallint NOT NULL,
                    layover_time decimal(18, 2),
                    layover_location varchar(max),
                    price smallint,
                    price_currency varchar(max),
                    price_trend varchar(max),
                    price_value varchar(max),
                    access_date datetimeoffset NOT NULL,
                    one_way bit NOT NULL,
                    has_train bit NOT NULL,
                    days_advance smallint NOT NULL
                );
                """
            
        cursor = self.conn.cursor()
        cursor.execute(query)
        cursor.close()

    def prepare_db_and_tables(self, overwrite_table=False):
        # create database
        if 'flight_analysis' not in self.list_all_databases():
            self.create_db()

        # create table
        if self.db_sql == 'postgre':
            if 'public.scraped' not in self.list_all_tables():
                self.create_scraped_table(overwrite_table)
        else:
            if 'scraped' not in self.list_all_tables():
                self.create_scraped_table(overwrite_table)
        
    def transform_and_clean_df(self, df):
        """
        Some necessary cleaning and transforming operations to the df
        before sending its content to the database
        """
        df["airlines"] = df.airlines.apply(lambda x: np.array(ast.literal_eval(str(x).replace("[", '"{').replace("]", '}"'))))
        df["layover_location"] = df.layover_location.apply(lambda x: np.array(ast.literal_eval(str(x).replace("[", '"{').replace("]", '}"'))))
        df['layover_time'] = df['layover_time'].fillna(np.nan).replace([np.nan], [None])
        df["layover_location"] = df["layover_location"].fillna(np.nan).replace([np.nan], [None])
        df["price_value"] = df["price_value"].fillna(np.nan).replace([np.nan], [None])

        return df
        
    def add_pandas_df_to_db(self, df):
        # clean df
        df = self.transform_and_clean_df(df)
        
        # Create a list of tuples from the dataframe values
        tuples = [tuple(x) for x in df.to_numpy()]
    
        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))
    
        cursor = self.conn.cursor()
    
        # SQL quert to execute
        if self.db_sql == 'postgre':
            query  = "INSERT INTO %s(%s) VALUES %%s" % ('public.scraped', cols)
            try:
                extras.execute_values(cursor, query, tuples)
            except (Exception, psycopg2.DatabaseError) as error:
                logger.error("Error: %s" % error)
                self.conn.rollback()
                cursor.close()
            
            logger.info("{} rows added to table [{}]".format(len(df), self.db_table))
            cursor.close()
        else:
            query = f"INSERT INTO {self.db_table}({cols}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            try:
                cursor.executemany(query, tuples)
                logger.info("{} rows added to table [{}]".format(len(df), self.db_table))
            except (Exception, pyodbc.DatabaseError) as error:
                logger.error("Error: %s" % error)
                self.conn.rollback()

            cursor.close()
            


        
        # # fix layover time
        # # TODO: improve this
        # cursor = self.conn.cursor()
        # query = f"""
        #     UPDATE {self.db_table}
        #     SET layover_time = CASE
        #     WHEN layover_time = -1 THEN null ELSE layover_time END;

        #     ALTER TABLE public.scraped 
        #     ALTER COLUMN layover_time TYPE smallint;
        # """
        # cursor.execute(query)
        # cursor.close()