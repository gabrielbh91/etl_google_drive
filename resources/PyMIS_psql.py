import pandas as pd
import time
#import pyodbc
import psycopg2

from decimal import Decimal
#from openpyxl import Workbook, load_workbook
from os import path
from re import compile, sub
#from unidecode import unidecode

# ambos erros podem ocorrer durante o restore do banco de dados.
#RESTORE_ERROR = pyodbc.ProgrammingError
#CONNECTION_ERROR = pyodbc.OperationalError

class Postgresql:

    def __init__(self,database = None):        
        if database:
            self.__psql_connection_name = database.split(':')[0] 
            self.__psql_connection = psycopg2.connect('postgresql://%s' % database)
            self.__psql_cursor = self.__psql_connection.cursor()
            print('connection with %s started.' % self.__psql_connection_name)
        else:
            print('Em desenvolvimento...')

    # Methods:
    def close(self):
        self.__psql_connection.close()
        print('connection with %s closed.' % (self.__psql_connection_name))
    
    def commit(self):
        self.__psql_connection.commit()
        print('changes commited.')

    def rollback(self):
        self.__psql_connection.rollback()
        print('changes rollbacked.')
    
    def run_query(self,query):
        self.__psql_cursor.execute(query)

    def run_query_values(self,query):
    # Este método não trata caracteres de quebra de linha e afins.
        self.__psql_cursor.execute(query)
        column = [i[0] for i in self.__psql_cursor.description]   
        data = [list(i) for i in self.__psql_cursor.fetchall()]
        Private_tools_postgresql.formatting_data(data)
        return  [column] + data
    
    def psql_to_df(self,query):
        self.__psql_cursor.execute(query)
        columns = [i[0] for i in self.__psql_cursor.description]
        data_pyodbc = self.__psql_cursor.fetchall()
        data = Private_tools_postgresql.process_data_psql_to_df(data_pyodbc)
        data_frame = pd.DataFrame(data, columns = columns)
        return data_frame

    def df_to_psql(self,table_name,data_frame, insert = False, istext = [], isbigint = []):
        ### Insert = True caso deseja apenas carregar uma tabela já existente
        ### istext = lista com os nomes das colunas que deseja importar como text
        ### isbigint = lista com os nomes das colunas que deseja importar como bigint 

        cursor = self.__psql_cursor

        if type(data_frame) == pd.DataFrame:        
            command_create_table, data_type = Private_tools_postgresql.building_create_table_query_from_df(table_name,data_frame,istext,isbigint)
            df_columns = data_frame.columns
            table_and_columns = Private_tools_postgresql.string_table_and_columns(table_name,df_columns)
            data_frame_transformed = Private_tools_postgresql.transform_data_df_to_sql(data_frame,data_type)

        elif type(data_frame) == pd.Series:
            df_columns = [data_frame.name]
            comando_create_table, data_type = Private_tools_postgresql.building_create_table_query_from_series(table_name,data_frame,istext,isbigint)
            table_and_columns = table_and_columns = Private_tools_postgresql.string_table_and_columns(table_name,df_columns)
            data_frame_transformed = Private_tools_postgresql.transform_data_series_to_sql(data_frame,data_type)

        insert_commands_list = Private_tools_postgresql.building_command_insert(data_frame_transformed,table_and_columns)

        if not insert:
            cursor.execute(command_create_table)
        Private_tools_postgresql.import_to_psql(cursor,insert_commands_list)

class Private_tools_postgresql:

    def formatting_data(data):
        num_line = 0
        for line in data:
            num_column = 0
            for element in line:
                if element == None:
                    data[num_line][num_column] = 'NULL'
                elif type(element) == Decimal:
                    data[num_line][num_column] = float(element)
                num_column += 1
            num_line += 1
    
    def process_data_psql_to_df(data_psycopg):
        data = [] 
        for line in data_psycopg:
            data.append(tuple(line))
        return data


    def building_create_table_query_from_df(table_name,data_frame,istext,isbigint):
        data_type = {}
        query = f""" DROP TABLE IF EXISTS {table_name};
CREATE TABLE {table_name}("""

        if type(data_frame) == pd.DataFrame:
            columns = data_frame.columns
        elif type(data_frame) == pd.Series:
            columns = [data_frame.name]

        campos = []
        for column in columns:
            if column in istext:
                campos.append('%s text' % (column))
                data_type[column] = 'nao_numerico'
            elif column in isbigint:
                campos.append('%s bigint' % (column))
                data_type[column] = 'numerico'
            elif data_frame[column].dtype == 'int64': #Inteiro
                campos.append('%s int' % (column))
                data_type[column] = 'numerico'
            elif data_frame[column].dtype == 'float64': #Decimal
                campos.append('%s float' % (column))
                data_type[column] = 'numerico'
            elif data_frame[column].dtype == '<M8[ns]': #Data
                campos.append('%s datetime2' % (column))
                data_type[column] = 'nao_numerico'
            else:
                campos.append('%s varchar(10485760)' % (column))
                data_type[column] = 'nao_numerico'
        query = query + ','.join(campos) + ');'
        print(query)
        return query, data_type

    def string_table_and_columns(table_name,columns):
        campos = []
        for column in columns:
            campos.append('%s' % (column))
        table_and_columns = table_name +'('+ ','.join(campos) + ')'
        return table_and_columns

    def transform_data_df_to_sql(data_frame,data_type):
        data_frame.fillna('null', inplace = True) # remove NaN e NaT
        data_frame = data_frame.astype(str)
        for column in data_frame.columns:
            data_frame[column] = data_frame[column].str.replace("'","''") # Remove "aspas simples" no texto
            if data_type[column] == 'nao_numerico':
                data_frame[column] = '\'' + data_frame[column] + '\'' # Insert "aspas simples" nos valores não numericos
        
        data_frame = data_frame.replace('\'null\'','null')  # Tratando valores null
        return data_frame

    def building_create_table_query_from_series(table_name,data_frame,istext,isbigint):
        data_type = {}
        query = f"""DROP TABLE IF EXISTS {table_name};
CREATE TABLE {table_name}("""

        campo = data_frame.name

        campo_query = ''
        if campo in istext:
            campo_query = '%s text' % (campo)
            data_type = 'nao_numerico'
        elif campo in isbigint:
            campo_query = '%s bigint' % (campo)
            data_type = 'numerico'
        elif data_frame.dtype == 'int64': #Inteiro
            campo_query = '%s int' % (campo)
            data_type = 'numerico'
        elif data_frame.dtype == 'float64': #Decimal
            campo_query = '%s float' % (campo)
            data_type = 'numerico'
        elif data_frame.dtype == '<M8[ns]': #Data
            campo_query = '%s datetime2' % (campo)
            data_type = 'nao_numerico'
        else:
            campo_query = '%s varchar(10485760)' % (campo)
            data_type = 'nao_numerico'


        query = query + campo_query + ');'
        return query, data_type

    def transform_data_series_to_sql(serie,data_type):
        serie.fillna('null', inplace = True) # Tratando NaN e NaT
        serie = serie.astype(str)
        serie = serie.str.replace("'","''") # Remove aspas simples do texto
            
        if data_type == 'nao_numerico':
            serie = '\'' + serie + '\'' # Incluí aspas simples nos valores não numericos
        
        serie = serie.replace('\'null\'','null')  #Tratando valores null

        return serie

    def building_command_insert(tranformed_dataframe,tables_and_columns):
        base = tranformed_dataframe.values.tolist()
        insert_commands_list = []
        range_insert_value = 10000
        for line in range(0,len(base),range_insert_value):
            values_formated_list = []
            for values in base[line:(line+range_insert_value)]:
                join_values = ','.join(values) if type(values) == list else values
                formated_values = '(%s)' % (join_values)
                values_formated_list.append(formated_values)
            insert_lines_command ='insert into %s values %s' % (tables_and_columns,','.join(values_formated_list))
            insert_commands_list.append(insert_lines_command)

        return insert_commands_list

    def import_to_psql(cursor,insert_commands_list):
        for command in insert_commands_list:
            cursor.execute(command)