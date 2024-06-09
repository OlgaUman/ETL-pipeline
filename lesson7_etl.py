from datetime import datetime, timedelta
import pandas as pd

import pandahouse as ph

from airflow.decorators import dag, task

import os


# Подключение к исходной базе
connection_get = {
    'host': os.getenv('HOST'),
    'password': os.getenv('PASSWORD'),
    'user': os.getenv('USER')
}

# Подключение для выгрузки
connection_load = {
    'host': os.getenv('HOST'),
    'password': os.getenv('PASSWORD_LOAD'),
    'user': os.getenv('USER_LOAD'),
    'database': 'test'
}

# Параметры для тасков
default_args = {
    'owner': os.getenv('OWNER'),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 5, 20)
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

# Функция для преобразования данных
def transform_types(df):
    df = df[['event_date', 'dimension', 'dimension_value', 'views', 'likes', 
            'messages_received', 'messages_sent', 'users_received', 'users_sent']]
    df = df.astype({'dimension': 'object', 'dimension_value': 'object', 
                              'views': 'int', 'likes': 'int', 
                              'messages_received': 'int', 'messages_sent': 'int', 
                              'users_received': 'int', 'users_sent': 'int'})
    return df

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_get_metrics():
    
    # Выгружаем данные из feed_astions
    @task()
    def extract_feeds():
        q_feed = '''
            SELECT
                toDate(time) AS event_date,
                user_id,
                gender,
                os,
                age,
                count(action='view') as views,
                count(action='like') as likes
            FROM
                simulator_20240420.feed_actions
            WHERE toDate(time) = yesterday()
            GROUP BY event_date, user_id, gender, os, age
        '''
        feed_df = ph.read_clickhouse(query=q_feed, connection=connection_get)
        return feed_df
    
    # Выгружаем данные из message_astions
    @task
    def extract_mes():
        q_message = '''
            WITH t1 AS (
                SELECT
                    toDate(time) AS event_date,
                    user_id, gender, os, age,
                    COUNT(receiver_id) AS messages_sent,
                    COUNT(DISTINCT receiver_id) AS users_sent
                FROM simulator_20240420.message_actions
                WHERE toDate(time) = yesterday()
                GROUP BY event_date, user_id, gender, os, age
                ),
                t2 AS (
                SELECT
                    receiver_id AS user_id, 
                    COUNT(user_id) AS messages_received,
                    COUNT(DISTINCT user_id) AS users_received
                FROM simulator_20240420.message_actions
                WHERE toDate(time) = yesterday()
                GROUP BY receiver_id
                )
                
            SELECT
                event_date,
                user_id, gender, os, age,
                messages_sent, users_sent,
                messages_received, users_received
            FROM t1
            LEFT JOIN t2 USING user_id
        '''
        mes_df = ph.read_clickhouse(query=q_message, connection=connection_get)
        return mes_df
    
    # Объединяем данные
    @task()
    def merge_df(feed_df, mes_df):
        total_df = feed_df.merge(mes_df, how='outer', 
                                 on=['event_date','user_id', 'gender', 'os', 'age'])
        total_df = total_df.fillna(0)
        return total_df

    # Срез по gender
    @task()
    def get_gender_metrics(total_df):
        gender_df = total_df.drop(columns=['user_id', 'os', 'age']) \
            .groupby(['event_date','gender'], as_index=False).sum()
        gender_df = gender_df.rename(columns={'gender': 'dimension_value'})
        gender_df['dimension'] = 'gender'
        gender_df = transform_types(gender_df)
        return gender_df
    
    # Срез по os
    @task()
    def get_os_metrics(total_df):
        os_df = total_df.drop(columns=['user_id', 'gender', 'age']) \
            .groupby(['event_date', 'os'], as_index=False).sum()
        os_df = os_df.rename(columns={'os': 'dimension_value'})
        os_df['dimension'] = 'os'
        os_df = transform_types(os_df)
        return os_df
    
    # Срез по age
    @task()
    def get_age_metrics(total_df):
        age_df = total_df.drop(columns=['user_id', 'os', 'gender']) \
            .groupby(['event_date', 'age'], as_index=False).sum()
        age_df = age_df.rename(columns={'age': 'dimension_value'})
        age_df['dimension'] = 'age'
        age_df = transform_types(age_df)
        return age_df
    
    # Объединяем данные
    @task
    def concat_data(gender_df, os_df, age_df):
        full_df = pd.concat([gender_df, os_df, age_df])
        return full_df
    
    # Загружаем данные в таблицу
    @task()
    def load_data(full_df):
        # Создаем таблицу для загрузки данных
        query_create_table = '''
            CREATE TABLE IF NOT EXISTS test.final_table
                (event_date Date,
                dimension String,
                dimension_value String,
                views Int64,
                likes Int64,
                messages_received Int64,
                messages_sent Int64,
                users_received Int64,
                users_sent Int64
                )
            ENGINE = MergeTree()
            ORDER BY event_date
        '''
        ph.execute(query=query_create_table, connection=connection_load)
        
        ph.to_clickhouse(df=full_df, table='final_table', 
                         index=False, connection=connection_load)
        
    
    # Устанавливаем зависимости
    feed_df = extract_feeds()
    mes_df = extract_mes()
    total_df = merge_df(feed_df, mes_df)
    gender_df = get_gender_metrics(total_df)
    os_df = get_os_metrics(total_df)
    age_df = get_age_metrics(total_df)
    full_df = concat_data(gender_df, os_df, age_df)
    load_data(full_df)
    
# Запускаем DAG
dag_get_metrics = dag_get_metrics()