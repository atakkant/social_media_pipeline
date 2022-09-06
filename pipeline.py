import pandas as pd
from sqlalchemy import create_engine
import time
import os

from dotenv import load_dotenv
load_dotenv()

users_url = "https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users"
messages_url = "https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages"

users_columns = ['createdAt', 'updatedAt','city','country', 'email', 'profile', 'subscription','id']
messages_columns = ['createdAt', 'receiverId', 'id', 'senderId']

users_table = 'users'
messages_table = 'messages'
subs_table = 'subscriptions'

def from_json_to_df(url,columns=None):
    df = pd.read_json(url)
    if columns:
        df = df[columns]
    return df

def get_domain(email):
    splitted = email.split("@")
    if len(splitted) > 1:
        return splitted[1]

def clean_domain_from_df(df):
    df['email'] = df['email'].apply(get_domain)
    df = df.rename(columns={'email':'email_domain'})
    return df

def create_subscription_df(df):
    subs_list = []
    for index,sub in df.iterrows():
        subs_l = df.loc[index,'subscription']
        if type(subs_l) == list:
            for r in subs_l:
                r['user_id'] = sub['id']
                subs_list.append(r)
        else:
            subs_l['user_id'] = sub['id']
            subs_list.append(subs_l)

    subs_df = pd.DataFrame(subs_list)
    return subs_df


def create_profile_columns(df):
    length = len(df)
    df['gender'] = ""
    df['isSmoking'] = ""
    df['income'] = ""    
    for index in range(length-1):
        subs_l = df.loc[index,'profile']
        df.loc[index,'gender'] = subs_l.get('gender')
        df.loc[index,'isSmoking'] = subs_l.get('isSmoking')
        df.loc[index,'income'] = subs_l.get('income')
    return df


def postgres_connect():
    postgres_user = os.getenv('POSTGRES_USER')
    passw = os.getenv('PASSWORD')
    port = os.getenv('PORT')
    host = os.getenv('HOST')
    dbname = os.getenv('DBNAME')
    con_uri = f"postgresql://{postgres_user}:{passw}@{host}:{port}/{dbname}"
    try:
        engine = create_engine(con_uri,pool_pre_ping=True)  
    except Exception as e:
        logging.error(e)
        engine = None
    
    return engine

def load_data_to_db(df,table_name,engine):
    print(f"start loading data to {table_name}")
    df.columns = df.columns.str.strip().str.lower()
    res = df.to_sql(
        name=table_name,
        con=engine,
        index=False,
        if_exists='append'
    )
    print("loading completed")
    return res

def transform_users(users_list,users_columns):
    print("transforming users is started")
    users = from_json_to_df(users_list,users_columns)
    users = clean_domain_from_df(users)
    users = create_profile_columns(users)
    del users['profile']
    print("transforming users is completed")
    return users

def transform_messages(messages_url,messages_columns):
    print("transforming messages is started")
    messages = from_json_to_df(messages_url,messages_columns)
    print("transforming messages is completed")
    print(messages.info())
    return messages

def transform_subscription(users_df):
    print("transforming subscription is started")
    subs_df = create_subscription_df(users_df)
    subs_df = subs_df.rename(columns={'status':'subs_status'})
    print("transforming users is completed")
    print(subs_df.info())
    return subs_df

def main(**kwargs):
    users_url = kwargs.get('users_url')
    users_columns = kwargs.get('users_columns')
    messages_url = kwargs.get('messages_url')
    messages_columns = kwargs.get('messages_columns')
    users_table = kwargs.get('users_table')
    messages_table = kwargs.get('messages_table')
    subs_table = kwargs.get('subs_table')

    users = transform_users(users_url,users_columns)
    print(users.info())
    messages = transform_messages(messages_url,messages_columns)
    subs_df = transform_subscription(users)
    del users['subscription']

    engine = postgres_connect()

    res = load_data_to_db(users,users_table,engine)
    print(f"number of rows loaded to {users_table}: {res}")

    rs = load_data_to_db(messages,messages_table,engine)
    print(f"number of rows loaded to {messages_table}: {rs}")

    r = load_data_to_db(subs_df,subs_table,engine)
    print(f"number of rows loaded to {subs_table}: {r}")


args_list = {
    'users_url': users_url,
    'users_columns': users_columns,
    'messages_url': messages_url,
    'messages_columns': messages_columns,
    'users_table': users_table,
    'messages_table': messages_table,
    'subs_table': subs_table    
}

if __name__ == '__main__':
    start = time.time()
    print("Data transfer started")
    main(**args_list)
    print("data transfer completed")
    end = time.time()
    total_time = end-start
    print(f"total time of process: {total_time}")