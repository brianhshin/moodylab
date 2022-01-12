import os
import sys
import psycopg2
import boto3
import argparse
import logging
from datetime import datetime, timedelta
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

sys.path.append('../')
from moodyutils import MoodyUtils


class MoodyBillsRawdata():

    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.environ['moodydb_conn_server'], 
            port=os.environ['moodydb_conn_port'], 
            database=os.environ['moodydb_conn_db'], 
            user=os.environ['moodydb_conn_user'], 
            password=os.environ['moodydb_conn_pw']
            )
        print(f'successful moodydb connection.')
        today = datetime.now().strftime("%Y%m%d")
        moodyutils = MoodyUtils()
        self.accounts_data = moodyutils.s3_download(f'moodybills/accounts/accounts_{today}.json')
        self.transactions_data = moodyutils.s3_download(f'moodybills/transactions/transactions_{today}.json')
        print(f'successful moodydb connection.')

    def create_accounts_rawdata(self,):
        cur = self.conn.cursor()
        accounts_create_sql = """
            CREATE TABLE IF NOT EXISTS rawdata.moodybills_accounts_rawdata (
                account_id TEXT NOT NULL,
                mask TEXT NOT NULL,
                available_balance TEXT NOT NULL,
                current_balance TEXT NOT NULL,
                iso_currency_code TEXT NOT NULL,
                balance_limit TEXT NOT NULL,
                official_name TEXT NOT NULL,
                subtype TEXT NOT NULL,
                account_type TEXT NOT NULL
            );
            """
        cur.execute(accounts_create_sql)
        for item in self.accounts_data:
            columns = list(item.keys())
            values = list(item.values())
            insert_columns = str(columns).replace("[", "").replace("]", "").replace("'", "")
            insert_values = str(values).replace("[", "").replace("]", "").replace('"', "'").replace('None', "'None'")
            insert_sql = f"""INSERT INTO rawdata.moodybills_accounts_rawdata ({insert_columns}) VALUES({insert_values});"""
            # print(f'---- insert_sql: {insert_sql}')
            cur.execute(insert_sql)
        self.conn.commit()
        log.info(f'inserted values into rawdata.moodybills_accounts_rawdata')

        cur = self.conn.cursor()
        transactions_create_sql = """
            CREATE TABLE IF NOT EXISTS rawdata.moodybills_transactions_rawdata (
                transaction_id TEXT NOT NULL,
                transaction_name TEXT NOT NULL,
                account_id TEXT NOT NULL,
                amount TEXT NOT NULL,
                iso_currency_code TEXT NOT NULL,
                category_id TEXT NOT NULL,
                transaction_date TEXT NOT NULL,
                authorized_date TEXT NOT NULL,
                address TEXT NOT NULL,
                city TEXT NOT NULL,
                country TEXT NOT NULL,
                lat TEXT NOT NULL,
                lon TEXT NOT NULL,
                postal_code TEXT NOT NULL,
                region TEXT NOT NULL,
                store_number TEXT NOT NULL,
                payee TEXT NOT NULL,
                payer TEXT NOT NULL,
                payment_method TEXT NOT NULL,
                payment_processor TEXT NOT NULL,
                ppd_id TEXT NOT NULL,
                reason TEXT NOT NULL,
                reference_number TEXT NOT NULL,
                payment_channel TEXT NOT NULL,
                pending TEXT NOT NULL,
                pending_transaction_id TEXT NOT NULL,
                account_owner TEXT NOT NULL,
                transaction_code TEXT NOT NULL,
                transaction_type TEXT NOT NULL
            );
            """
        cur.execute(transactions_create_sql)
        for item in self.transactions_data:
            print(self.transactions_data[0])
            columns = list(item.keys())
            values = list(item.values())
            insert_columns = str(columns).replace("[", "").replace("]", "").replace("'", "")
            insert_values = str(values).replace("[", "").replace("]", "").replace('None', "'None'")
            insert_sql = f"""INSERT INTO rawdata.moodybills_transactions_rawdata ({insert_columns}) VALUES({insert_values});"""
            # print(f'---- insert_sql: {insert_sql}')
            cur.execute(insert_sql)
        conn.commit()
        log.info(f'inserted values into rawdata.moodybills_transactions_rawdata')

if __name__ == '__main__':
    MoodyBillsRawdata().create_accounts_rawdata()