import os
import sys
import psycopg2
from datetime import datetime, timedelta
sys.path.append('../')
from moodyutils import MoodyUtils
import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


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
        self.moodyutils = MoodyUtils()
        print(f'successful moodydb connection.')
        self.schema = 'rawdata'
        self.today = datetime.now().strftime("%Y%m%d")

    def create_accounts_rawdata(self):
        accounts_data = self.moodyutils.s3_download(f'moodybills/accounts/accounts_{self.today}.json')
        accounts_rawdata_table = 'moodybills_accounts_rawdata'
        cur = self.conn.cursor()
        accounts_create_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{accounts_rawdata_table} (
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
        for item in accounts_data:
            insert_sql = self.moodyutils.insert_data(self.schema, accounts_rawdata_table, item)
            cur.execute(insert_sql)

        self.conn.commit()
        log.info(f'inserted values into {self.schema}.{accounts_rawdata_table}')

    def create_transactions_rawdata(self):
        transactions_data = self.moodyutils.s3_download(f'moodybills/transactions/transactions_{self.today}.json')
        transactions_rawdata_table = 'moodybills_transactions_rawdata'
        cur = self.conn.cursor()
        transactions_create_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{transactions_rawdata_table} (
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
        for item in transactions_data:
            insert_sql = self.moodyutils.insert_data(self.schema, transactions_rawdata_table, item)    
            cur.execute(insert_sql)
        self.conn.commit()
        log.info(f'inserted values into {self.schema}.{transactions_rawdata_table}')
