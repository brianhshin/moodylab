import os
import sys
import psycopg2
from datetime import datetime, timedelta
sys.path.append('../')
from moodyutils import MoodyUtils
import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class MoodyBillsProd():

    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.environ['moodydb_conn_server'], 
            port=os.environ['moodydb_conn_port'], 
            database=os.environ['moodydb_conn_db'], 
            user=os.environ['moodydb_conn_user'], 
            password=os.environ['moodydb_conn_pw']
            )
        self.schema = 'moodybills'
        self.today = datetime.now().strftime("%Y%m%d")

    def create_accounts_dim(self):
        accounts_dim_table = f'accounts_dim'
        cur = self.conn.cursor()
        accounts_dim_sql = f"""
            CREATE SCHEMA IF NOT EXISTS {self.schema};

            -- DROP TABLE IF EXISTS {self.schema}.{accounts_dim_table};

            CREATE TABLE IF NOT EXISTS {self.schema}.{accounts_dim_table} (
                account_id TEXT NOT NULL,
                balance_date DATE NOT NULL,
                mask TEXT NOT NULL,
                account_owner TEXT,
                available_balance FLOAT,
                current_balance FLOAT,
                iso_currency_code TEXT,
                balance_limit TEXT,
                official_name TEXT,
                subtype TEXT,
                account_type TEXT,
                last_modified_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                created_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (account_id, balance_date)
            );

            INSERT INTO {self.schema}.{accounts_dim_table}
                SELECT
                    account_id,
                    balance_date,
                    mask,
                    account_owner,
                    available_balance,
                    current_balance,
                    iso_currency_code,
                    balance_limit,
                    official_name,
                    subtype,
                    account_type,
                    CURRENT_TIMESTAMP
                FROM staging.accounts_staging
                WHERE TRUE
                ORDER BY balance_date ASC
            ON CONFLICT(account_id, balance_date) DO UPDATE SET
                mask = excluded.mask,
                account_owner = excluded.account_owner,
                available_balance = excluded.available_balance,
                current_balance = excluded.current_balance,
                iso_currency_code = excluded.iso_currency_code,
                balance_limit = excluded.balance_limit,
                official_name = excluded.official_name,
                subtype = excluded.subtype,
                account_type = excluded.account_type,
                last_modified_timestamp = CURRENT_TIMESTAMP
            ;
            """
        cur.execute(accounts_dim_sql)
        self.conn.commit()
        log.info(f'created {self.schema}.{accounts_dim_table}')

    def create_transactions_dim(self):
        transactions_dim_table = f'transactions_dim'
        cur = self.conn.cursor()
        transactions_dim_sql = f"""
            -- DROP TABLE IF EXISTS {self.schema}.{transactions_dim_table};

            CREATE TABLE IF NOT EXISTS {self.schema}.{transactions_dim_table} (
                transaction_id TEXT PRIMARY KEY,
                transaction_name TEXT NOT NULL,
                account_id TEXT,
                amount FLOAT,
                iso_currency_code TEXT,
                category_id INT,
                transaction_date DATE,
                authorized_date DATE,
                address TEXT,
                city TEXT,
                lat FLOAT,
                lon FLOAT,
                postal_code TEXT,
                region TEXT,
                store_number INT,
                payment_processor TEXT,
                payment_channel TEXT,
                transaction_type TEXT,
                last_modified_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                created_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );

            INSERT INTO {self.schema}.{transactions_dim_table}
                SELECT
                    transaction_id,
                    transaction_name,
                    account_id,
                    amount,
                    iso_currency_code,
                    category_id,
                    transaction_date,
                    authorized_date,
                    address,
                    city,
                    lat,
                    lon,
                    postal_code,
                    region,
                    store_number,
                    payment_processor,
                    payment_channel,
                    transaction_type,
                    CURRENT_TIMESTAMP
                FROM staging.transactions_staging
            ON CONFLICT(transaction_id) DO UPDATE SET
                transaction_name = excluded.transaction_name,
                account_id = excluded.account_id,
                amount = excluded.amount,
                iso_currency_code = excluded.iso_currency_code,
                category_id = excluded.category_id,
                transaction_date = excluded.transaction_date,
                authorized_date = excluded.authorized_date,
                address = excluded.address,
                city = excluded.city,
                lat = excluded.lat,
                lon = excluded.lon,
                postal_code = excluded.postal_code,
                region = excluded.region,
                store_number = excluded.store_number,
                payment_processor = excluded.payment_processor,
                payment_channel = excluded.payment_channel,
                transaction_type = excluded.transaction_type,
                last_modified_timestamp = CURRENT_TIMESTAMP
            ;
            """
        cur.execute(transactions_dim_sql)
        self.conn.commit()
        self.conn.close()
        log.info(f'inserted values into {self.schema}.{transactions_dim_table}')
