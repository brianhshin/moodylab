import os
import sys
import psycopg2
from datetime import datetime, timedelta
sys.path.append('../')
from moodyutils import MoodyUtils
import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class MoodyBillsStaging():

    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.environ['moodydb_conn_server'], 
            port=os.environ['moodydb_conn_port'], 
            database=os.environ['moodydb_conn_db'], 
            user=os.environ['moodydb_conn_user'], 
            password=os.environ['moodydb_conn_pw']
            )
        print(f'successful moodydb connection.')
        self.schema = 'staging'
        self.today = datetime.now().strftime("%Y%m%d")

    def create_accounts_staging(self):
        accounts_staging_table = f'moodybills_accounts_{self.schema}'
        cur = self.conn.cursor()
        accounts_staging_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{accounts_staging_table} (
         	SELECT
         		CAST(account_id AS VARCHAR) AS account_id,
         		DATE(balance_date) AS balance_date,
         		CAST(mask AS INT) AS mask,
   		        CAST(available_balance AS FLOAT) AS available_balance,
         		CAST(current_balance AS FLOAT) AS current_balance,
         		CAST(iso_currency_code AS VARCHAR) AS iso_currency_code,
         		CASE
         			WHEN balance_limit = 'None'
         				THEN NULL
         			ELSE CAST(balance_limit AS FLOAT)
         		END balance_limit,
         		CASE
         			WHEN official_name = 'None'
         				THEN NULL
         			ELSE CAST(official_name AS VARCHAR)
         		END official_name,
         		CAST(subtype AS VARCHAR) AS subtype,
         		CAST(account_type AS VARCHAR) AS account_type
         	FROM rawdata.moodybills_accounts_rawdata
         ;
            """
        cur.execute(accounts_staging_sql)
        self.conn.commit()
        log.info(f'created {self.schema}.{accounts_staging_table}')

    def create_transactions_staging(self):
        transactions_staging_table = f'moodybills_transactions_{self.schema}'
        cur = self.conn.cursor()
        transactions_create_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{transactions_staging_table} (
                SELECT
                    CAST(transaction_id AS VARCHAR) AS transaction_id,
                    CAST(transaction_name AS VARCHAR) AS transaction_name,
                    CAST(account_id AS VARCHAR) AS account_id,
                    CAST(amount AS FLOAT) AS amount,
                    CAST(iso_currency_code AS VARCHAR) AS iso_currency_code,
                    CAST(category_id AS INT) AS category_id,
                    DATE(transaction_date) AS transaction_date,
                    CASE
                        WHEN authorized_date = 'None'
                            THEN NULL
                        ELSE DATE(authorized_date)
                    END authorized_date,
                    CASE
                        WHEN address = 'None'
                            THEN NULL
                        ELSE CAST(address AS VARCHAR)
                    END address,
                    CASE
                        WHEN city = 'None'
                            THEN NULL
                        ELSE CAST(city AS VARCHAR)
                    END city,
                    CASE
                        WHEN lat = 'None'
                            THEN NULL
                        ELSE CAST(lat AS FLOAT)
                    END lat,
                    CASE
                        WHEN lon = 'None'
                            THEN NULL
                        ELSE CAST(lon AS FLOAT)
                    END lon,
                    CASE
                        WHEN postal_code = 'None'
                            THEN NULL
                        ELSE CAST(postal_code AS INT)
                    END postal_code,
                    CASE
                        WHEN region = 'None'
                            THEN NULL
                        ELSE CAST(region AS VARCHAR)
                    END region,
                    CASE
                        WHEN store_number = 'None'
                            THEN NULL
                        ELSE CAST(store_number AS INT)
                    END store_number,
                    CASE
                        WHEN payment_processor = 'None'
                            THEN NULL
                        ELSE CAST(payment_processor AS VARCHAR)
                    END payment_processor,
                    CAST(payment_channel AS VARCHAR) AS payment_channel,
                    CAST(transaction_type AS VARCHAR) AS transaction_type
                FROM rawdata.moodybills_transactions_rawdata
            ;
            """
        cur.execute(transactions_create_sql)
        self.conn.commit()
        log.info(f'inserted values into {self.schema}.{transactions_staging_table}')
