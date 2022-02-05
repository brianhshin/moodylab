import sys
import os
import math
import argparse
import json
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from time import sleep
from moodybills_plaid import MoodyBills
from moodybills_rawdata import MoodyBillsRawdata
from moodybills_staging import MoodyBillsStaging
from moodybills_prod import MoodyBillsProd
sys.path.append('../')
from moodyutils import MoodyUtils
import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class MoodyBillsETL():

    def __init__(self):
        self.moodydb_rawdata = MoodyBillsRawdata()
        self.moodydb_staging = MoodyBillsStaging()
        self.moodydb_prod = MoodyBillsProd()
        self.moodybills = MoodyBills()
        self.moodyutils = MoodyUtils()

        parser = argparse.ArgumentParser(description='moodybills scraper.')
        parser.add_argument('--is_backfill', default='N', help='backfill and scrape all records (Y/N)?')
        args = parser.parse_args()
        self.is_backfill = args.is_backfill.upper()

        self.today = datetime.now().strftime("%Y%m%d")
        self.account_lines = [
            ('jme_chase', os.environ['jme_chase_access_token']), 
            ('bri_boa', os.environ['bri_boa_access_token'])
            ]

    def moodybills_backfill(self, line):
        line_transactions = []
        first_date = '2016-05-01'
        months_to_date = pd.date_range(first_date, self.today, freq='MS').strftime('%Y-%m-%d').tolist()
        log.info(f'months to date since 2021-05-01: {months_to_date}.')
        for month in months_to_date:
            start_date = month
            end_dt = datetime.strptime(month, '%Y-%m-%d').date() + relativedelta(months=+1) - relativedelta(days=1)
            end_date = end_dt.strftime("%Y-%m-%d")
            week_accounts, week_transactions = self.moodybills.get_moodybills(line, 500, start_date, end_date)
            line_accounts = week_accounts
            line_transactions += week_transactions
            sleep(5)
        return line_accounts, line_transactions

    def moodybills_current(self, line):
        line_transactions = []
        start_dt = (datetime.now().date() + timedelta(-1)).strftime("%Y-%m-%d")
        end_dt = datetime.now().strftime("%Y-%m-%d")
        day_accounts, day_transactions = self.moodybills.get_moodybills(line, 30, start_dt, end_dt)
        line_accounts = day_accounts
        line_transactions += day_transactions
        return line_accounts, line_transactions

    def ingest_moodybills(self):
        accounts = []
        transactions = []
        for line in self.account_lines:
            line_transactions = []
            if self.is_backfill == 'Y':
                line_accounts, line_transactions = self.moodybills_backfill(line)
            else:
                line_accounts, line_transactions = self.moodybills_current(line)
            accounts += line_accounts
            transactions += line_transactions

        accounts_filepath = f'moodybills/accounts/accounts_{self.today}.json'
        transactions_filepath = f'moodybills/transactions/transactions_{self.today}.json'
        accounts_response = self.moodyutils.s3_upload(accounts, accounts_filepath)
        transactions_response = self.moodyutils.s3_upload(transactions, transactions_filepath)

    def load_rawdata(self):
        self.moodydb_rawdata.create_accounts_rawdata()
        self.moodydb_rawdata.create_transactions_rawdata()

    def load_staging(self):
        self.moodydb_staging.create_accounts_staging()
        self.moodydb_staging.create_transactions_staging()
    def load_prod(self):
        self.moodydb_prod.create_accounts_dim()
        self.moodydb_prod.create_transactions_dim()

# # u know what it do
if __name__ == '__main__':
#     # # parse is_backfill from args
#     # is_backfill = get_args()
#     # print('is_backfill:', is_backfill)
#     # ingest_moodybills(is_backfill)

    MoodyBillsETL().ingest_moodybills()
    MoodyBillsETL().load_rawdata()
    MoodyBillsETL().load_staging()
    MoodyBillsETL().load_prod()


