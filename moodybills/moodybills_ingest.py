import sys
import os
import math
import argparse
import json
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from moodybills_plaid import MoodyBills
from moodybills_rawdata import MoodyBillsRawdata
from moodybills_staging import MoodyBillsStaging
sys.path.append('../')
from moodyutils import MoodyUtils

def get_args():
    parser = argparse.ArgumentParser(description='moodybills scraper.')
    parser.add_argument('--is_backfill', default='N', help='backfill and scrape all records (Y/N)?')
    args = parser.parse_args()
    is_backfill = args.is_backfill.upper()
    return is_backfill

def ingest_moodybills(is_backfill):
    lines = [
        ('jme_chase', os.environ['jme_chase_access_token']), 
        ('bri_boa', os.environ['bri_boa_access_token'])
        ]
    moodybills = MoodyBills()
    moodyutils = MoodyUtils()
    accounts = []
    transactions = []
    today = datetime.now().strftime("%Y%m%d")
    for line in lines:
        line_transactions = []
        if is_backfill == 'Y':
            first_date = '2021-05-01'
            months_to_date = pd.date_range(first_date, today, freq='MS').strftime('%Y-%m-%d').tolist()
            print(f'months to date since 2021-05-01: {months_to_date}.')
            for month in months_to_date:
                start_date = month
                start_dt = datetime.strptime(month, '%Y-%m-%d').date()
                end_dt = start_dt + relativedelta(months=+1)
                end_date = end_dt.strftime("%Y-%m-%d")
                week_accounts, week_transactions = moodybills.get_moodybills(line, 500, start_date, end_date)
                line_accounts = week_accounts
                line_transactions += week_transactions
        else:
            start_dt = (datetime.now().date() + timedelta(-1)).strftime("%Y-%m-%d")
            end_dt = datetime.now().strftime("%Y-%m-%d")
            day_accounts, day_transactions = moodybills.get_moodybills(line, 30, start_dt, end_dt)
            line_accounts = day_accounts
            line_transactions += day_transactions
        accounts += line_accounts
        transactions += line_transactions

    accounts_filepath = f'moodybills/accounts/accounts_{today}.json'
    transactions_filepath = f'moodybills/transactions/transactions_{today}.json'
    accounts_response = moodyutils.s3_upload(accounts, accounts_filepath)
    transactions_response = moodyutils.s3_upload(transactions, transactions_filepath)

def load_rawdata():
    moodydb_rawdata = MoodyBillsRawdata()
    moodydb_rawdata.create_accounts_rawdata()
    moodydb_rawdata.create_transactions_rawdata()

def load_staging():
    moodydb_staging = MoodyBillsStaging()
    moodydb_staging.create_accounts_staging()
    moodydb_staging.create_transactions_staging()

# u know what it do
if __name__ == '__main__':
    # parse is_backfill from args
    is_backfill = get_args()
    print('is_backfill:', is_backfill)
    ingest_moodybills(is_backfill)
    load_rawdata()
    load_staging()


