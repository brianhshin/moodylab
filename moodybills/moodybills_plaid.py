import os
import json
import boto3
import plaid
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.api import plaid_api
from datetime import datetime, timedelta


class MoodyBills():

    def __init__(self):
        # plaid creds
        client_id = os.environ['plaid_client_id']
        secret = os.environ['plaid_secret']
        configuration = plaid.Configuration(
            host=plaid.Environment.Development,
            api_key={
                'clientId': client_id,
                'secret': secret,
                }
            )
        api_client = plaid.ApiClient(configuration)
        self.client = plaid_api.PlaidApi(api_client)
        print(f'successful plaid api connection.')

    def get_moodybills(self, line, count, start_dt, end_dt):
        request = TransactionsGetRequest(
            access_token=line[1],
            start_date=datetime.strptime(start_dt, '%Y-%m-%d').date(),
            end_date=datetime.strptime(end_dt, '%Y-%m-%d').date(),
            options=TransactionsGetRequestOptions(count=count)
            )
        response = self.client.transactions_get(request)
        print(f'{line[0]} total_transactions: {response["total_transactions"]}')
        accounts = []
        transactions = []
        for account in response['accounts']:
            account_dict = {
                'account_id': account['account_id'],
                'mask': account['mask'],
                'available_balance': account['balances']['available'],
                'current_balance': account['balances']['current']
                }
            accounts.append(account_dict)
        for transaction in response['transactions']:
            transactions_dict = {
                'transaction_id': transaction['transaction_id'],
                'transaction_name': transaction['name'],
                'amount': transaction['amount'],
                'authorized_date': transaction['authorized_date'],
                'date': transaction['date'],
                'category': transaction['category'],
                'account_id': transaction['account_id']
                }
            transactions.append(transactions_dict)
        print(f'{line[0]} number of accounts: {len(accounts)}')
        print(f'{line[0]} number of transactions: {len(transactions)}')
        print(accounts)
        print(transactions)
        return account, transactions
