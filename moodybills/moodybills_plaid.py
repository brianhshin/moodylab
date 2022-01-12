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
                'balance_date': datetime.today().strftime('%Y-%m-%d'),
                'mask': account['mask'],
                'available_balance': account['balances']['available'],
                'current_balance': account['balances']['current'],
                'iso_currency_code': account['balances']['iso_currency_code'],
                'balance_limit': account['balances']['limit'],
                'official_name': account['official_name'],
                'subtype': account['subtype'],
                'account_type': account['type']
                }
            accounts.append(account_dict)
        for transaction in response['transactions']:
            transactions_dict = {
                'transaction_id': transaction['transaction_id'],
                'transaction_name': transaction['name'],
                'account_id': transaction['account_id'],
                'amount': transaction['amount'],
                'iso_currency_code': transaction['iso_currency_code'],
                'category_id': transaction['category_id'],
                'transaction_date': transaction['date'],
                'authorized_date': transaction['authorized_date'],
                'address': transaction['location']['address'],
                'city': transaction['location']['city'],
                'country': transaction['location']['country'],
                'lat': transaction['location']['lat'],
                'lon': transaction['location']['lon'],
                'postal_code': transaction['location']['postal_code'],
                'region': transaction['location']['region'],
                'store_number': transaction['location']['store_number'],
                'payee': transaction['payment_meta']['payee'],
                'payer': transaction['payment_meta']['payer'],
                'payment_method': transaction['payment_meta']['payment_method'],
                'payment_processor': transaction['payment_meta']['payment_processor'],
                'ppd_id': transaction['payment_meta']['ppd_id'],
                'reason': transaction['payment_meta']['reason'],
                'reference_number': transaction['payment_meta']['reference_number'],
                'payment_channel': transaction['payment_channel'],
                'pending': transaction['pending'],
                'pending_transaction_id': transaction['pending_transaction_id'],
                'account_owner': transaction['account_owner'],
                'transaction_code': transaction['transaction_code'],
                'transaction_type': transaction['transaction_type']
                }
            transactions.append(transactions_dict)
        print(f'{line[0]} number of accounts: {len(accounts)}')
        print(f'{line[0]} number of transactions: {len(transactions)}')
        return accounts, transactions
