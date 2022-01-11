import plaid
import boto3
import json
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.api import plaid_api
from datetime import datetime

client_id = '61cc8121dab78b001ae38a9b'
secret = '7b976076a39a07250223254dc96aa4'
start_dt = '2021-07-01'
end_dt = '2021-12-31'

configuration = plaid.Configuration(
    host=plaid.Environment.Development,
    api_key={
        'clientId': client_id,
        'secret': secret,
        }
    )

api_client = plaid.ApiClient(configuration)
client = plaid_api.PlaidApi(api_client)


# jamie_chase_access_token = 'access-development-3db7ef64-406c-4f31-a271-56292f0127c5'
# brian_boa_access_token = 'access-development-ce800b64-3f45-4ed7-8e1c-33919e76104c'
# moodylab aws access keys
# access_key_id = 'AKIA5636VTX5SO2PSRCE'
# secret_access_key = '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'


access_token = 'access-development-3db7ef64-406c-4f31-a271-56292f0127c5'
request = TransactionsGetRequest(
    access_token=access_token,
    start_date=datetime.strptime(start_dt, '%Y-%m-%d').date(),
    end_date=datetime.strptime(end_dt, '%Y-%m-%d').date(),
    options=TransactionsGetRequestOptions(count=500)
    )
response = client.transactions_get(request)
print(f'total_transactions: {response["total_transactions"]}')

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

print(f'number of accounts: {len(accounts)}')
print(f'number of transactions: {len(transactions)}')

access_key_id = 'AKIA5636VTX5SO2PSRCE'
secret_access_key = '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'

s3_bucket = 'moodylab'
session = boto3.Session(
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key
    )

s3 = session.resource('s3')
accounts_json = json.dumps(accounts)
transactions_json = json.dumps(transactions, default=str)

today = datetime.now().strftime("%Y%m%d")

accounts_response = s3.Object(s3_bucket, f'moodybills/accounts/accounts_{today}.json').put(Body=accounts_json) #Metadata=metadata
transactions_response = s3.Object(s3_bucket, f'moodybills/transactions/transactions_{today}.json').put(Body=transactions_json) #Metadata=metadata

print(f'account s3 upload response: {accounts_response}')
print(f'transactions s3 upload response: {transactions_response}')


# # the transactions in the response are paginated, so make multiple calls while increasing the offset to
# # retrieve all transactions
# while len(transactions) < response['total_transactions']:
#     options = TransactionsGetRequestOptions()
#     options.offset = len(transactions)

#     request = TransactionsGetRequest(
#         access_token=access_token,
#         start_date=datetime.strptime('2020-01-01', '%Y-%m-%d').date(),
#         end_date=datetime.strptime('2021-01-01', '%Y-%m-%d').date(),
#         options=options
#     )
#     response = client.transactions_get(request)
