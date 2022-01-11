# checking account
response['accounts'][0]['account_id']  # account id hash - STRING
response['accounts'][0]['mask'] # account 4 digit identifer (last 4 of account for checking, card for credit) - STRING
response['accounts'][0]['balances']['available'] # current balance including pending charges - FLOAT
response['accounts'][0]['balances']['current'] # current balance excluding pending charges - FLOAT

# credit account
response['accounts'][1]['account_id']

# transactions
response['total_transactions'] # total transactions in given time period - INT
response['transactions'][0] # transaction 1
response['transactions'][0]['account_id'] # account_id used for transaction 1 - STRING
response['transactions'][0]['amount'] # amount charged for transaction 1 - FLOAT
response['transactions'][0]['authorized_date'] # authroized_date of transaction 1 (maybe diff from date for pending transactions) - DATETIME
response['transactions'][0]['date'] # date of transaction 1 - DATETIME
response['transactions'][0]['category'] # category for transaction 1 (ex: ['Transfer', 'Third Party', 'Venmo']) - LIST
response['transactions'][0]['transaction_id'] # transaction_id for transaction 1 - STRING




{
    'accounts': [
        {
            'account_id': 'Ya7jVD83wwF30M85ML9MIry0d4rZkAFQwr3Bp',
            'balances': {
                'available': 1149.02,
                'current': 1449.02,
                'iso_currency_code': 'USD',
                'limit': None,
                'unofficial_currency_code': None
                },
            'mask': '0069',
            'name': 'Adv Plus Banking',
            'official_name': 'Adv Plus Banking',
            'subtype': 'checking',
            'type': 'depository'
        },
        {
            'account_id': 'zQdXeD47nnF3ZYvbYLOYI9qjK59k3OUOd439K',
            'balances': {
                'available': 2201.73,
                'current': 5333.26,
                'iso_currency_code': 'USD',
                'limit': 10000.0,
                'unofficial_currency_code': None
                },
            'mask': '3294',
            'name': 'Travel Rewards Visa Signature',
            'official_name': 'Travel Rewards Visa Signature',
            'subtype': 'credit card',
            'type': 'credit'
        }
    ],
    'item': {
        'available_products': ['balance', 'liabilities'],
        'billed_products': ['auth', 'transactions'],
        'consent_expiration_time': None,
        'error': None,
        'institution_id': 'ins_127989',
        'item_id': 'rYpPe9y6NNfn3mprmKEmUqzYyDyBYAIBbDoVB',
        'products': ['auth', 'transactions'],
        'update_type': 'background',
        'webhook': ''
        },
    'request_id': 'xY0wKEjapQ1YN4L',
    'total_transactions': 1,
    'transactions': [
        {
            'account_id': 'Ya7jVD83wwF30M85ML9MIry0d4rZkAFQwr3Bp',
            'account_owner': None,
            'amount': 300.0,
            'authorized_date': datetime.date(2021, 12, 29),
            'authorized_datetime': None,
            'category': ['Transfer', 'Third Party', 'Venmo'],
            'category_id': '21010001',
            'check_number': None,
            'date': datetime.date(2021, 12, 29),
            'datetime': None,
            'iso_currency_code': 'USD',
            'location': {
                'address': None,
                'city': None,
                'country': None,
                'lat': None,
                'lon': None,
                'postal_code': None,
                'region': None,
                'store_number': None
                },
            'merchant_name': None,
            'name': 'Venmo',
            'payment_channel': 'other',
            'payment_meta': {
                'by_order_of': None,
                'payee': None,
                'payer': None,
                'payment_method': 'ACH',
                'payment_processor': None,
                'ppd_id': None,
                'reason': None,
                'reference_number': None
                },
            'pending': True,
            'pending_transaction_id': None,
            'personal_finance_category': None,
            'transaction_code': None,
            'transaction_id': 'JDRrV8qNKKhxgOM3O56OHqRpVYzqXeIbQ8jOJ',
            'transaction_type': 'special',
            'unofficial_currency_code': None
        }
    ]
}