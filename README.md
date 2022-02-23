# moodylab
this is where my personal projects will be. tinkering in the lab while in champ select

WIP:
- jenkins pipeline deployment on ec2
- dbt for transformations

## moodybills
moodybills is a personal finance postgres db. i registered with the plaid api and got connections to our different bank accounts.  [link to plaid safety/security policies](https://plaid.com/safety/)

- moodybills/moodybills_ingest.py  - kicks off the ETL, importing the other modules. uploads a json file to s3 for accounts and transactions by date (ex: accounts_20210113.json).

- moodybills/moodybills_rawdata.py  - creates rawdata tables, downloads json from s3 and inserts data for accounts and transactions.

- moodybills/moodybills_staging.py - creates staging tables from rawdata tables.

- moodyutils.py - has common utility functions (upload/download from s3, etc.)
