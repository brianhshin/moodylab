# moodylab
this is where my personal projects will be. tinkering in the lab while in champ select

## moodybills
moodybills is a personal finance postgres db. i registered with the plaid api and got connections to our different bank accounts.  [link to plaid safety/security policies](https://plaid.com/safety/)

- moodybills/moodybills_ingest.py  - kicks off the ETL, importing the other modules.

- moodybills/moodybills_rawdata.py  - creates rawdata tables and inserts data for accounts and transactions objects.

- moodybills/moodybills_staging.py - creats staging tables from rawdata tables
