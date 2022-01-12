import os
import psycopg2
import boto3
import argparse
import logging
from datetime import datetime, timedelta
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

sys.path.append('../')
from moodyutils import MoodyUtils



def create_connection(drop=True):
  try:
    # connect to db specified in run_etl_sim
    conn = psycopg2.connect(os.environ['moodydb_conn_string'])
    cur = conn.cursor()

    s3 = boto3.resource('s3',
         aws_access_key_id=os.environ['aws_access_key_id'],
         aws_secret_access_key=os.environ['aws_secret_access_key'])

    if drop == True:
        # drop tables for each ETL run
        cur.execute('DROP TABLE IF EXISTS {}'.format('komiko_rawdata.bjjh_fighters_rawdata'))
        cur.execute('DROP TABLE IF EXISTS {}'.format('komiko_rawdata.bjjh_matches_rawdata'))

        log.info(f'dropping all tables.')
    else:
        log.info(f'did not drop all tables.')
  except Exception as e:
    print(e)
  return conn, s3

def get_s3_file():
    today = datetime.now().strftime("%Y%m%d")
    moodyutils = MoodyUtils()
    accounts_data = moodyutils.s3_download(f'moodybills/accounts/accounts_{today}.json')
    transaction = moodyutils.s3_download(f'moodybills/accounts/accounts_{today}.json')




def fighters_rawdata(conn, df):

    cur = conn.cursor()
    fighters_create_sql = """
        CREATE TABLE IF NOT EXISTS komiko_rawdata.bjjh_fighters_rawdata (
            fighter_id TEXT NOT NULL,
            fighter_fname TEXT NOT NULL,
            fighter_lname TEXT NOT NULL,
            fighter_nname TEXT NOT NULL,
            fighter_team TEXT NOT NULL
        );
        """

    cur.execute(fighters_create_sql)
    columns = str(list(df.columns)).replace("'","").replace("'","").replace("[", "").replace("]", "")

    for i in range(df.shape[0]):
        val_list = list(df.values[i])
        values = [value.replace("'", "''") if isinstance(value, str) else value for value in val_list]
        values = str(values).replace("[", "").replace("]", "").replace('"', "'")
        insert_sql = f"""INSERT INTO komiko_rawdata.bjjh_fighters_rawdata ({columns}) VALUES({values});"""
        cur.execute(insert_sql)

    conn.commit()
    log.info(f'inserted values into bjjh_fighters_rawdata.')


def matches_rawdata(conn, df):

    cur = conn.cursor()
    matches_create_sql = """
        CREATE TABLE IF NOT EXISTS komiko_rawdata.bjjh_matches_rawdata (
            fighter_id TEXT NOT NULL,
            opponent_id TEXT NOT NULL,
            result TEXT NOT NULL,
            method TEXT NOT NULL,
            event TEXT NOT NULL,
            weight TEXT NOT NULL,
            stage TEXT NOT NULL,
            year  TEXT NOT NULL
        );
        """

    cur.execute(matches_create_sql)
    columns = str(list(df.columns)).replace("'","").replace("'","").replace("[", "").replace("]", "")

    for i in range(df.shape[0]):
        val_list = list(df.values[i])
        values = [value.replace("'", "''") if isinstance(value, str) else value for value in val_list]
        values = str(values).replace("[", "").replace("]", "").replace('"', "'")
        insert_sql = f"""INSERT INTO komiko_rawdata.bjjh_matches_rawdata ({columns}) VALUES({values});
        """
        cur.execute(insert_sql)

    conn.commit()
    log.info(f'inserted values into bjjh_matches_rawdata.')


def drop_tables_arg():

    parser = argparse.ArgumentParser(description='this python script will run BJJH.RAWDATA tables.')
    parser.add_argument('--drop_tables', default='N', help='If Y, this drops all of the tables in BJJH.RAWDATA and then kicks off a run of the most recent load. If N, it appends.')
    args=parser.parse_args()
    drop_tables = args.drop_tables.upper()

    return drop_tables


def create_rawdata_tables(s3):

    items = []

    for key in s3.meta.client.list_objects_v2(Bucket='komiko-dev', Prefix='current/bjjh')['Contents']:
        items.append(key['Key'])

    for item in items:
        if item.split('.')[1] != 'csv':
            items.remove(item)

        elif 'fighters' in item:
            fighters = get_local_files(item, s3)
            fighters_rawdata(conn, fighters)
        elif 'matches' in item:
            matches = get_local_files(item, s3)
            matches_rawdata(conn, matches)

    log.info(f'updated komiko-dev.komiko.komiko_rawdata.bjjh tables. itspizzatime.jpeg.')

def get_args():

    parser = argparse.ArgumentParser(description='warzone_rawdata_local.py build rawdata tables for all CODWARZONE tables.')
    parser.add_argument('--drop_tables', default='N', help='drop tables, or not (Y or N). default is N.')
    args = parser.parse_args()
    drop_tables = args.drop_tables.upper()

    return drop_tables

if __name__ == '__main__':

    drop_tables = get_args()

    if drop_tables == 'Y':
        log.info(f'dropping tables.')
        drop_check = input('are you sure you want to drop all the tables in BJJH.RAWDATA (Y/N)?')
        if drop_check.upper() != 'Y':
            log.info(f'peace- cancelling script.')
            sys.exit()
        else:
            log.info(f'dropping tables in BJJH.RAWDATA.')
            conn, s3 = create_connection(drop=True)
    else:
        conn, s3 = create_connection(drop=False)

    create_rawdata_tables(s3)