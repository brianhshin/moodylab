from datetime import datetime
from moodyutils import MoodyUtils
import yaml

def lgy_ingest():
    moodyutils = MoodyUtils()
    today = datetime.now().strftime("%Y%m%d")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open("./config/lgy_search_config.yaml", "r") as file:
        lgy_config_data = yaml.safe_load(file)

    for search_query in lgy_config_data:
        homes_search_name, homes_data = moodyutils.lgy_scrape(search_query)
        homes_df = moodyutils.lgy_process(homes_data, now)
        homes_filepath = f'moodyhome/raw/lgy/{homes_search_name}_{today}'
        print(f'-- {homes_search_name} --uploading file.')
        upload_response = moodyutils.s3_upload(homes_df.to_json(orient='records'), homes_filepath)

if __name__ == '__main__':
    lgy_ingest()
