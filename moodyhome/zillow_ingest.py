from datetime import datetime
from moodyutils import MoodyUtils
import yaml

def zillow_ingest():

    moodyutils = MoodyUtils()
    today = datetime.now().strftime("%Y%m%d")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open("./config/zillow_search_config.yaml", "r") as file:
        zillow_config_data = yaml.safe_load(file)

    for search_query in zillow_config_data:
        homes_search_name, homes_data = moodyutils.zillow_scrape(search_query)
        homes_df = moodyutils.zillow_process(homes_data, now)
        homes_filepath = f"moodyhome/raw/zillow/{homes_search_name.replace('-', '_')}_{today}"
        print(f'-- {homes_search_name} --uploading file.')
        # upload_response = moodyutils.s3_upload(homes_df.to_json(orient='records'), homes_filepath)
        homes_df.to_csv(f"~/brian/projects/moodylab/moodyhome/moodyhome_files/{homes_search_name.replace('/', '_')}.csv", index=False, encoding='utf-8')


if __name__ == '__main__':
    zillow_ingest()
