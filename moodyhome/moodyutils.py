from datetime import datetime, timedelta
from bs4 import BeautifulSoup as bs
from urllib.request import Request, urlopen
from json import loads, dumps
import os
import boto3
import pandas as pd
import json
import requests
import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

class MoodyUtils():

    def __init__(self):
        access_key_id = os.environ['aws_access_key_id']
        secret_access_key = os.environ['aws_secret_access_key']
        self.s3_bucket = 'moodylab-us-east-1'
        self.session = boto3.Session(
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key
                )

    def s3_upload(self, content, filepath):
        s3 = self.session.resource('s3')
        content_json = json.dumps(content, default=str) # for json
        # content_json = content # for csv
        today = datetime.now().strftime("%Y%m%d")
        response = s3.Object(self.s3_bucket, filepath).put(Body=content_json)
        log.info(f'successfully uploaded to s3://{self.s3_bucket}/{filepath}.')
        return response

    def s3_download(self, filepath):
        s3 = self.session.resource('s3')
        content = s3.Object(bucket_name=self.s3_bucket, key=filepath).get()['Body'].read()
        content_json = json.loads(content)
        log.info(f'successfully downloaded from s3://{self.s3_bucket}/{filepath}.')
        return content_json

    def lgy_scrape(self, search_query):
        hdr = {'User-Agent': 'Mozilla/5.0'}
        homes_search_name = search_query['name']
        city = search_query['data']['city'].lower().replace(' ', '%20')
        state = search_query['data']['state']
        homes_url = f"https://lgy.va.gov/lgyhub/api/condos/search?approved=false&city={city}&stateCode={state}"
        print(f'-- {homes_search_name} --extracting listings.')
        homes_req = Request(homes_url,headers=hdr)
        homes_html = urlopen(homes_req)
        homes_soup = bs(homes_html, 'lxml')
        homes_data = json.loads(homes_soup.get_text())
        print(f'-- {homes_search_name} --found {len(homes_data)} listings.')
        return homes_search_name, homes_data

    def lgy_process(self, homes_data, now):
        for home in homes_data:
            if home['reviewCompletedDate'] != None:
                home['reviewCompletedDate'] = datetime.fromtimestamp(home['reviewCompletedDate'] / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
            if home['approvalRequestRecievedDate'] != None:
                home['approvalRequestRecievedDate'] = datetime.fromtimestamp(home['approvalRequestRecievedDate'] / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
        homes_df = pd.DataFrame(homes_data)
        homes_df['source'] = 'lgy'
        homes_df['ingestion_timestamp'] = now
        return homes_df

    def zillow_scrape(self, search_query):
        homes_search_name = search_query['name']
        headers = {
            'authority': 'www.zillow.com',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9,ko;q=0.8',
            'cookie': 'x-amz-continuous-deployment-state=AYABeDrTMEGCbfbKE2XOuCivKcwAPgACAAFEAB1kM2Jsa2Q0azB3azlvai5jbG91ZGZyb250Lm5ldAABRwAVRzA3MjU1NjcyMVRZRFY4RDcyVlpWAAEAAkNEABpDb29raWUAAACAAAAADPcmDICFoQWzGEX5RQAwdo94wbaIEapW7vrplWaDthKNe1MgYmmP0RtkJq6NEpLwj30iUBfgbHDeQ8E8TosJAgAAAAAMAAQAAAAAAAAAAAAAAAAAAGR4Qob5bvPSiPgo6CY96kT%2F%2F%2F%2F%2FAAAAAQAAAAAAAAAAAAAAAQAAAAzOB9w5Eazjpw6BBqKqPf+odFxPHHUYXDTKqawZUYXDTKqawQ==; zgsession=1|066fde2d-4a7d-4b72-b290-fa415940e65f; _ga=GA1.2.906244060.1706123048; zg_anonymous_id=%2203837382-60fe-4cf3-a23d-43e88ddbee75%22; _gcl_au=1.1.329604461.1706123048; DoubleClickSession=true; pxcts=59e3e9da-baeb-11ee-87cd-b04a1948863e; _pxvid=59e3db94-baeb-11ee-87cd-0248fc55ea24; __pdst=b2f32a341d964b69804814744f478a19; _pin_unauth=dWlkPU4ySmpNbUUzTnpndE9EWmpPUzAwWkdJMExXSXhOak10TURKbU1qRTVZVFUxWTJNMQ; g_state={"i_l":0}; _derived_epik=dj0yJnU9M2g5QUlIc1d5Yjk4QVdDSXNLTVZ6SUlVSEYtXzFINEkmbj1WMEFYYm95elZzTzhnVnhvRThycGl3Jm09NCZ0PUFBQUFBR1c2bDFNJnJtPTQmcnQ9QUFBQUFHVzZsMU0mc3A9NA; _gid=GA1.2.233014837.1709392515; _pxff_cc=U2FtZVNpdGU9TGF4Ow==; _pxff_cfp=1; _pxff_bsco=1; _hp2_ses_props.1215457233=%7B%22ts%22%3A1709392515749%2C%22d%22%3A%22www.zillow.com%22%2C%22h%22%3A%22%2F%22%7D; _clck=kataeu%7C2%7Cfjq%7C0%7C1484; zguid=24|%241fa6622f-53db-4ee4-bb8b-8bdfc7b90bed; x-amz-continuous-deployment-state=AYABeDrTMEGCbfbKE2XOuCivKcwAPgACAAFEAB1kM2Jsa2Q0azB3azlvai5jbG91ZGZyb250Lm5ldAABRwAVRzA3MjU1NjcyMVRZRFY4RDcyVlpWAAEAAkNEABpDb29raWUAAACAAAAADPcmDICFoQWzGEX5RQAwdo94wbaIEapW7vrplWaDthKNe1MgYmmP0RtkJq6NEpLwj30iUBfgbHDeQ8E8TosJAgAAAAAMAAQAAAAAAAAAAAAAAAAAAGR4Qob5bvPSiPgo6CY96kT%2F%2F%2F%2F%2FAAAAAQAAAAAAAAAAAAAAAQAAAAzOB9w5Eazjpw6BBqKqPf+odFxPHHUYXDTKqawZUYXDTKqawQ==; zjs_anonymous_id=%221fa6622f-53db-4ee4-bb8b-8bdfc7b90bed%22; JSESSIONID=1DC22951C4CD2E743005CC0BE1F4E7B2; ZILLOW_SID=1|AAAAAVVbFRIBVVsVEvOgJyxDKyHTWBy%2BWkvYe%2F0oDsfkm6C4fmTVZNCvBk5BZg9V3S3BmufQKebSKv5hMfWYc6Qfc%2B9k0eYS5w; loginmemento=1|78aa2a781c9fd9de2608b89a1f293446023f35e718467dc596ec54ed9145bca9; userid=X|3|1186065537627ae%7C5%7C5QRDetkgszhwd792YdyUD3T6uz-xr_YUMJoCXYLEn7I%3D; ZILLOW_SSID=1|AAAAAVVbFRIBVVsVEhJg4nX8dZoKUhrxi%2FBMxXwK2ObBueguJhwWd7FhHjP9LZXpTCPTZoyBxhImvSrk4La9xO5Q38iVj0di6Q; zjs_user_id=%22X1-ZUrspxon0p88ax_77v72%22; _hp2_id.1215457233=%7B%22userId%22%3A%225942109857022743%22%2C%22pageviewId%22%3A%225139361838047646%22%2C%22sessionId%22%3A%225236114728742952%22%2C%22identity%22%3A%22X1-ZUrspxon0p88ax_77v72%22%2C%22trackerVersion%22%3A%224.0%22%2C%22identityField%22%3Anull%2C%22isIdentified%22%3A1%7D; _uetsid=ac3b8f60d8a711eea9efbbe1e7df59e0; _uetvid=d0752a70798e11ed810ee3a65ea17882; _clsk=x11baw%7C1709392561698%7C9%7C0%7Cx.clarity.ms%2Fcollect; search=6|1711984572240%7Cregion%3Dsouth-end-boston-ma%26rb%3DSouth-End%252C-Boston%252C-MA%26rect%3D42.34784%252C-71.05861%252C42.332471%252C-71.083192%26disp%3Dmap%26mdm%3Dauto%26listPriceActive%3D1%26fs%3D1%26fr%3D0%26mmm%3D1%26rs%3D0%26ah%3D0%09%09275429%09%7B%22isList%22%3Atrue%2C%22isMap%22%3Afalse%7D%09%09%09%09%09; AWSALB=WX0HBrgrNeQZV+Ldyw3zhdu/vV3kyNR7RU1ph9Awfw3L0lZO7gTmoyHK5xRxa4vVZoyqsgjd54NW7OVU303Gh77yVqoTV7EpI+IE4oCnhPdzH6cyXmypjvMvGcP0; AWSALBCORS=WX0HBrgrNeQZV+Ldyw3zhdu/vV3kyNR7RU1ph9Awfw3L0lZO7gTmoyHK5xRxa4vVZoyqsgjd54NW7OVU303Gh77yVqoTV7EpI+IE4oCnhPdzH6cyXmypjvMvGcP0; _px3=ed4dc4e1348b36891b727feff75162254ae98aa24a77c2b6eac3e3ca390a26cc:vtT6lXwLLWHB86JWomcUw2iPccZU7bSH9gaEnapOMYAZPylcfhg+513wDuCyIE+5k6UBTcquG9lK5SEbKlGtZg==:1000:staijbFZqBLHSOndXzTqMbn0z2h+V8e/JjtFnJqhLETA/1XulTgNoJzLSTNl2kgDfQjs6KqgKeLMcumVjwRU0ZhdJKf/bTTrvtZHOBocAQTfssJc3RP20j15Q/Ge0KalbCnwwuJ5PVPPMj8BKMWWs6Hko8GbWVwToO3ckVMgDsHNipcpQjlAEhLhojMdDeayiyWqSDsl30cKRxQgvHgNCHG3OkDAwnTJsoMoPojJQOs=',
            'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            'sec-ch-ua-mobile': '?0' ,
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        }
        url = f"https://www.zillow.com/{homes_search_name}/"
        response = requests.get(url, headers=headers)
        html_content = response.text
        soup = bs(html_content, 'html.parser')
        script_tag = soup.find('script', id='__NEXT_DATA__')
        json_string = script_tag.string
        data = json.loads(json_string)
        homes_data = data['props']['pageProps']['searchPageState']['cat1']['searchResults']['listResults']
        return homes_search_name, homes_data

    def zillow_process(self, homes_data, now):
        homes_df = pd.DataFrame(homes_data)
        homes_df['source'] = 'zillow'
        homes_df['ingestion_timestamp'] = now
        return homes_df
