import os
import json
import boto3
from datetime import datetime, timedelta

class MoodyUtils():
    def __init__(self):
        # aws creds
        access_key_id = os.environ['aws_access_key_id']
        secret_access_key = os.environ['aws_secret_access_key']
        self.s3_bucket = 'moodylab'

        self.session = boto3.Session(
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key
                )
        print(f'successful aws connection.')

    def s3_upload(self, content, filepath):
        s3 = self.session.resource('s3')
        content_json = json.dumps(content, default=str)
        today = datetime.now().strftime("%Y%m%d")
        response = s3.Object(self.s3_bucket, filepath).put(Body=content_json)
        print(f'successfully uploaded to s3://{self.s3_bucket}/{filepath}.')
        return response

    def s3_download(self, filepath):
        s3 = self.session.resource('s3')
        content = s3.Object(bucket_name=self.s3_bucket, key=filepath).get()['Body'].read()
        print(f'successfully downloaded from s3://{self.s3_bucket}/{filepath}.')
        return content