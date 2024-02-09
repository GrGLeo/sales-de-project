import os
from datetime import datetime
import json
import zipfile
import boto3

class AwsInstance(boto3.Session):
    '''
    AWS Session instance with methods for interacting with S3.

    This class extends boto3.Session and provides methods for creating an S3 bucket,
    pushing data to S3, and handling late data pushing.
    '''
    def __init__(self):
        access = os.getenv('AWS_ACCESS_ID')
        key = os.getenv('AWS_ACCESS_KEY')
        self.bucket_name = os.getenv('AWS_BUCKET')
        super().__init__(aws_access_key_id=access, aws_secret_access_key=key)
        self.s3 = self.resource('s3')

    def create_bucket(self):
        'Creates an S3 bucket with a specified location constraint.'
        self.s3.create_bucket(
            Bucket=self.bucket_name,
            CreateBucketConfiguration={'LocationConstraint':'eu-west-3'}
            )

    def push_to_s3(self, flat_dict:dict ,filepath:str) -> None:
        'Push the zipped file to s3'
        bucket = self.s3.Bucket(self.bucket_name)
        # write zipp file
        zippath = zipp_file(write_json(flat_dict, filepath))
        # get name for s3
        uploaded_file = zippath.split('.')
        date = datetime.today().strftime('%Y-%m-%dT%H:%M')
        uploaded_file[0] = uploaded_file[0] + '_' + date
        uploaded_file = '.'.join(uploaded_file)
        # push zipped file to s3
        try:
            bucket.upload_file(zippath,uploaded_file)
            with open('s3_files.txt', 'a', encoding='UTF-8') as file:
                file.write(uploaded_file)
                file.write('\n')
            os.remove(filepath)
            os.remove(zippath)
        except Exception as e:
            print(e)
            os.remove(filepath)

    def push_late_data(self):
        '''
        Check if data were not pushed into a s3 bucket and saved locally
        Push every saved zipped file to s3
        '''
        # TODO save not pushed raw_data
        # pushed all raw_data to bucket
        return

    def delete_bucket(self):
        'Simple method to delete the bucket'
        # Doesn't work getting access denied
        bucket = self.s3.Bucket(self.bucket_name)
        with open('s3_files.txt', 'r', encoding='UTF-8') as file:
            objects = [
                {
                    'Key':line.strip()
                }
            for line in file]
        response = bucket.delete_objects(
            Delete={
                'Objects':objects,
            }
        )
        print(response)
        response = bucket.delete()
        print(response)
        os.remove('s3_files.txt')

def write_json(flat_dict:dict,filepath:str) -> str:
    'Write a json file from the flatten sale dict'
    json_data = json.dumps(flat_dict)
    with open(filepath, 'w', encoding='UTF-8') as f:
        f.write(json_data)
    return filepath

def zipp_file(filepath:str) -> str:
    'Zipp the data json file'
    zippath = filepath.replace('json','zip')
    with zipfile.ZipFile(zippath, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(filepath, os.path.basename(filepath))
    return zippath
