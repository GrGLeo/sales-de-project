import boto3
import json
import zipfile
import os
from datetime import datetime

def get_session():
    access = os.getenv('AWS_ACCESS_ID')
    key = os.getenv('AWS_ACCESS_KEY')
    session = boto3.Session(aws_access_key_id=access, aws_secret_access_key=key)
    return session

def write_json(flat_dict:dict,filepath:str) -> str:
    json_data = json.dumps(flat_dict)
    with open(filepath, 'w') as f:
        f.write(json_data)
    return filepath

def zipp_file(filepath:str) -> str:
    zippath = filepath.replace('json','zip')
    with zipfile.ZipFile(zippath, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(filepath, os.path.basename(filepath))
    return zippath

def push_to_s3(flat_dict:dict ,filepath:str, bucket:str) -> None:
    # setup bucket connexion
    session = get_session()
    s3 = session.resource('s3')
    bucket = s3.Bucket('sales-raw-data-dexp')
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
        os.remove(filepath)
        os.remove(zippath)
    except Exception as e:
        print(e)
        os.remove(filepath)

def push_late_data():
    # TODO save not pushed raw_data
    # pushed all raw_data to bucket
    pass
