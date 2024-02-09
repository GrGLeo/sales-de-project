import os
from datetime import datetime
import json
import zipfile
import boto3

def get_session():
    '''
    Get the session to AWS
    '''
    access = os.getenv('AWS_ACCESS_ID')
    key = os.getenv('AWS_ACCESS_KEY')
    session = boto3.Session(aws_access_key_id=access, aws_secret_access_key=key)
    return session

def write_json(flat_dict:dict,filepath:str) -> str:
    '''
    Write a json file from the flatten sale dict
    '''
    json_data = json.dumps(flat_dict)
    with open(filepath, 'w', encoding='UTF-8') as f:
        f.write(json_data)
    return filepath

def zipp_file(filepath:str) -> str:
    '''
    Zipp the data json file
    '''
    zippath = filepath.replace('json','zip')
    with zipfile.ZipFile(zippath, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(filepath, os.path.basename(filepath))
    return zippath

def push_to_s3(flat_dict:dict ,filepath:str, bucket:str) -> None:
    '''
    Push the zipped file to s3
    '''
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
    '''
    Check if data were not pushed into a s3 bucket and saved locally
    Push every zipped file to s3
    '''
    # TODO save not pushed raw_data
    # pushed all raw_data to bucket
    return
