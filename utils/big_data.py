import boto3
import json
import zipfile
import os

def write_json(flat_dict:dict,filepath:str) -> str:
    json_data = json.dumps(flat_dict)
    with open(filepath) as f:
        f.write(json_data)
    return filepath

def zipp_file(filepath:str):
    zippath = filepath.replace('json','zip')
    with zipfile.ZipFile(filepath, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write('data.json', os.path.basename('data.json'))

def push_to_s3(filepath:str,bucket:str):
    pass
