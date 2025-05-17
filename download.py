import os
import zipfile
import shutil

import boto3
import botocore

from tqdm import tqdm


s3 = boto3.client(
    "s3",
    config=botocore.config.Config(signature_version=botocore.UNSIGNED),
)

bucket_name = 'divvy-tripdata'
local_directory = './raw_data'

os.makedirs(local_directory, exist_ok=True)

objects = s3.list_objects_v2(Bucket=bucket_name)

for obj in tqdm(objects.get('Contents', [])):
    file_name = obj['Key']
    local_file_path = os.path.join(local_directory, file_name)

    if not os.path.exists(os.path.dirname(local_file_path)):
        os.makedirs(os.path.dirname(local_file_path))

    s3.download_file(bucket_name, file_name, local_file_path)


destination_folder = './_data'

if os.path.exists(destination_folder):
    shutil.rmtree(destination_folder)

os.makedirs(destination_folder, exist_ok=True)

for obj_name in tqdm(os.listdir(local_directory)):

    if obj_name.endswith(".zip"):

        zip_path = os.path.join(local_directory, obj_name)
        
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(destination_folder)