import os
import zipfile
import shutil

import boto3
import botocore

from tqdm import tqdm


def create_s3_client():
    return boto3.client(
        "s3",
        config=botocore.config.Config(signature_version=botocore.UNSIGNED),
    )


def recreate_directory(path):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def ensure_parent_dir(path):
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def download_bucket_objects(s3, bucket_name, local_directory):
    os.makedirs(local_directory, exist_ok=True)

    continuation_token = None

    while True:
        params = {"Bucket": bucket_name}
        if continuation_token is not None:
            params["ContinuationToken"] = continuation_token

        response = s3.list_objects_v2(**params)
        contents = response.get("Contents", [])

        for obj in tqdm(contents):
            file_name = obj["Key"]
            local_file_path = os.path.join(local_directory, file_name)
            ensure_parent_dir(local_file_path)
            s3.download_file(bucket_name, file_name, local_file_path)

        if not response.get("IsTruncated"):
            break

        continuation_token = response.get("NextContinuationToken")


def extract_archives(source_directory, destination_directory):
    recreate_directory(destination_directory)

    for obj_name in tqdm(os.listdir(source_directory)):
        if not obj_name.endswith(".zip"):
            continue

        zip_path = os.path.join(source_directory, obj_name)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(destination_directory)


def organize_files(root):
    stations_dir = os.path.join(root, "stations")
    trips_dir = os.path.join(root, "trips")

    os.makedirs(stations_dir, exist_ok=True)
    os.makedirs(trips_dir, exist_ok=True)

    for name in os.listdir(root):
        path = os.path.join(root, name)

        if name in {"stations", "trips"}:
            continue

        if not os.path.isfile(path):
            continue

        if name.startswith("Divvy_Stations_") and (name.endswith(".csv") or name.endswith(".xlsx")):
            shutil.move(path, os.path.join(stations_dir, name))
        elif name.endswith(".csv") and (
            name.startswith("Divvy_Trips_")
            or "divvy-tripdata" in name
            or "divvy-publictripdata" in name
        ):
            shutil.move(path, os.path.join(trips_dir, name))


def filter_trip_files_by_date(path):
    for name in os.listdir(path):
        if not name.endswith(".csv"):
            continue

        try:
            yyyymm = int(name[:6])
        except Exception:
            continue

        if yyyymm >= 202501:
            file_path = os.path.join(path, name)
            os.remove(file_path)


s3 = create_s3_client()
bucket_name = "divvy-tripdata"
local_directory = "./raw_data"
destination_directory = "./_data"

download_bucket_objects(s3, bucket_name, local_directory)
extract_archives(local_directory, destination_directory)
organize_files(destination_directory)
filter_trip_files_by_date(os.path.join(destination_directory, "trips"))