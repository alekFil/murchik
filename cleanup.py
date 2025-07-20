import os
from dotenv import load_dotenv
import boto3
from botocore.config import Config

load_dotenv()

STORAGE_ENDPOINT = os.getenv("STORAGE_ENDPOINT")
STORAGE_ACCESS_KEY = os.getenv("STORAGE_ACCESS_KEY")
STORAGE_SECRET_KEY = os.getenv("STORAGE_SECRET_KEY")
STORAGE_BUCKET_NAME = os.getenv("STORAGE_BUCKET_NAME")
STORAGE_REGION = os.getenv("STORAGE_REGION")

s3_client = boto3.client(
    "s3",
    endpoint_url=STORAGE_ENDPOINT,
    aws_access_key_id=STORAGE_ACCESS_KEY,
    aws_secret_access_key=STORAGE_SECRET_KEY,
    region_name=STORAGE_REGION,
    config=Config(s3={"addressing_style": "virtual"}),
)

def clear_bucket():
    paginator = s3_client.get_paginator('list_objects_v2')
    to_delete = []
    for page in paginator.paginate(Bucket=STORAGE_BUCKET_NAME):
        for obj in page.get('Contents', []):
            to_delete.append({'Key': obj['Key']})
    if not to_delete:
        print("Бакет уже пуст.")
        return
    # Удаляем по одному объекту (обход проблемы с Content-MD5)
    for obj in to_delete:
        s3_client.delete_object(Bucket=STORAGE_BUCKET_NAME, Key=obj['Key'])
        print(f"Удалён: {obj['Key']}")
    print("Очистка завершена.")

if __name__ == '__main__':
    clear_bucket() 