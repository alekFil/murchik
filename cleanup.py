import os
from dotenv import load_dotenv
import boto3
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import argparse

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

def clear_bucket_multithreaded(max_workers=16):
    paginator = s3_client.get_paginator('list_objects_v2')
    to_delete = []
    for page in paginator.paginate(Bucket=STORAGE_BUCKET_NAME):
        for obj in page.get('Contents', []):
            to_delete.append(obj['Key'])
    if not to_delete:
        print("Бакет уже пуст.")
        return
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(s3_client.delete_object, Bucket=STORAGE_BUCKET_NAME, Key=key) for key in to_delete]
        with tqdm(total=len(futures), desc="Удаление файлов") as pbar:
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"[ERROR] {e}")
                pbar.update(1)
    print("Очистка завершена.")

def main():
    parser = argparse.ArgumentParser(description="Очистка бакета S3")
    parser.add_argument('--threads', type=int, default=16, help="Количество потоков для удаления (по умолчанию 16)")
    args = parser.parse_args()
    clear_bucket_multithreaded(max_workers=args.threads)

if __name__ == '__main__':
    main() 