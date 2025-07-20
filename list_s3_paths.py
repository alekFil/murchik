import os
import base64
from dotenv import load_dotenv
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import argparse
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

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

def get_path_for_key(key, filters=None):
    try:
        head = s3_client.head_object(Bucket=STORAGE_BUCKET_NAME, Key=key)
        metadata = head.get('Metadata', {})
        encoded_path = metadata.get('original_path_b64')
        if encoded_path:
            original_path = base64.b64decode(encoded_path.encode()).decode()
            if not filters or any(f in original_path for f in filters):
                return original_path
    except Exception as e:
        print(f"[WARN] Не удалось получить путь для {key}: {e}")
    return None

def save_s3_paths_to_file(output_file, filters=None, max_workers=16):
    paginator = s3_client.get_paginator('list_objects_v2')
    all_keys = []
    for page in paginator.paginate(Bucket=STORAGE_BUCKET_NAME):
        for obj in page.get('Contents', []):
            all_keys.append(obj['Key'])
    paths = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_path_for_key, key, filters): key for key in all_keys}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Чтение метаданных"):
            result = future.result()
            if result:
                paths.append(result)
    with open(output_file, 'w', encoding='utf-8') as f:
        for path in paths:
            f.write(path + '\n')
    print(f"Сохранено {len(paths)} путей в файл {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Сохранить оригинальные пути файлов из S3 в файл")
    parser.add_argument('--output', required=True, help="Путь к выходному текстовому файлу")
    parser.add_argument('--filter', action='append', help="Фильтр по подстроке в пути (можно указывать несколько раз)")
    parser.add_argument('--threads', type=int, default=16, help="Количество потоков для head_object (по умолчанию 16)")
    args = parser.parse_args()
    save_s3_paths_to_file(args.output, filters=args.filter, max_workers=args.threads)

if __name__ == '__main__':
    main() 