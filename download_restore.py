import os
import base64
from dotenv import load_dotenv
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import argparse
from tqdm import tqdm

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

def download_files_to_directories(destination_root, quiet=False, show_count=False):
    # Получаем список всех объектов
    paginator = s3_client.get_paginator('list_objects_v2')
    all_keys = []
    total_size = 0
    for page in paginator.paginate(Bucket=STORAGE_BUCKET_NAME):
        for obj in page.get('Contents', []):
            all_keys.append(obj['Key'])
            total_size += obj.get('Size', 0)
    if show_count:
        print(f"Доступно файлов для скачивания: {len(all_keys)}")
        print(f"Общий объём: {total_size} байт ({total_size/1024/1024:.2f} МБ)")
        return
    with tqdm(total=total_size, unit='B', unit_scale=True, desc="Скачивание данных") as pbar:
        for key in all_keys:
            try:
                head = s3_client.head_object(Bucket=STORAGE_BUCKET_NAME, Key=key)
                metadata = head.get('Metadata', {})
                encoded_path = metadata.get('original_path_b64')
                if not encoded_path:
                    if not quiet:
                        print(f"[WARN] Пропущен {key} — нет оригинального пути в метаданных")
                    continue
                original_path = base64.b64decode(encoded_path.encode()).decode()
                # Восстанавливаем относительный путь (без диска)
                relative_path = os.path.relpath(original_path, os.path.splitdrive(original_path)[0] + os.sep)
                download_path = os.path.join(destination_root, relative_path)
                os.makedirs(os.path.dirname(download_path), exist_ok=True)
                with open(download_path, 'wb') as f:
                    s3_client.download_fileobj(STORAGE_BUCKET_NAME, key, f)
                pbar.update(head.get('ContentLength', 0))
                if not quiet:
                    print(f"Скачан {key} -> {download_path}")
            except ClientError as e:
                if not quiet:
                    print(f"[ERROR] Ошибка при загрузке {key}: {e}")
            except Exception as e:
                if not quiet:
                    print(f"[ERROR] Неожиданная ошибка при загрузке {key}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Скачивание файлов из S3 с восстановлением путей")
    parser.add_argument('--download', required=True, help="Директория для скачивания файлов из S3")
    parser.add_argument('--quiet', action='store_true', help="Тихий режим: только прогресс-бар без лишних сообщений")
    parser.add_argument('--show-count', action='store_true', help="Показать только количество файлов и общий объём, не скачивать")
    args = parser.parse_args()
    download_files_to_directories(args.download, quiet=args.quiet, show_count=args.show_count)

if __name__ == '__main__':
    main() 