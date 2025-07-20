import os
import uuid
import base64
import argparse
import mimetypes
import fnmatch
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from botocore.config import Config
from tqdm import tqdm

load_dotenv()


# Конфигурация S3
STORAGE_ENDPOINT = os.getenv("STORAGE_ENDPOINT")
STORAGE_ACCESS_KEY = os.getenv("STORAGE_ACCESS_KEY")
STORAGE_SECRET_KEY = os.getenv("STORAGE_SECRET_KEY")
STORAGE_BUCKET_NAME = os.getenv("STORAGE_BUCKET_NAME")
STORAGE_REGION = os.getenv("STORAGE_REGION")
UPLOAD_URL_EXPIRATION = os.getenv("UPLOAD_URL_EXPIRATION", 86400)


s3_client = boto3.client(
    "s3",
    endpoint_url=STORAGE_ENDPOINT,
    aws_access_key_id=STORAGE_ACCESS_KEY,
    aws_secret_access_key=STORAGE_SECRET_KEY,
    region_name=STORAGE_REGION,
    config=Config(s3={"addressing_style": "virtual"}),
)

def load_exclude_patterns(file_path):
    patterns = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    patterns.append(line)
    except Exception as e:
        print(f"[WARN] Не удалось прочитать файл исключений {file_path}: {e}")
    return patterns

def should_exclude(filename, exclude_patterns):
    return any(fnmatch.fnmatch(filename, pattern) for pattern in exclude_patterns)

def get_file_md5(file_path):
    import hashlib
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def upload_files_from_directories(directories, exclude_patterns, quiet=False):
    for directory in directories:
        # Считаем общее количество файлов для прогресс-бара
        all_files = []
        for root, _, files in os.walk(directory):
            for filename in files:
                if not should_exclude(filename, exclude_patterns):
                    all_files.append((root, filename))
        with tqdm(total=len(all_files), desc=f"Загрузка из {directory}") as pbar:
            for root, filename in all_files:
                local_path = os.path.join(root, filename)
                file_md5 = get_file_md5(local_path)
                file_key = file_md5  # md5 как ключ
                original_ext = os.path.splitext(filename)[1][1:]  # Без точки

                encoded_path = base64.b64encode(local_path.encode()).decode()
                metadata = {
                    'original_path_b64': str(encoded_path),
                    'original_ext': str(original_ext)
                }

                content_type, _ = mimetypes.guess_type(filename)
                content_type = content_type or 'application/octet-stream'

                # Проверка, есть ли уже такой объект
                try:
                    s3_client.head_object(Bucket=STORAGE_BUCKET_NAME, Key=file_key)
                    if not quiet:
                        print(f"[SKIP] {local_path} уже загружен (md5: {file_md5})")
                    pbar.update(1)
                    continue
                except ClientError as e:
                    if e.response['Error']['Code'] != '404':
                        if not quiet:
                            print(f"[ERROR] Ошибка при проверке существования {local_path}: {e}")
                        pbar.update(1)
                        continue
                except Exception as e:
                    if not quiet:
                        print(f"[ERROR] Неожиданная ошибка при проверке {local_path}: {e}")
                    pbar.update(1)
                    continue

                try:
                    with open(local_path, 'rb') as f:
                        s3_client.upload_fileobj(
                            f,
                            STORAGE_BUCKET_NAME,
                            file_key,
                            ExtraArgs={
                                'Metadata': metadata,
                                'ContentType': content_type
                            }
                        )
                    if not quiet:
                        print(f"Загружен файл: {local_path} -> {file_key} ({content_type})")
                    # Отладочный вывод метаданных
                    if not quiet:
                        try:
                            head = s3_client.head_object(Bucket=STORAGE_BUCKET_NAME, Key=file_key)
                            print(f"[DEBUG] Метаданные в S3: {head.get('Metadata', {})}")
                        except Exception as meta_e:
                            print(f"[DEBUG] Не удалось получить метаданные: {meta_e}")
                except Exception as e:
                    if not quiet:
                        print(f"[ERROR] Не удалось загрузить {local_path}: {e}")
                pbar.update(1)

def download_files_to_directories(destination_root):
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=STORAGE_BUCKET_NAME):
            for obj in page.get('Contents', []):
                key = obj['Key']
                try:
                    head = s3_client.head_object(Bucket=STORAGE_BUCKET_NAME, Key=key)
                    metadata = head.get('Metadata', {})
                    encoded_path = metadata.get('original_path_b64')

                    if not encoded_path:
                        print(f"[WARN] Пропущен {key} — нет оригинального пути в метаданных")
                        continue

                    original_path = base64.b64decode(encoded_path.encode()).decode()
                    relative_path = os.path.relpath(original_path, os.path.splitdrive(original_path)[0] + os.sep)
                    download_path = os.path.join(destination_root, relative_path)

                    os.makedirs(os.path.dirname(download_path), exist_ok=True)

                    with open(download_path, 'wb') as f:
                        s3_client.download_fileobj(STORAGE_BUCKET_NAME, key, f)

                    print(f"Скачан {key} -> {download_path}")
                except ClientError as e:
                    print(f"[ERROR] Ошибка при загрузке {key}: {e}")
    except ClientError as e:
        print(f"[ERROR] Ошибка при работе с бакетом: {e}")

def main():
    parser = argparse.ArgumentParser(description="Загрузка/скачивание файлов с S3")
    parser.add_argument('--upload', nargs='+', help="Список директорий для загрузки в S3")
    parser.add_argument('--download', help="Директория для скачивания файлов из S3")
    parser.add_argument('--exclude-patterns', nargs='*', default=[], help="Шаблоны файлов для исключения")
    parser.add_argument('--exclude-from', help="Файл с шаблонами исключений (один на строку)")
    parser.add_argument('--quiet', action='store_true', help="Тихий режим: только прогресс-бар без лишних сообщений")

    args = parser.parse_args()

    exclude_patterns = args.exclude_patterns or []
    if args.exclude_from:
        file_patterns = load_exclude_patterns(args.exclude_from)
        exclude_patterns.extend(file_patterns)

    if args.upload:
        upload_files_from_directories(args.upload, exclude_patterns, quiet=args.quiet)
    elif args.download:
        download_files_to_directories(args.download)
    else:
        print("Укажи --upload <путь> или --download <путь>")

if __name__ == '__main__':
    main()
