"""Cria o bucket datalake no MinIO (se ainda não existir)."""
import subprocess, sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "minio", "-q"])

from minio import Minio

client = Minio("minio.storage:9000", access_key="admin", secret_key="admin123", secure=False)

if not client.bucket_exists("datalake"):
    client.make_bucket("datalake")
    print("Bucket datalake criado.")
else:
    print("Bucket datalake já existe.")
