import json
import shutil
from pathlib import Path

import boto3

from pipeline import config


class LocalLake:
    """Substitut boto3 minimal quand LAKE=local : meme surface d'API que le client S3
    (put_object / get_object / upload_file / download_file / list_objects_v2),
    backed par le systeme de fichiers sous LOCAL_ROOT/<bucket>/."""

    def __init__(self, root: Path):
        self.root = root

    def _path(self, bucket: str, key: str) -> Path:
        return self.root / bucket / key

    def put_object(self, Bucket, Key, Body, ContentType=None):
        p = self._path(Bucket, Key)
        p.parent.mkdir(parents=True, exist_ok=True)
        body = Body.read() if hasattr(Body, "read") else bytes(Body)
        p.write_bytes(body)

    def get_object(self, Bucket, Key):
        body = self._path(Bucket, Key).read_bytes()

        class _Body:
            def read(self):
                return body

        return {"Body": _Body()}

    def upload_file(self, Filename, Bucket, Key):
        p = self._path(Bucket, Key)
        p.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(Filename, p)

    def download_file(self, Bucket, Key, Filename):
        target = Path(Filename)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self._path(Bucket, Key), target)

    def list_objects_v2(self, Bucket, Prefix="", **kw):
        base = self._path(Bucket, "")
        keys = []
        if base.exists():
            keys = sorted(str(p.relative_to(base)).replace("\\", "/")
                          for p in base.rglob("*") if p.is_file())
        return {"Contents": [{"Key": k} for k in keys if k.startswith(Prefix)],
                "IsTruncated": False}


def client():
    if config.LAKE == "local":
        return LocalLake(config.LOCAL_ROOT)
    return boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ROOT_USER,
        aws_secret_access_key=config.MINIO_ROOT_PASSWORD,
        region_name="us-east-1",
    )


def put_json(s3, bucket: str, key: str, payload) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json; charset=utf-8")


def list_keys(s3, bucket: str, prefix: str):
    keys, token = [], None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        keys.extend(o["Key"] for o in resp.get("Contents", []))
        if not resp.get("IsTruncated"):
            return keys
        token = resp["NextContinuationToken"]


def load_manifests(s3):
    manifests = []
    for key in list_keys(s3, config.BUCKET_BRONZE, config.MANIFEST_PREFIX + "/"):
        obj = s3.get_object(Bucket=config.BUCKET_BRONZE, Key=key)
        manifests.append(json.loads(obj["Body"].read().decode("utf-8")))
    return manifests
