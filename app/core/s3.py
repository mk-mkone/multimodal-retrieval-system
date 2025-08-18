from pathlib import Path
from typing import Optional
import uuid
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from app.core.config import settings


class S3Client:
    """
    Minimal S3/MinIO client.

    - Upload/download files and bytes
    - Build stable object keys
    - Existence check, delete, presigned URLs
    """

    def __init__(
        self,
        bucket: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: Optional[str] = None,
    ) -> None:
        self.bucket = bucket or settings.S3_BUCKET
        self._session = boto3.session.Session(
            aws_access_key_id=access_key or settings.S3_ACCESS_KEY,
            aws_secret_access_key=secret_key or settings.S3_SECRET_KEY,
            region_name=region or settings.S3_REGION or "us-east-1",
        )
        self._s3 = self._session.client(
            "s3",
            endpoint_url=endpoint_url or settings.S3_ENDPOINT_URL,
            config=Config(signature_version="s3v4"),
        )

    @staticmethod
    def make_key(prefix: str, filename: str | None = None) -> str:
        name = filename or str(uuid.uuid4())
        return f"{prefix.rstrip('/')}/{name.lstrip('/')}"

    @staticmethod
    def to_uri(bucket: str, key: str) -> str:
        return f"s3://{bucket}/{key}"

    def upload_file(self, local_path: str | Path, key: str) -> str:
        p = Path(local_path)
        self._s3.upload_file(str(p), self.bucket, key)
        return self.to_uri(self.bucket, key)

    def upload_bytes(
        self, data: bytes, key: str, content_type: Optional[str] = None
    ) -> str:
        kwargs = {"Bucket": self.bucket, "Key": key, "Body": data}
        if content_type:
            kwargs["ContentType"] = content_type
        self._s3.put_object(**kwargs)
        return self.to_uri(self.bucket, key)

    def download_file(self, key: str, local_path: str | Path) -> Path:
        p = Path(local_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        self._s3.download_file(self.bucket, key, str(p))
        return p

    def get_object_bytes(self, key: str) -> bytes:
        obj = self._s3.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()

    def exists(self, key: str) -> bool:
        try:
            self._s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
                return False
            raise

    def delete(self, key: str) -> None:
        self._s3.delete_object(Bucket=self.bucket, Key=key)

    def presigned_url(self, key: str, expires: int = 3600) -> str:
        return self._s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=expires,
        )
