from __future__ import annotations

import shutil
from pathlib import Path
from typing import Annotated

import boto3
import requests
from fastapi import APIRouter, Query, status

from bdi_api.settings import Settings
from bdi_api.s1.exercise import prepare_data as s1_prepare_data

settings = Settings()

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)

DAY = "20231101"
REMOTE_DATE_PATH = "2023/11/01"
S3_PREFIX = f"raw/day={DAY}/"


def _ensure_empty_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _generate_readsb_filenames_every_5s() -> list[str]:
    """HHmmssZ.json.gz every 5 seconds for a full day (ascending)."""
    out: list[str] = []
    for sec in range(0, 24 * 60 * 60, 5):
        hh = sec // 3600
        mm = (sec % 3600) // 60
        ss = sec % 60
        out.append(f"{hh:02d}{mm:02d}{ss:02d}Z.json.gz")
    return out


@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
Limits the number of files to download.
You must always start from the first the page returns and
go in ascending order in order to correctly obtain the results.
I'll test with increasing number of files starting from 100.
""",
        ),
    ] = 100,
) -> str:
    """Download files from ADSBExchange and upload them into S3 under raw/day=20231101/."""
    if file_limit <= 0:
        return "OK uploaded=0"

    s3_bucket = settings.s3_bucket
    if not s3_bucket:
        raise ValueError("Missing S3 bucket. Set env var BDI_S3_BUCKET.")

    base_url = f"{settings.source_url}/{REMOTE_DATE_PATH}/"

    s3 = boto3.client("s3")
    uploaded = 0
    session = requests.Session()

    for filename in _generate_readsb_filenames_every_5s():
        if uploaded >= file_limit:
            break

        url = base_url + filename

        # Some files can be missing; skip those
        head = session.head(url, timeout=30)
        if head.status_code != 200:
            continue

        s3_key = S3_PREFIX + filename

        with session.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            s3.upload_fileobj(r.raw, s3_bucket, s3_key)

        uploaded += 1

    return f"OK uploaded={uploaded}"


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Download raw files from S3 to local raw/day=20231101/ and reuse S1 prepare."""
    s3_bucket = settings.s3_bucket
    if not s3_bucket:
        raise ValueError("Missing S3 bucket. Set env var BDI_S3_BUCKET.")

    local_raw_dir = Path(settings.raw_dir) / f"day={DAY}"
    _ensure_empty_dir(local_raw_dir)

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=s3_bucket, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue

            filename = key[len(S3_PREFIX) :]
            dest = local_raw_dir / filename
            dest.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(s3_bucket, key, str(dest))

    return s1_prepare_data()