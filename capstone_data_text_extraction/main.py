#!/usr/bin/env python3
"""
Event-driven PDF -> Parquet extractor for ECS Fargate.

Modes:
  --mode sweep        : one-time backfill over an input prefix
  --mode sqs-worker   : long-poll SQS for S3:ObjectCreated events; process keys
"""

import os, io, gc, sys, json, time, hashlib, logging, tempfile, traceback, argparse, math
from datetime import datetime, timezone
from pathlib import PurePosixPath

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pandas as pd
import pyarrow as pa, pyarrow.parquet as pq
import fitz  # PyMuPDF
from PIL import Image
import pytesseract

# ---------------- config / knobs ----------------
logging.basicConfig(level=os.getenv("LOGLEVEL","INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("worker")

DEFAULT_REGION   = "us-east-1"
OCR_LANG         = os.getenv("OCR_LANG", "eng")
OCR_DPI          = int(os.getenv("OCR_DPI", "200"))
MIN_TEXT_LEN     = int(os.getenv("MIN_TEXT_LEN", "20"))
BATCH_SIZE       = int(os.getenv("BATCH_SIZE", "50"))
SKIP_IF_EXISTS   = os.getenv("SKIP_IF_EXISTS","true").lower() == "true"
SORT_KEYS        = os.getenv("SORT_KEYS","true").lower() == "true"
VIS_TIMEOUT      = int(os.getenv("VISIBILITY_TIMEOUT","900"))
WAIT_TIME        = int(os.getenv("WAIT_TIME_SECONDS","20"))
MAX_SQS_BATCH    = int(os.getenv("MAX_SQS_BATCH","5"))
os.environ.setdefault("OMP_THREAD_LIMIT","1")   # keep Tesseract modest

# botocore client with retries
cfg = Config(retries={"max_attempts": 10, "mode": "standard"})
s3_client_cache = {}

def s3(region: str):
    # small cache so we reuse clients
    if region not in s3_client_cache:
        s3_client_cache[region] = boto3.client("s3", region_name=region, config=cfg)
    return s3_client_cache[region]

# ---------------- helpers ----------------
def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def stable_doc_id(local_path: str) -> str:
    st = os.stat(local_path)
    h = hashlib.sha1(f"{os.path.basename(local_path)}|{st.st_size}|{int(st.st_mtime)}".encode()).hexdigest()
    return h[:20]

def out_key_for(in_key: str, in_prefix: str, out_prefix: str) -> str:
    assert in_key.startswith(in_prefix), f"Key not under {in_prefix}: {in_key}"
    rel = in_key[len(in_prefix):]
    p   = PurePosixPath(rel)
    return f"{out_prefix}{p.parent.as_posix()}/{p.stem}_raw_text.parquet"

def list_pdf_keys(s3c, bucket: str, prefix: str, max_files=None, sort_keys=True):
    keys = []
    paginator = s3c.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.lower().endswith(".pdf"):
                keys.append(k)
    if sort_keys: keys.sort()
    if max_files: keys = keys[:max_files]
    return keys

def s3_exists(s3c, bucket: str, key: str) -> bool:
    try:
        s3c.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False

def s3_download_with_retry(s3c, bucket: str, key: str, dest: str, attempts: int = 5):
    for i in range(1, attempts+1):
        try:
            s3c.download_file(Bucket=bucket, Key=key, Filename=dest)
            return
        except Exception as e:
            log.warning("[retry %d/%d] download %s: %s", i, attempts, key, e)
            if i == attempts: raise
            time.sleep(min(2**i, 20))

def s3_upload_with_retry(s3c, local_path: str, bucket: str, key: str, extra_args=None, attempts: int = 5):
    for i in range(1, attempts+1):
        try:
            s3c.upload_file(local_path, bucket, key, ExtraArgs=(extra_args or {}))
            return
        except Exception as e:
            log.warning("[retry %d/%d] upload %s: %s", i, attempts, key, e)
            if i == attempts: raise
            time.sleep(min(2**i, 20))

# ---------------- OCR (disk render) ----------------
def ocr_page_to_text(doc: fitz.Document, page_idx: int, dpi: int, lang: str) -> str:
    page = doc.load_page(page_idx)
    zoom = dpi / 72.0
    mat  = fitz.Matrix(zoom, zoom)
    pix  = page.get_pixmap(matrix=mat, colorspace=fitz.csGRAY, alpha=False)  # grayscale
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        pix.save(tmp.name); tmp_path = tmp.name
    try:
        cfg  = r"--oem 1 --psm 6 -c preserve_interword_spaces=1"
        text = pytesseract.image_to_string(tmp_path, lang=lang, config=cfg).strip()
    finally:
        try: os.remove(tmp_path)
        except: pass
        gc.collect()
    return text

# ---------------- Extract (PyMuPDF + OCR), streaming Parquet ----------------
def extract_to_parquet_stream(local_pdf_path: str, out_local_parquet: str):
    ts = datetime.now(timezone.utc)
    doc_id = stable_doc_id(local_pdf_path)
    source_name = os.path.basename(local_pdf_path)

    writer = None
    batch  = []
    ocr_pages = 0

    def flush():
        nonlocal writer, batch
        if not batch: return
        dfb = pd.DataFrame(batch)
        tbl = pa.Table.from_pandas(dfb, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(out_local_parquet, tbl.schema, compression="zstd")
        writer.write_table(tbl)
        batch.clear()
        del dfb, tbl
        gc.collect()

    # Opening the PDF can fail on corrupt files; catch and bubble up gracefully
    try:
        doc = fitz.open(local_pdf_path)
    except Exception as e:
        raise RuntimeError(f"Failed to open PDF: {e}")

    with doc:
        n_pages = doc.page_count
        for i in range(n_pages):
            try:
                page = doc.load_page(i)
                txt = (page.get_text("text") or "").strip()
                is_ocr = False
                if len(txt) < MIN_TEXT_LEN:
                    txt = ocr_page_to_text(doc, i, OCR_DPI, OCR_LANG)
                    is_ocr = True
                    ocr_pages += 1

                batch.append({
                    "doc_id": doc_id,
                    "source_name": source_name,
                    "page": i+1,
                    "text": txt,           # RAW ONLY
                    "is_ocr": is_ocr,
                    "char_len": len(txt),
                    "sha256": sha256(txt),
                    "extracted_at": ts
                })
                if len(batch) >= BATCH_SIZE:
                    flush()
            except Exception as e:
                # keep going on single-page failures; log and continue
                log.error("[page-error] %s page %d: %s", source_name, i+1, e)
                continue

    flush()
    if writer: writer.close()
    return n_pages, ocr_pages

def process_one_key(region: str, bucket: str, in_prefix: str, out_prefix: str, in_key: str, sse_args=None, index:int=0, total:int=0):
    s3c = s3(region)
    out_key = out_key_for(in_key, in_prefix, out_prefix)

    if SKIP_IF_EXISTS and s3_exists(s3c, bucket, out_key):
        log.info("[%d/%d] [skip] s3://%s/%s (exists)", index, total, bucket, out_key)
        return

    log.info("[%d/%d] [start] s3://%s/%s", index, total, bucket, in_key)
    t0 = time.time()

    with tempfile.TemporaryDirectory() as td:
        local_pdf     = f"{td}/{PurePosixPath(in_key).name}"
        local_parquet = f"{td}/{PurePosixPath(out_key).name}"

        # download with retries
        s3_download_with_retry(s3c, bucket, in_key, local_pdf)

        # extract -> Parquet
        n_pages, ocr_pages = extract_to_parquet_stream(local_pdf, local_parquet)

        # upload with retries
        s3_upload_with_retry(s3c, local_parquet, bucket, out_key, extra_args=(sse_args or {"ServerSideEncryption":"AES256"}))

    dt = time.time() - t0
    ocr_ratio = (ocr_pages / n_pages) if n_pages else 0.0
    log.info("[%d/%d] [ok] %s -> s3://%s/%s (pages=%d, ocr=%.1f%%, %.1fs)",
             index, total, in_key, bucket, out_key, n_pages, ocr_ratio*100.0, dt)

# ---------------- SQS worker ----------------
def parse_s3_event(record: dict) -> tuple[str,str]:
    evt = record.get("s3", {})
    b   = evt.get("bucket", {}).get("name")
    k   = evt.get("object", {}).get("key")
    return b, k

def run_sqs_worker(region: str, bucket: str, out_prefix: str, in_prefix_hint: str | None, queue_url: str, sse_args=None):
    s3c = s3(region)
    sqs = boto3.client("sqs", region_name=region, config=cfg)

    log.info("SQS worker started. queue=%s", queue_url)
    while True:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=MAX_SQS_BATCH,
            WaitTimeSeconds=WAIT_TIME,
            VisibilityTimeout=VIS_TIMEOUT,
        )
        msgs = resp.get("Messages", [])
        if not msgs:
            continue

        for m in msgs:
            receipt = m["ReceiptHandle"]
            try:
                body = json.loads(m["Body"])
                if "Message" in body and isinstance(body["Message"], str):
                    body = json.loads(body["Message"])
                for rec in body.get("Records", []):
                    if rec.get("eventSource") == "aws:s3":
                        b, k = parse_s3_event(rec)
                        if not b or not k:
                            continue
                        if in_prefix_hint and not k.startswith(in_prefix_hint):
                            log.info("[skip-key] %s (outside hint prefix)", k); continue
                        if not k.lower().endswith(".pdf"):
                            log.info("[skip-nonpdf] %s", k); continue
                        in_prefix = in_prefix_hint or "/".join(k.split("/")[:3]) + "/"
                        process_one_key(region, bucket=bucket, in_prefix=in_prefix, out_prefix=out_prefix, in_key=k, sse_args=sse_args, index=1, total=1)
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
            except Exception as e:
                log.error("Processing error: %s", e)
                traceback.print_exc()
                # leave message for retry
                continue

# ---------------- Sweep mode ----------------
def run_sweep(region: str, bucket: str, in_prefix: str, out_prefix: str, max_files: int | None, sse_args=None):
    s3c = s3(region)
    keys = list_pdf_keys(s3c, bucket, in_prefix, max_files=max_files, sort_keys=SORT_KEYS)
    if not keys:
        log.warning("No PDFs under s3://%s/%s", bucket, in_prefix)
        return
    total = len(keys)
    log.info("Found %d PDF(s) under s3://%s/%s", total, bucket, in_prefix)

    done = 0
    t0_all = time.time()
    for idx, k in enumerate(keys, start=1):
        try:
            process_one_key(region, bucket, in_prefix, out_prefix, k, sse_args=sse_args, index=idx, total=total)
            done += 1
        except Exception as e:
            log.error("[ERROR] %s: %s", k, e)
            traceback.print_exc()
    dt_all = time.time() - t0_all
    log.info("[done] sweep processed %d/%d PDFs in %.1fs", done, total, dt_all)

# ---------------- CLI ----------------
def main():
    ap = argparse.ArgumentParser(description="Event-driven S3 PDF->Parquet extractor")
    ap.add_argument("--mode", choices=["sweep","sqs-worker"], required=True)
    ap.add_argument("--bucket", default=os.getenv("BUCKET"), help="S3 bucket")
    ap.add_argument("--in-prefix", default=os.getenv("IN_PREFIX"), help="Input prefix (sweep mode) or hint (worker)")
    ap.add_argument("--out-prefix", default=os.getenv("OUT_PREFIX"), required=False, help="Output prefix")
    ap.add_argument("--region", default=os.getenv("AWS_REGION", DEFAULT_REGION))
    ap.add_argument("--max-files", type=int, default=None, help="Max PDFs in sweep mode (omit for full run)")
    ap.add_argument("--queue-url", default=os.getenv("SQS_QUEUE_URL"), help="SQS queue URL (sqs-worker mode)")
    ap.add_argument("--sse", choices=["AES256","aws:kms"], default=os.getenv("SSE","AES256"))
    ap.add_argument("--kms-key-id", default=os.getenv("SSE_KMS_KEY_ID"))
    args = ap.parse_args()

    if not args.bucket:
        ap.error("--bucket (or BUCKET env) is required")
    if not args.out_prefix:
        ap.error("--out-prefix (or OUT_PREFIX env) is required")

    sse_args = {"ServerSideEncryption": "AES256"} if args.sse == "AES256" else \
               {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": args.kms_key_id}

    if args.mode == "sweep":
        if not args.in_prefix:
            ap.error("--in-prefix required in sweep mode")
        ip = args.in_prefix if args.in_prefix.endswith("/") else args.in_prefix + "/"
        op = args.out_prefix if args.out_prefix.endswith("/") else args.out_prefix + "/"
        run_sweep(args.region, args.bucket, ip, op, args.max_files, sse_args=sse_args)
    else:
        if not args.queue_url:
            ap.error("--queue-url (or SQS_QUEUE_URL env) required in sqs-worker mode")
        op = args.out_prefix if args.out_prefix.endswith("/") else args.out_prefix + "/"
        run_sqs_worker(args.region, args.bucket, op, args.in_prefix, args.queue_url, sse_args=sse_args)

if __name__ == "__main__":
    main()
