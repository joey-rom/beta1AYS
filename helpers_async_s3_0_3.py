# helpers_async_s3.py
import os
import re
import io
import base64
import shutil
import mimetypes
import zipfile
import logging
import threading
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Callable

import boto3
from botocore.config import Config as BotoConfig
from werkzeug.utils import secure_filename
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

# =========================
# Basic config
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "ays-artifacts-prod")
S3_UPLOAD_PREFIX = os.getenv("S3_UPLOAD_PREFIX", "uploads")
S3_RESULTS_PREFIX = os.getenv("S3_RESULTS_PREFIX", "results")

# Where your local Excel dashboard lives (by design this is the only persistent local file)
DASHBOARD_XLSX = os.path.join(BASE_DIR, "data", "ays_dashboard.xlsx")

_s3 = boto3.client("s3", region_name=AWS_REGION, config=BotoConfig(s3={"addressing_style": "virtual"}))

# Background executor + in-memory job store
EXECUTOR = ThreadPoolExecutor(max_workers=max(os.cpu_count() or 4, 4))
JOBS: Dict[str, Dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()

# Customer-safe columns for Excel export (no links, no internal IDs)
CUSTOMER_EXPORT_COLUMNS = [
    "Date",
    "AYS ID",
    "Email",
    "Project Name",
    "Manufacturer Terms",
    "Recommendation",
]

# =========================
# Small utils
# =========================
def s3_key(*parts) -> str:
    """Join path fragments into an S3 key safely."""
    return "/".join(str(p).strip("/\\") for p in parts if p)

def s3_upload_file(local_path: str, key: str) -> None:
    _s3.upload_file(local_path, S3_BUCKET, key)

def s3_upload_bytes(data: bytes, key: str) -> None:
    ct, _ = mimetypes.guess_type(key)
    _s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data, ContentType=ct or "application/octet-stream")

def s3_presign_get(key: str, expires: int = 3600) -> str:
    return _s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=expires,
    )

def s3_console_url_for_project(project_id: str, subpath: str = "") -> str:
    """
    AWS console URL (for admins only). Not for customers.
    """
    prefix = s3_key(S3_RESULTS_PREFIX, project_id, subpath)
    return (
        f"https://s3.console.aws.amazon.com/s3/buckets/{S3_BUCKET}"
        f"?region={AWS_REGION}&prefix={prefix}/&showversions=false"
    )

def slugify(s: str) -> str:
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[-\s]+", "_", s).strip("_")
    return s[:80] if s else "project"

def make_project_id(subject: str) -> str:
    base = slugify(subject or "Project")
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"{base}_AYS-{stamp}"

def _safe_unlink(path: str) -> None:
    try:
        if path and os.path.isfile(path):
            os.remove(path)
    except FileNotFoundError:
        pass
    except Exception as e:
        logging.warning(f"cleanup: could not remove {path}: {e}")

def _safe_rmtree(path: str) -> None:
    try:
        if path and os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)
    except Exception as e:
        logging.warning(f"cleanup: could not rmtree {path}: {e}")

# =========================
# Dashboard helpers
# =========================
_DASHBOARD_LOCK = threading.Lock()

def _normalize_mfg_terms(manufacturer_terms):
    if manufacturer_terms is None:
        return ""
    if isinstance(manufacturer_terms, (list, tuple, set)):
        return ", ".join([str(x) for x in manufacturer_terms if str(x).strip()])
    return str(manufacturer_terms).strip()


def _normalize_date_only(submitted_at):
    """
    Accepts None, datetime, or a string.
    Returns an MM/DD/YYYY string.
    """
    if submitted_at is None:
        dt = datetime.now()
    elif isinstance(submitted_at, datetime):
        dt = submitted_at
    elif isinstance(submitted_at, str):
        # Try a couple common formats you used earlier
        for fmt in ("%m/%d/%Y %H:%M", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(submitted_at, fmt)
                break
            except ValueError:
                dt = None
        if dt is None:
            dt = datetime.now()
    else:
        dt = datetime.now()
    # Date only (no time) so DataTables/Excel don’t choke
    return dt.strftime("%m/%d/%Y")

def log_completed_job_row(
    *,
    ays_id: str,
    from_email: str,
    project_name: str,
    manufacturer_terms,
    recommendation: str,
    project_id: str,
    doc_folder: str,
    zip_key: str,
    job_id: str | None = None,
    submitted_at=None,  # ISO string like 2025-08-20T02:11:32Z
):
    os.makedirs(os.path.dirname(DASHBOARD_XLSX), exist_ok=True)

    # Keep date-only for display; keep full ISO for sorting
    date_str = _normalize_date_only(submitted_at)  # -> "MM/DD/YYYY"
    submitted_at_iso = (submitted_at or datetime.utcnow().isoformat(timespec='seconds') + "Z")

    mfg_str = _normalize_mfg_terms(manufacturer_terms)
    download_url = f"/dl/{job_id}" if job_id else ""  # (or /result/<id> if you prefer)

    visible_cols = [
        "Date", "AYS ID", "Email", "Project Name",
        "Manufacturer Terms", "Recommendation", "Download URL",
    ]
    internal_cols = ["Project ID", "Doc Folder", "S3 Zip Key", "Job ID", "Submitted At"]  # <-- add
    all_cols = visible_cols + internal_cols

    new_row = {
        "Date": date_str,
        "AYS ID": ays_id or "",
        "Email": from_email or "",
        "Project Name": project_name or "",
        "Manufacturer Terms": mfg_str,
        "Recommendation": recommendation or "",
        "Download URL": download_url,
        "Project ID": project_id or "",
        "Doc Folder": doc_folder or "",
        "S3 Zip Key": zip_key or "",
        "Job ID": job_id or "",
        "Submitted At": submitted_at_iso,  # <-- store for sorting
    }

    with _DASHBOARD_LOCK:
        if os.path.exists(DASHBOARD_XLSX):
            df = pd.read_excel(DASHBOARD_XLSX, dtype=str).fillna("")
            for c in all_cols:
                if c not in df.columns:
                    df[c] = ""
            df = df[all_cols]
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        else:
            df = pd.DataFrame([new_row], columns=all_cols)

        df.to_excel(DASHBOARD_XLSX, index=False)


# Optional: legacy project/status hooks (kept for compatibility; currently no-ops to avoid extra rows)
def log_project_submission(*args, **kwargs):
    """No-op: we only add dashboard rows on completion to keep it clean."""
    return None

def update_project_status(*args, **kwargs):
    """No-op placeholder."""
    return None

def update_dashboard_progress(*args, **kwargs):
    """No-op placeholder."""
    return None

# =========================
# Simple job manager
# =========================
def submit_job(fn: Callable, *args, **kwargs) -> str:
    import uuid
    job_id = str(uuid.uuid4())
    with JOBS_LOCK:
        JOBS[job_id] = {"state": "QUEUED", "created": time.time(), "info": {}}
    fut = EXECUTOR.submit(fn, job_id, *args, **kwargs)
    with JOBS_LOCK:
        JOBS[job_id]["future"] = fut
    return job_id

def set_job(job_id: str, **fields) -> None:
    with JOBS_LOCK:
        if job_id in JOBS:
            JOBS[job_id].update(fields)

def get_job(job_id: str) -> Dict[str, Any]:
    with JOBS_LOCK:
        return JOBS.get(job_id, {})

# =========================
# Core pipeline (per document) with CLEANUP
# =========================
def run_pipeline_to_s3(
    job_id: str,
    payload: Dict[str, Any],
    callbacks: Dict[str, Any],
    upload_folder: str,    # kept for signature compatibility; we don't use it
    processed_folder: str,
) -> Dict[str, Any]:
    """
    Process ONE PDF and upload artifacts to S3:
        uploads/<ProjectID>/<DocFolder>/original.pdf  (in S3)
        results/<ProjectID>/<DocFolder>/...           (in S3)

    We avoid writing to local /uploads entirely. All scratch lives under a per-job work dir,
    which is removed in a finally block so NO local leftovers remain.

    callbacks must include:
      - process_pdf_file(path) -> dict
      - create_highlighted_only_pdf(highlighted_pdf_path, results, output_path)
      - generate_email_body(subject, total_keywords, manufacturer_rows, competitor_rows, recommendation, logo_base64, ays_id) -> HTML
      - logo_base64
    callbacks may include (optional):
      - write_results_to_excel(results, excel_path)  # if provided, we'll include tables.xlsx in ZIP
    """
    process_pdf_file = callbacks["process_pdf_file"]
    create_highlighted_only_pdf = callbacks["create_highlighted_only_pdf"]
    generate_email_body = callbacks["generate_email_body"]
    logo_base64 = callbacks["logo_base64"]
    write_results_to_excel = callbacks.get("write_results_to_excel")  # optional

    project_id = payload.get("ProjectID") or make_project_id(payload.get("Subject", "Project"))
    original_subject = payload.get("Subject") or "Untitled Project"
    secure_name = secure_filename(payload.get("AttachmentName") or "document.pdf")
    doc_folder = slugify(Path(secure_name).stem) or "doc"
    project_slug = slugify(original_subject)

    # All scratch here; we won't use the global UPLOAD_FOLDER to minimize risk if the process crashes.
    work_root = os.path.join(processed_folder, f"job-{job_id}")
    os.makedirs(work_root, exist_ok=True)

    # Save the raw upload to the job scratch dir
    raw_local = os.path.join(work_root, secure_name)

    # Keep track of anything we might need to clean up
    highlighted_local_orig = None
    email_pdf_local = None
    tables_local = None
    only_high_local = None
    high_full_local = None
    zip_local = None

    try:
        with open(raw_local, "wb") as f:
            f.write(base64.b64decode(payload["AttachmentContent"]))

        # Run your heavy processor (it produces a highlighted PDF into processed_folder)
        results = process_pdf_file(raw_local)
        if not results or not results.get("results"):
            raise RuntimeError("Processing failed.")

        # Locate the highlighted PDF that processor saved in processed_folder
        highlighted_name = results.get("filename")
        if not highlighted_name:
            raise RuntimeError("Highlighted PDF missing.")
        highlighted_local_orig = os.path.join(processed_folder, highlighted_name)
        if not os.path.isfile(highlighted_local_orig):
            raise RuntimeError("Highlighted PDF not found on disk.")

        # Create highlighted-only PDF into the job scratch
        base_name = f"{project_slug}_{doc_folder}"
        only_high_local = os.path.join(work_root, f"{base_name}_only_highlights.pdf")
        create_highlighted_only_pdf(highlighted_local_orig, results, only_high_local)

        # Copy full highlighted PDF into scratch (for the ZIP)
        high_full_local = os.path.join(work_root, f"{base_name}_highlighted.pdf")
        shutil.copy2(highlighted_local_orig, high_full_local)

        # Optional: tables.xlsx if caller provides writer
        if write_results_to_excel:
            excel_local = os.path.join(work_root, f"tables_{doc_folder}.xlsx")
            write_results_to_excel(results, excel_local)
            tables_local = os.path.join(work_root, f"{base_name}_tables.xlsx")
            shutil.copy2(excel_local, tables_local)

        # Build email summary PDF
        results_data = results["results"]
        total_keywords = sum(len(t) for t in results_data.values() if t)
        # right after: results_data = results.get("results", {})  (keep that line)

        def _extract_rows(results_data, key):
            out = []
            for r in (results_data.get(key) or []):
                if not isinstance(r, dict):
                    continue
                out.append({
                    "Word": str(r.get("Word", "")),
                    "Page": r.get("Page", ""),
                    "Section": r.get("Section", ""),
                    "Section Name": r.get("Section Name", ""),
                })
            return out
        
        manufacturer_rows = _extract_rows(results_data, "manufacturer")
        competitor_rows   = _extract_rows(results_data, "competitor")


        has_mfg, has_comp = bool(manufacturer_rows), bool(competitor_rows)
        if has_mfg and has_comp:
            recommendation, subject_summary = (
                "You and your competitor are specified. Bid this opportunity!",
                "Specified - Bid!",
            )
        elif has_mfg:
            recommendation, subject_summary = (
                "You are Specified! Bid this opportunity!",
                "Specified - Bid!",
            )
        elif has_comp:
            recommendation, subject_summary = (
                "Your competitor is specified - Review this opportunity.",
                "Competitor Specified",
            )
        else:
            recommendation, subject_summary = (
                "You are not specified. Pass on this opportunity",
                "Not Specified - Do not Bid!",
            )

        ays_id = f"AYS-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        from xhtml2pdf import pisa
        email_pdf_local = os.path.join(work_root, f"{base_name}_email_summary.pdf")
        email_body_html = generate_email_body(
            original_subject,
            total_keywords,
            manufacturer_rows,
            competitor_rows,
            recommendation,
            logo_base64,
            ays_id,
        )
        with open(email_pdf_local, "wb") as f:
            pisa.CreatePDF(email_body_html, dest=f)

        # Create ZIP bundle
        zip_local = os.path.join(work_root, f"{base_name}_report.zip")
        with zipfile.ZipFile(zip_local, "w") as zipf:
            zipf.write(high_full_local, arcname=os.path.basename(high_full_local))
            zipf.write(only_high_local, arcname=os.path.basename(only_high_local))
            if tables_local and os.path.isfile(tables_local):
                zipf.write(tables_local, arcname=os.path.basename(tables_local))
            zipf.write(email_pdf_local, arcname=os.path.basename(email_pdf_local))

        # --------------------------
        # Upload to S3
        # --------------------------
        # Raw original goes to uploads/<project>/<doc>/original.pdf
        upload_key = s3_key(S3_UPLOAD_PREFIX, project_id, doc_folder, "original.pdf")
        s3_upload_file(raw_local, upload_key)

        # Artifacts under results/<project>/<doc>/
        result_prefix = s3_key(S3_RESULTS_PREFIX, project_id, doc_folder)

        def _put(local_path: str, name: str) -> Dict[str, str]:
            key = s3_key(result_prefix, name)
            s3_upload_file(local_path, key)
            return {"key": key, "url": s3_presign_get(key, expires=3600)}

        s3_objects = {}
        s3_objects[os.path.basename(high_full_local)] = _put(high_full_local, os.path.basename(high_full_local))
        s3_objects[os.path.basename(only_high_local)] = _put(only_high_local, os.path.basename(only_high_local))
        if tables_local and os.path.isfile(tables_local):
            s3_objects[os.path.basename(tables_local)] = _put(tables_local, os.path.basename(tables_local))
        s3_objects[os.path.basename(email_pdf_local)] = _put(email_pdf_local, os.path.basename(email_pdf_local))
        s3_objects[os.path.basename(zip_local)] = _put(zip_local, os.path.basename(zip_local))

        zip_key = s3_objects[os.path.basename(zip_local)]["key"]
        zip_url = s3_objects[os.path.basename(zip_local)]["url"]

        # Return rich metadata so the route can log dashboard & expose /result/<job_id>
        return {
            "job_id": job_id,
            "project_id": project_id,
            "doc_folder": doc_folder,
            "upload_key": upload_key,
            "result_prefix": result_prefix,
            "files": s3_objects,
            "zip_key": zip_key,
            "zip_url": zip_url,
            "ays_id": ays_id,
            "manufacturer_terms": [row.get("Word", "") for row in manufacturer_rows if row.get("Word")],
            "recommendation": subject_summary,
        }

    finally:
        # --- CLEANUP: ensure no local artifacts remain ---
        # Remove the highlighted PDF that your processor wrote into the global processed folder
        _safe_unlink(highlighted_local_orig)

        # Remove the entire per-job scratch tree (raw upload + all derived files)
        _safe_rmtree(work_root)

        # Optional: if you still have a classic /awsays/uploads or /awsays/processed, keep them tidy
        # (We didn't write to UPLOAD_FOLDER at all, but this helps in case other parts did.)
        try:
            for p in (upload_folder, processed_folder):
                if p and os.path.isdir(p):
                    # Remove empty subdirs left behind
                    for root, dirs, files in os.walk(p, topdown=False):
                        for name in files:
                            # no-op: don't blindly remove; we only clean our own work_root above
                            pass
                        for name in dirs:
                            full = os.path.join(root, name)
                            if not os.listdir(full):
                                _safe_rmtree(full)
        except Exception as e:
            logging.warning(f"cleanup: post-walk tidy failed: {e}")
            
# --- S3 File Explorer helpers -----------------------------------------------
# Root of the in-app browser (keep it inside results/)
BROWSER_ROOT = f"{S3_RESULTS_PREFIX.strip('/')}/"  # e.g. "results/"

def _clean_prefix(raw: str | None) -> str:
    """
    Normalize a client-provided prefix into a safe S3 prefix under BROWSER_ROOT.
    Removes leading slashes, prevents .. traversal, and ensures trailing slash
    for folder-like listing calls.
    """
    raw = (raw or "").strip()
    raw = raw.lstrip("/\\")
    raw = raw.replace("..", "")
    if not raw or not raw.startswith(BROWSER_ROOT):
        # default to root if nothing / or outside allowed tree
        norm = BROWSER_ROOT
    else:
        norm = raw
    if not norm.endswith("/"):
        norm += "/"
    return norm

def is_allowed_key(key: str) -> bool:
    """Only allow keys under the browser root."""
    return key.startswith(BROWSER_ROOT)

def s3_list_dir(prefix: str) -> dict:
    """
    List a 'directory' (prefix) like a file explorer:
      - folders: list of {name, prefix}
      - files:   list of {name, key, size, last_modified, content_type}
    """
    prefix = _clean_prefix(prefix)
    paginator = _s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix=prefix,
        Delimiter="/",
    )

    folders = []
    files = []
    for page in pages:
        for cp in page.get("CommonPrefixes", []):
            p = cp.get("Prefix")
            if not p:
                continue
            # last folder component without trailing slash
            name = p.rstrip("/").split("/")[-1]
            folders.append({"name": name, "prefix": p})

        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):  # skip the directory key itself
                continue
            name = key[len(prefix):]
            if "/" in name:
                # anything deeper will show via CommonPrefixes, skip
                continue
            files.append({
                "name": name,
                "key": key,
                "size": obj.get("Size", 0),
                "last_modified": (obj.get("LastModified") or datetime.utcnow()).isoformat(),
                "content_type": mimetypes.guess_type(name)[0] or "application/octet-stream",
            })

    # breadcrumbs for UI
    # e.g. results/Proj/Doc/  -> ["results", "Proj", "Doc"]
    crumb_parts = prefix.rstrip("/").split("/")
    crumbs = []
    walk = []
    for part in crumb_parts:
        if part == "":
            continue
        walk.append(part)
        pfx = "/".join(walk) + "/"
        # only make crumbs clickable if inside BROWSER_ROOT (avoid "results" itself linking out)
        crumbs.append({"label": part, "prefix": pfx if pfx.startswith(BROWSER_ROOT) else BROWSER_ROOT})

    return {
        "prefix": prefix,
        "breadcrumbs": crumbs,
        "folders": folders,
        "files": files,
        "bucket": S3_BUCKET,
        "root": BROWSER_ROOT,
    }


def project_index_from_dashboard():
    """
    Returns { project_id: {project_name, date, email, sort_key:int} }
    using the latest row per Project ID based on Submitted At (ISO).
    """
    if not os.path.exists(DASHBOARD_XLSX):
        return {}

    try:
        df = pd.read_excel(DASHBOARD_XLSX).fillna("")
    except Exception:
        return {}

    needed = {"Project ID", "Project Name", "Date", "Email"}
    if not needed.issubset(set(df.columns)):
        return {}

    # Prefer Submitted At; fallback to Date
    df["__submitted"] = pd.to_datetime(df.get("Submitted At", ""), errors="coerce", utc=True)
    fallback = pd.to_datetime(df.get("Date", ""), errors="coerce", utc=True)
    df["__submitted"] = df["__submitted"].fillna(fallback)

    # keep newest per project
    latest = df.sort_values(["Project ID", "__submitted"]).groupby("Project ID", as_index=False).tail(1)

    out = {}
    for _, r in latest.iterrows():
        pid = str(r.get("Project ID", "")).strip()
        if not pid:
            continue
        ts = r["__submitted"]
        sort_key = int(ts.timestamp()) if pd.notna(ts) else 0
        out[pid] = {
            "project_name": str(r.get("Project Name", "")).strip() or pid,
            "date": str(r.get("Date", "")).strip(),
            "email": str(r.get("Email", "")).strip(),
            "sort_key": sort_key,
        }
    return out




