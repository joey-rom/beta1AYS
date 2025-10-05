# helpers_async_s3.py
from __future__ import annotations

# =========================
# Imports
# =========================
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
import pandas as pd
from botocore.config import Config as BotoConfig
from werkzeug.utils import secure_filename
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote as urlquote

# ---- global single-doc lock (add this near imports) ----
import fcntl
from contextlib import contextmanager

_LOCK_PATH = os.environ.get("AYS_LOCK_FILE", "/awsays/run/ays_pipeline.lock")
os.makedirs(os.path.dirname(_LOCK_PATH), exist_ok=True)

@contextmanager
def _global_job_lock():
    fd = os.open(_LOCK_PATH, os.O_CREAT | os.O_RDWR)
    try:
        logging.debug("AYS: waiting for global pipeline lock")
        fcntl.flock(fd, fcntl.LOCK_EX)  # blocks until available
        logging.debug("AYS: acquired global pipeline lock")
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
        logging.debug("AYS: released global pipeline lock")


# =========================
# App / Env config
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "ays-artifacts-beta1")
S3_UPLOAD_PREFIX = os.getenv("S3_UPLOAD_PREFIX", "uploads")
S3_RESULTS_PREFIX = os.getenv("S3_RESULTS_PREFIX", "results")

# Only persistent local file by design (dashboard workbook)
DASHBOARD_XLSX = os.path.join(BASE_DIR, "data", "ays_dashboard.xlsx")

# Public base for absolute links (so HTML viewed from S3 still resolves app routes)
APP_PUBLIC_BASE = os.getenv("APP_PUBLIC_BASE", "").rstrip("/")

# Root for the in-app file explorer (safety guard)
BROWSER_ROOT = f"{S3_RESULTS_PREFIX.strip('/')}/"  # e.g. "results/"

# =========================
# AWS client & executor
# =========================
_s3 = boto3.client("s3", region_name=AWS_REGION, config=BotoConfig(s3={"addressing_style": "virtual"}))
EXECUTOR = ThreadPoolExecutor(max_workers=max(os.cpu_count() or 4, 4))

# Lightweight in-memory job store
JOBS: Dict[str, Dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()

# =========================
# S3 utilities
# =========================
def s3_key(*parts) -> str:
    """Join path fragments into a safe S3 key."""
    return "/".join(str(p).strip("/\\") for p in parts if p)

def s3_upload_file(local_path: str, key: str) -> None:
    _s3.upload_file(local_path, S3_BUCKET, key)

def s3_upload_bytes(data: bytes, key: str) -> None:
    ct, _ = mimetypes.guess_type(key)
    _s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data, ContentType=ct or "application/octet-stream")

def s3_presign_get(key: str, expires: int = 3600, extra: dict | None = None) -> str:
    """
    Presign S3 GET. If extra is provided, it can include:
      ResponseContentType, ResponseContentDisposition, etc.
    """
    params = {"Bucket": S3_BUCKET, "Key": key}
    if extra:
        params.update(extra)
    return _s3.generate_presigned_url("get_object", Params=params, ExpiresIn=expires)

def s3_console_url_for_project(project_id: str, subpath: str = "") -> str:
    """Admin-only convenience link to the bucket console for a project."""
    prefix = s3_key(S3_RESULTS_PREFIX, project_id, subpath)
    return (
        f"https://s3.console.aws.amazon.com/s3/buckets/{S3_BUCKET}"
        f"?region={AWS_REGION}&prefix={prefix}/&showversions=false"
    )

# =========================
# General helpers
# =========================
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
# Dashboard writer
# =========================
_DASHBOARD_LOCK = threading.Lock()

def _normalize_mfg_terms(manufacturer_terms) -> str:
    if manufacturer_terms is None:
        return ""
    if isinstance(manufacturer_terms, (list, tuple, set)):
        return ", ".join([str(x) for x in manufacturer_terms if str(x).strip()])
    return str(manufacturer_terms).strip()

def _normalize_date_only(submitted_at) -> str:
    """
    Accepts None, datetime, or a string.
    Returns an 'MM/DD/YYYY' string for user-facing display.
    """
    if submitted_at is None:
        dt = datetime.now()
    elif isinstance(submitted_at, datetime):
        dt = submitted_at
    elif isinstance(submitted_at, str):
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
    submitted_at=None,  # e.g. "2025-08-20T02:11:32Z" (stored for sorting)
) -> None:
    """
    Append a finished-document row to the dashboard xlsx.
    Visible columns: Date, AYS ID, Email, Project Name, Manufacturer Terms, Recommendation, Download URL
    Internal columns: Project ID, Doc Folder, S3 Zip Key, Job ID, Submitted At
    """
    os.makedirs(os.path.dirname(DASHBOARD_XLSX), exist_ok=True)

    # display date (no time) and stored ISO for stable sorting
    date_str = _normalize_date_only(submitted_at)  # "MM/DD/YYYY"
    submitted_at_iso = submitted_at or datetime.utcnow().isoformat(timespec="seconds") + "Z"

    new_row = {
        "Date": date_str,
        "AYS ID": ays_id or "",
        "Email": from_email or "",
        "Project Name": project_name or "",
        "Manufacturer Terms": _normalize_mfg_terms(manufacturer_terms),
        "Recommendation": recommendation or "",
        "Download URL": f"/dl/{job_id}" if job_id else "",
        # internal
        "Project ID": project_id or "",
        "Doc Folder": doc_folder or "",
        "S3 Zip Key": zip_key or "",
        "Job ID": job_id or "",
        "Submitted At": submitted_at_iso,
    }

    visible_cols = [
        "Date", "AYS ID", "Email", "Project Name",
        "Manufacturer Terms", "Recommendation", "Download URL",
    ]
    internal_cols = ["Project ID", "Doc Folder", "S3 Zip Key", "Job ID", "Submitted At"]
    all_cols = visible_cols + internal_cols

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

# =========================
# Explorer index helpers (newest-first)
# =========================
def _parse_dt_any(s: str) -> datetime:
    if not s:
        return datetime.fromtimestamp(0)
    for fmt in ("%m/%d/%Y %H:%M", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(s), fmt)
        except ValueError:
            continue
    try:
        # ISO like "2025-08-20T02:11:32Z"
        return datetime.fromisoformat(str(s).rstrip("Z"))
    except Exception:
        return datetime.fromtimestamp(0)

def project_index_from_dashboard() -> dict[str, dict]:
    """
    Returns {project_id: {project_id, project_name, email, date, sort_key}}
    using the latest row per Project ID based on 'Submitted At' (ISO) or fallback to 'Date'.
    """
    if not os.path.exists(DASHBOARD_XLSX):
        return {}

    try:
        df = pd.read_excel(DASHBOARD_XLSX, dtype=str).fillna("")
    except Exception:
        return {}

    needed = {"Project ID", "Project Name", "Date", "Email"}
    if not needed.issubset(df.columns):
        return {}

    # Prefer Submitted At; fallback to Date
    sub = pd.to_datetime(df.get("Submitted At", ""), errors="coerce", utc=True)
    dat = pd.to_datetime(df.get("Date", ""), errors="coerce", utc=True)
    df["__submitted"] = sub.fillna(dat)

    latest = df.sort_values(["Project ID", "__submitted"]).groupby("Project ID", as_index=False).tail(1)

    out: dict[str, dict] = {}
    for _, r in latest.iterrows():
        pid = str(r.get("Project ID", "")).strip()
        if not pid:
            continue
        ts = r["__submitted"]
        sort_key = int(ts.timestamp()) if pd.notna(ts) else 0
        out[pid] = {
            "project_id": pid,
            "project_name": str(r.get("Project Name", "")).strip() or pid.split("_AYS-")[0].replace("_", " "),
            "email": str(r.get("Email", "")).strip(),
            "date": str(r.get("Date", "")).strip(),
            "sort_key": sort_key,
        }
    return out

def list_projects_from_dashboard() -> list[dict]:
    items = list(project_index_from_dashboard().values())
    items.sort(key=lambda x: x.get("sort_key", 0), reverse=True)  # newest first
    return items

def get_project_meta(project_id: str) -> dict | None:
    return project_index_from_dashboard().get(project_id)

def list_project_docs(project_id: str) -> list[dict]:
    """
    Inspect S3 at results/<project_id>/ and return:
      [{ doc_folder, artifacts: {highlighted, only_highlights, tables, email_pdf, email_html, zip} }, ...]
    """
    prefix = s3_key(S3_RESULTS_PREFIX, project_id) + "/"
    try:
        resp = _s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/")
    except Exception:
        logging.exception("list_project_docs: list_objects_v2 failed (project root)")
        return []

    folders = [cp.get("Prefix", "") for cp in resp.get("CommonPrefixes", []) if cp.get("Prefix")]
    out: list[dict] = []

    for f in folders:
        # f like: results/<project_id>/<doc_folder>/
        doc_folder = f.rstrip("/").split("/")[-1]
        try:
            resp2 = _s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f)
        except Exception:
            logging.exception("list_project_docs: list_objects_v2 failed (doc folder)")
            continue

        arts: dict[str, str | None] = {
            "zip": None,
            "tables": None,
            "highlighted": None,
            "only_highlights": None,
            "email_pdf": None,
            "email_html": None,
        }

        for obj in resp2.get("Contents", []):
            key = obj.get("Key", "")
            if not key or key.endswith("/"):
                continue
            name = key.rsplit("/", 1)[-1].lower()

            if name.endswith("_report.zip"):
                arts["zip"] = key
            elif name.endswith("_tables.xlsx") or name.endswith("tables.xlsx"):
                arts["tables"] = key
            elif name.endswith("_highlighted.pdf") or name.endswith("highlighted.pdf"):
                arts["highlighted"] = key
            elif name.endswith("_only_highlights.pdf") or name.endswith("only_highlights.pdf"):
                arts["only_highlights"] = key
            elif name.endswith("_email_summary.pdf") or name.endswith("email_summary.pdf"):
                arts["email_pdf"] = key
            elif name.endswith("_email_summary.html") or name.endswith("email_summary.html"):
                arts["email_html"] = key

        out.append({"doc_folder": doc_folder, "artifacts": arts})

    out.sort(key=lambda d: d["doc_folder"])  # adjust if you want another order
    return out

# =========================
# In-memory job manager
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
# Email table + body
# =========================
def format_html_table(rows, pdf_key: str | None = None) -> str:
    """
    Build HTML table. If pdf_key provided, each row gets a 'View' link to open
    the highlighted PDF at the row's Page number.
    """
    rows = rows or []
    if not rows:
        return "<p><em>No results.</em></p>"

    header_style = (
        "background:#ff8c00;color:#fff;"
        "border-bottom:1px solid #e5e7eb;padding:6px 8px;"
        "text-align:left;"
    )
    cell_style = "border-bottom:1px solid #f0f2f5;padding:6px 8px"

    base = APP_PUBLIC_BASE or ""
    from urllib.parse import quote as urlquote
    def link_for(page):
        if not pdf_key:
            return None
        try:
            p = int(str(page))
            return f"{base}/view/by-key?key={urlquote(pdf_key)}#page={p}"
        except Exception:
            return None

    out = [
        '<table style="width:100%;border-collapse:collapse;font-size:14px">',
        "<thead><tr>",
        f'<th style="{header_style}">Word</th>',
        f'<th style="{header_style}">Page</th>',
        f'<th style="{header_style}">Section</th>',
        f'<th style="{header_style}">Section Name</th>',
        f'<th style="{header_style}">View</th>',
        "</tr></thead><tbody>",
    ]
    for r in rows:
        word    = (r.get("Word") or "").strip()
        page    = r.get("Page", "")
        section = r.get("Section", "")
        sname   = r.get("Section Name", "")
        href    = link_for(page)
        view_td = f'<a href="{href}" target="_blank" rel="noopener">Open PDF</a>' if href else ""
        out.append(
            "<tr>"
            f'<td style="{cell_style}">{word}</td>'
            f'<td style="{cell_style}">{page}</td>'
            f'<td style="{cell_style}">{section}</td>'
            f'<td style="{cell_style}">{sname}</td>'
            f'<td style="{cell_style}">{view_td}</td>'
            "</tr>"
        )
    out.append("</tbody></table>")
    return "".join(out)


def generate_email_body(
    original_subject: str,
    total_keywords: int,
    manufacturer_rows: list[dict],
    competitor_rows: list[dict],
    recommendation: str,
    logo_base64: str,
    ays_id: str,
    highlighted_pdf_key: str | None = None,
) -> str:
    """
    Returns HTML summary. If highlighted_pdf_key is provided, rows include 'View' links
    that open the highlighted PDF on the correct page.
    """
    mfg_rows = manufacturer_rows or []
    comp_rows = competitor_rows or []

    logo_tag = (
        f'<img alt="AYS" src="data:image/png;base64,{logo_base64}" '
        f'style="height:40px;vertical-align:middle;margin-right:10px" />'
    )

    mfg_table_html = format_html_table(mfg_rows, pdf_key=highlighted_pdf_key)
    comp_table_html = format_html_table(comp_rows, pdf_key=highlighted_pdf_key)

    base_href = (APP_PUBLIC_BASE or "/") + "/"
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <base href="{base_href}" target="_blank">
  <title>{original_subject} — AYS Summary</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body {{ font-family: Arial, sans-serif; color:#111; margin:0; padding:24px; background:#fff; }}
    .header {{ display:flex; align-items:center; gap:12px; margin-bottom:16px; }}
    .h1 {{ font-size:20px; font-weight:700; margin:0; }}
    .card {{ border:1px solid #e5e7eb; border-radius:12px; padding:16px; margin:14px 0; background:#fafafa; }}
    .muted {{ color:#555; }}
    .kv {{ line-height:1.6; }}
    .kv b {{ display:inline-block; width:200px; }}
    .section-title {{ font-size:16px; font-weight:700; margin:16px 0 8px; }}
  </style>
</head>
<body>
  <div class="header">
    {logo_tag}
    <h1 class="h1">AYS Report</h1>
  </div>

  <div class="card">
    <div class="kv"><b>Original Subject:</b> {original_subject}</div>
    <div class="kv"><b>AYS ID:</b> {ays_id}</div>
    <div class="kv"><b>Total Keywords:</b> {total_keywords}</div>
    <div class="kv"><b>Manufacturers (count):</b> {len(mfg_rows)}</div>
    <div class="kv"><b>Competitors (count):</b> {len(comp_rows)}</div>
    <div class="kv"><b>Recommendation:</b> <b>{recommendation}</b></div>
  </div>

  <div class="section-title">Manufacturer Terms</div>
  {mfg_table_html}

  <div class="section-title">Competitor Terms</div>
  {comp_table_html}

  <p class="muted">Best regards,<br>The AYS Team</p>
</body>
</html>"""

# =========================
# File-explorer helpers
# =========================
def _clean_prefix(raw: str | None) -> str:
    """
    Normalize a client-provided prefix into a safe S3 prefix under BROWSER_ROOT.
    Removes leading slashes, prevents '..' traversal, ensures trailing slash.
    """
    raw = (raw or "").strip().lstrip("/\\").replace("..", "")
    if not raw or not raw.startswith(BROWSER_ROOT):
        norm = BROWSER_ROOT
    else:
        norm = raw
    if not norm.endswith("/"):
        norm += "/"
    return norm

def is_allowed_key(key: str) -> bool:
    """Only allow keys under the explorer root."""
    return key.startswith(BROWSER_ROOT)

def s3_list_dir(prefix: str) -> dict:
    """
    List a 'directory' (prefix) like a file explorer:
      - folders: list of {name, prefix}
      - files:   list of {name, key, size, last_modified, content_type}
      - breadcrumbs for UI
    """
    prefix = _clean_prefix(prefix)
    paginator = _s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/")

    folders, files = [], []
    for page in pages:
        for cp in page.get("CommonPrefixes", []):
            p = cp.get("Prefix")
            if not p:
                continue
            name = p.rstrip("/").split("/")[-1]
            folders.append({"name": name, "prefix": p})

        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            name = key[len(prefix):]
            if "/" in name:  # deeper children will appear as CommonPrefixes
                continue
            files.append({
                "name": name,
                "key": key,
                "size": obj.get("Size", 0),
                "last_modified": (obj.get("LastModified") or datetime.utcnow()).isoformat(),
                "content_type": mimetypes.guess_type(name)[0] or "application/octet-stream",
            })

    # breadcrumbs
    parts = prefix.rstrip("/").split("/")
    crumbs, walk = [], []
    for part in parts:
        if not part:
            continue
        walk.append(part)
        pfx = "/".join(walk) + "/"
        crumbs.append({"label": part, "prefix": pfx if pfx.startswith(BROWSER_ROOT) else BROWSER_ROOT})

    return {
        "prefix": prefix,
        "breadcrumbs": crumbs,
        "folders": folders,
        "files": files,
        "bucket": S3_BUCKET,
        "root": BROWSER_ROOT,
    }

# =========================
# Core pipeline (per document) + strict cleanup
# =========================
def run_pipeline_to_s3(
    job_id: str,
    payload: Dict[str, Any],
    callbacks: Dict[str, Any],
    upload_folder: str,
    processed_folder: str,
) -> Dict[str, Any]:
    with _global_job_lock():
        # everything below runs while the global lock is held
        process_pdf_file = callbacks["process_pdf_file"]
        create_highlighted_only_pdf = callbacks["create_highlighted_only_pdf"]
        generate_email_body_cb = callbacks["generate_email_body"]
        logo_base64 = callbacks["logo_base64"]
        write_results_to_excel = callbacks.get("write_results_to_excel")

        project_id = payload.get("ProjectID") or make_project_id(payload.get("Subject", "Project"))
        original_subject = payload.get("Subject") or "Untitled Project"
        secure_name = secure_filename(payload.get("AttachmentName") or "document.pdf")
        doc_folder = slugify(Path(secure_name).stem) or "doc"
        project_slug = slugify(original_subject)

        # Per-job scratch
        work_root = os.path.join(processed_folder, f"job-{job_id}")
        os.makedirs(work_root, exist_ok=True)
        raw_local = os.path.join(work_root, secure_name)

        highlighted_local_orig = None
        tables_local = None
        high_full_local = None
        only_high_local = None
        email_html_local = None
        email_pdf_local = None
        zip_local = None

        try:
            # Save input
            with open(raw_local, "wb") as f:
                f.write(base64.b64decode(payload["AttachmentContent"]))

            # Heavy processing
            results = process_pdf_file(raw_local)
            if not results or not results.get("results"):
                raise RuntimeError("Processing failed.")

            highlighted_name = results.get("filename")
            if not highlighted_name:
                raise RuntimeError("Highlighted PDF missing.")
            highlighted_local_orig = os.path.join(processed_folder, highlighted_name)
            if not os.path.isfile(highlighted_local_orig):
                raise RuntimeError("Highlighted PDF not found on disk.")

            # Derive names
            base_name = f"{project_slug}_{doc_folder}"

            # Derived PDFs in scratch
            only_high_local = os.path.join(work_root, f"{base_name}_only_highlights.pdf")
            create_highlighted_only_pdf(highlighted_local_orig, results, only_high_local)

            high_full_local = os.path.join(work_root, f"{base_name}_highlighted.pdf")
            shutil.copy2(highlighted_local_orig, high_full_local)

            # Optional tables
            if write_results_to_excel:
                excel_local = os.path.join(work_root, f"tables_{doc_folder}.xlsx")
                write_results_to_excel(results, excel_local)
                tables_local = os.path.join(work_root, f"{base_name}_tables.xlsx")
                shutil.copy2(excel_local, tables_local)

            # Build email summary (HTML+PDF)
            results_data = results.get("results", {})
            total_keywords = sum(len(v) for v in results_data.values() if v)

            def _extract_rows(bucket: str) -> list[dict]:
                out = []
                for r in (results_data.get(bucket) or []):
                    if isinstance(r, dict):
                        out.append({
                            "Word": str(r.get("Word", "")),
                            "Page": r.get("Page", ""),
                            "Section": r.get("Section", ""),
                            "Section Name": r.get("Section Name", ""),
                        })
                return out

            manufacturer_rows = _extract_rows("manufacturer")
            competitor_rows   = _extract_rows("competitor")

            has_mfg, has_comp = bool(manufacturer_rows), bool(competitor_rows)
            if has_mfg and has_comp:
                recommendation, subject_summary = ("You and your competitor are specified. Bid this opportunity!", "Specified - Bid!")
            elif has_mfg:
                recommendation, subject_summary = ("You are Specified! Bid this opportunity!", "Specified - Bid!")
            elif has_comp:
                recommendation, subject_summary = ("Your competitor is specified - Review this opportunity.", "Competitor Specified")
            else:
                recommendation, subject_summary = ("You are not specified. Pass on this opportunity", "Not Specified - Do not Bid!")

            ays_id = f"AYS-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

            # S3 prefixes
            upload_key    = s3_key(S3_UPLOAD_PREFIX, project_id, doc_folder, "original.pdf")
            result_prefix = s3_key(S3_RESULTS_PREFIX, project_id, doc_folder)
            highlighted_s3_key = s3_key(result_prefix, f"{base_name}_highlighted.pdf")

            # Email HTML (with per-row links)
            email_html_local = os.path.join(work_root, f"{base_name}_email_summary.html")
            email_body_html = generate_email_body_cb(
                original_subject,
                total_keywords,
                manufacturer_rows,
                competitor_rows,
                recommendation,
                logo_base64,
                ays_id,
                highlighted_pdf_key=highlighted_s3_key,
            )
            with open(email_html_local, "w", encoding="utf-8") as f:
                f.write(email_body_html)

            # PDF version of the email
            from xhtml2pdf import pisa
            email_pdf_local = os.path.join(work_root, f"{base_name}_email_summary.pdf")
            with open(email_pdf_local, "wb") as f:
                pisa.CreatePDF(email_body_html, dest=f)

            # ZIP bundle
            zip_local = os.path.join(work_root, f"{base_name}_report.zip")
            with zipfile.ZipFile(zip_local, "w") as zipf:
                zipf.write(high_full_local,   arcname=os.path.basename(high_full_local))
                zipf.write(only_high_local,   arcname=os.path.basename(only_high_local))
                if tables_local and os.path.isfile(tables_local):
                    zipf.write(tables_local, arcname=os.path.basename(tables_local))
                zipf.write(email_pdf_local,   arcname=os.path.basename(email_pdf_local))
                zipf.write(email_html_local,  arcname=os.path.basename(email_html_local))

            # Upload S3 artifacts
            s3_upload_file(raw_local, upload_key)

            def _put(local_path: str, name: str) -> Dict[str, str]:
                key = s3_key(result_prefix, name)
                s3_upload_file(local_path, key)
                return {"key": key, "url": s3_presign_get(key, expires=3600)}

            s3_objects: Dict[str, Dict[str, str]] = {}
            s3_objects[os.path.basename(high_full_local)]  = _put(high_full_local,  os.path.basename(high_full_local))
            s3_objects[os.path.basename(only_high_local)]  = _put(only_high_local,  os.path.basename(only_high_local))
            if tables_local and os.path.isfile(tables_local):
                s3_objects[os.path.basename(tables_local)] = _put(tables_local, os.path.basename(tables_local))
            s3_objects[os.path.basename(email_pdf_local)]  = _put(email_pdf_local,  os.path.basename(email_pdf_local))
            s3_objects[os.path.basename(email_html_local)] = _put(email_html_local, os.path.basename(email_html_local))
            s3_objects[os.path.basename(zip_local)]        = _put(zip_local,        os.path.basename(zip_local))

            zip_key = s3_objects[os.path.basename(zip_local)]["key"]

            return {
                "job_id": job_id,
                "project_id": project_id,
                "doc_folder": doc_folder,
                "upload_key": upload_key,
                "result_prefix": result_prefix,
                "files": s3_objects,
                "zip_key": zip_key,
                "zip_url": s3_objects[os.path.basename(zip_local)]["url"],
                "ays_id": ays_id,
                "manufacturer_terms": [r.get("Word", "") for r in manufacturer_rows if r.get("Word")],
                "recommendation": subject_summary,
            }

        finally:
            # cleanup happens while still under the lock, so scratch dirs don't collide
            _safe_unlink(highlighted_local_orig)
            _safe_rmtree(work_root)

        
# --- Customer/CSV export columns (for dashboard downloads) ---
CUSTOMER_EXPORT_COLUMNS = [
    "Date",
    "AYS ID",
    "Email",
    "Project Name",
    "Manufacturer Terms",
    "Recommendation",
]


def customer_export_df(df):
    """Return only customer-visible columns in a stable order (missing cols become empty)."""
    cols = []
    for c in CUSTOMER_EXPORT_COLUMNS:
        if c in df.columns:
            cols.append(c)
        else:
            df[c] = ""   # create empty if missing
            cols.append(c)
    return df[cols]

# helpers_async_s3_0_6.py  (top of file)
import os, fcntl, time, logging
from contextlib import contextmanager

_LOCK_PATH = os.environ.get("AYS_LOCK_FILE", "/awsays/run/ays_pipeline.lock")
os.makedirs(os.path.dirname(_LOCK_PATH), exist_ok=True)







