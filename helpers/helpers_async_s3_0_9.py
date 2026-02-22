# helpers_async_s3_0_9.py
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
import io
import re
import os
from urllib.parse import quote_plus

import boto3
import pandas as pd
from PyPDF2 import PdfReader, PdfWriter
import os
import logging

import boto3
from botocore.config import Config as BotoConfig
from flask import current_app, jsonify


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
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "ays-artifacts-betatwo")
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
_s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    config=BotoConfig(s3={"addressing_style": "virtual"}),
)
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
    _s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=data,
        ContentType=ct or "application/octet-stream",
    )


def s3_presign_get(key: str, expires: int = 3600, extra: dict | None = None) -> str:
    """
    Presign S3 GET. If extra is provided, it can include:
      ResponseContentType, ResponseContentDisposition, etc.
    """
    params = {"Bucket": S3_BUCKET, "Key": key}
    if extra:
        params.update(extra)
    return _s3.generate_presigned_url(
        "get_object", Params=params, ExpiresIn=expires
    )


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
def slugify(s: str, max_length: int = 80) -> str:
    """
    Turn an arbitrary string into a filesystem/S3-safe slug.

    - Removes non-word characters
    - Collapses whitespace/dashes to single underscores
    - Trims to max_length
    """
    s = (s or "").strip()
    if not s:
        return "project"

    # keep letters, numbers, underscore, whitespace, and dash
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    # collapse spaces/dashes to single underscores
    s = re.sub(r"[-\s]+", "_", s).strip("_")
    if not s:
        s = "project"

    return s[:max_length]


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
        for fmt in (
            "%m/%d/%Y %H:%M",
            "%m/%d/%Y",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ):
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
    meta_fields: dict | None = None,
    pages_processed: int | None = None,
    attachment_name: str | None = None,  # NEW: user-facing file name
) -> None:
    """
    Append a finished-document row to the dashboard xlsx.

    Visible columns: Date, AYS ID, Attachment Name, Email, Project Name,
                     Manufacturer Terms, Recommendation, Download URL, etc.
    Internal columns: Project ID, Doc Folder, S3 Zip Key, Job ID, Submitted At
    """
    os.makedirs(os.path.dirname(DASHBOARD_XLSX), exist_ok=True)

    # display date (no time) and stored ISO for stable sorting
    date_str = _normalize_date_only(submitted_at)  # "MM/DD/YYYY"
    submitted_at_iso = (
        submitted_at
        or datetime.utcnow().isoformat(timespec="seconds") + "Z"
    )

    new_row = {
        "Date": date_str,
        "AYS ID": ays_id or "",
        "Attachment Name": attachment_name or "",  # NEW: clean display name
        "Email": from_email or "",
        "Project Name": project_name or "",
        "Manufacturer Terms": _normalize_mfg_terms(manufacturer_terms),
        "Recommendation": recommendation or "",
        "Pages Processed": pages_processed if pages_processed is not None else "",
        "Download URL": f"/dl/{job_id}" if job_id else "",
        # internal
        "Project ID": project_id or "",
        "Doc Folder": doc_folder or "",
        "S3 Zip Key": zip_key or "",
        "Job ID": job_id or "",
        "Submitted At": submitted_at_iso,
    }

    # Expand meta_* fields into user-friendly columns
    meta_fields = meta_fields or {}
    for k, v in meta_fields.items():
        # e.g. meta_bid_date -> "Bid Date"
        col_name = k.replace("meta_", "").replace("_", " ").title()
        new_row[col_name] = v

    visible_cols = [
        "Date",
        "AYS ID",
        "Attachment Name",   # NEW visible column
        "Email",
        "Project Name",
        "Bid Date",
        "Drawing Date",
        "Address",
        "Engineer",
        "General Contractor",
        "Notes",
        "Pages Processed",
        "Manufacturer Terms",
        "Recommendation",
        "Download URL",
    ]
    internal_cols = [
        "Project ID",
        "Doc Folder",
        "S3 Zip Key",
        "Job ID",
        "Submitted At",
    ]
    all_cols = visible_cols + internal_cols

    with _DASHBOARD_LOCK:
        if os.path.exists(DASHBOARD_XLSX):
            df = pd.read_excel(DASHBOARD_XLSX, dtype=str).fillna("")
            # make sure all expected columns exist
            for c in all_cols:
                if c not in df.columns:
                    df[c] = ""
            df = df[all_cols]
            df = pd.concat(
                [df, pd.DataFrame([new_row])], ignore_index=True
            )
        else:
            df = pd.DataFrame([new_row], columns=all_cols)

        df.to_excel(DASHBOARD_XLSX, index=False)


# =========================
# Explorer index helpers (newest-first)
# =========================
def _parse_dt_any(s: str) -> datetime:
    if not s:
        return datetime.fromtimestamp(0)
    for fmt in (
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
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

    latest = (
        df.sort_values(["Project ID", "__submitted"])
        .groupby("Project ID", as_index=False)
        .tail(1)
    )

    out: dict[str, dict] = {}
    for _, r in latest.iterrows():
        pid = str(r.get("Project ID", "")).strip()
        if not pid:
            continue
        ts = r["__submitted"]
        sort_key = int(ts.timestamp()) if pd.notna(ts) else 0
        out[pid] = {
            "project_id": pid,
            "project_name": str(r.get("Project Name", "")).strip()
            or pid.split("_AYS-")[0].replace("_", " "),
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
        resp = _s3.list_objects_v2(
            Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/"
        )
    except Exception:
        logging.exception(
            "list_project_docs: list_objects_v2 failed (project root)"
        )
        return []

    folders = [
        cp.get("Prefix", "")
        for cp in resp.get("CommonPrefixes", [])
        if cp.get("Prefix")
    ]
    out: list[dict] = []

    for f in folders:
        # f like: results/<project_id>/<doc_folder>/
        doc_folder = f.rstrip("/").split("/")[-1]
        try:
            resp2 = _s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f)
        except Exception:
            logging.exception(
                "list_project_docs: list_objects_v2 failed (doc folder)"
            )
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
            elif name.endswith("_highlighted.pdf") or name.endswith(
                "highlighted.pdf"
            ):
                arts["highlighted"] = key
            elif name.endswith("_only_highlights.pdf") or name.endswith(
                "only_highlights.pdf"
            ):
                arts["only_highlights"] = key
            elif name.endswith("_email_summary.pdf") or name.endswith(
                "email_summary.pdf"
            ):
                arts["email_pdf"] = key
            elif name.endswith("_email_summary.html") or name.endswith(
                "email_summary.html"
            ):
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
        JOBS[job_id] = {
            "state": "QUEUED",
            "created": time.time(),
            "info": {},
        }
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
def format_html_table(
    rows,
    *,
    pdf_key: str | None = None,
    sections_index: dict | None = None,
) -> str:
    """
    Build HTML table for email + PDF.

    - If rows is empty, return simple 'No results.' text
      (exactly like the old behavior that never crashed).
    - Otherwise render a 7-column table:

        Word | Page | Section | Section Name | View Page | View Section | Save Section

    - If pdf_key is provided:
        - 'View Page' links to the exact page in the highlighted PDF.
    - If sections_index is provided:
        - 'View Section' / 'Save Section' link to the section span.

    This version avoids <colgroup> and any weird structure so xhtml2pdf
    always sees a normal table with a fixed number of columns.
    """
    rows = rows or []
    if not rows:
        # Old behavior: just a simple line of text instead of a table
        # when there are no results for that block.
        return "<p><em>No results.</em></p>"

    table_style = (
        "width:100%;"
        "border-collapse:collapse;"
        "font-size:14px;"
    )

    base_cell_wrap = (
        "word-wrap:break-word;"
        "overflow-wrap:break-word;"
        "white-space:normal;"
        "vertical-align:top;"
    )

    header_style = (
        "background:#ff8c00;color:#fff;"
        "border-bottom:1px solid #e5e7eb;"
        "padding:6px 8px;"
        "text-align:left;"
        + base_cell_wrap
    )
    cell_style = (
        "border-bottom:1px solid #f0f2f5;"
        "padding:6px 8px;"
        + base_cell_wrap
    )

    base = APP_PUBLIC_BASE or ""
    from urllib.parse import quote as urlquote

    def link_for_page(page):
        """Link to a specific page of the highlighted PDF."""
        if not pdf_key:
            return None
        try:
            p = int(str(page))
            return f"{base}/view/by-key?key={urlquote(pdf_key)}&page={p}"
        except Exception:
            return None

    def links_for_section(section_code):
        """
        Return (view_section_href, save_section_href) for the whole CSI section
        based on sections_index mapping: Section → (start_page, stop_page).
        """
        if not pdf_key or not sections_index:
            return None, None

        sec = str(section_code).strip()
        span = sections_index.get(sec)
        if not span:
            return None, None

        start_page, stop_page = span
        if not start_page:
            return None, None

        try:
            s = int(start_page)
            e = int(stop_page or start_page)
        except Exception:
            return None, None

        view_href = f"{base}/view/by-key?key={urlquote(pdf_key)}&page={s}"
        save_href = (
            f"{base}/section/dl?key={urlquote(pdf_key)}&start={s}&stop={e}"
        )
        return view_href, save_href

    # Build a *simple* table (no colgroup) with a fixed set of headers
    out = [
        f'<table style="{table_style}">',
        "<thead><tr>",
        f'<th style="{header_style}">Word</th>',
        f'<th style="{header_style}">Page</th>',
        f'<th style="{header_style}">Section</th>',
        f'<th style="{header_style}">Section Name</th>',
        f'<th style="{header_style}">View Page</th>',
        f'<th style="{header_style}">View Section</th>',
        f'<th style="{header_style}">Save Section</th>',
        "</tr></thead><tbody>",
    ]

    for r in rows:
        word = (r.get("Word") or "").strip()
        page = r.get("Page", "")
        section = r.get("Section", "")
        sname = r.get("Section Name", "")

        # Per-page link
        view_page_href = link_for_page(page)
        view_page_td = (
            f'<a href="{view_page_href}" target="_blank" rel="noopener">Open Page</a>'
            if view_page_href
            else ""
        )

        # Section-level links
        view_sec_href, save_sec_href = links_for_section(section)
        view_sec_td = (
            f'<a href="{view_sec_href}" target="_blank" rel="noopener">View Section</a>'
            if view_sec_href
            else ""
        )
        save_sec_td = (
            f'<a href="{save_sec_href}" target="_blank" rel="noopener">Save Section</a>'
            if save_sec_href
            else ""
        )

        out.append(
            "<tr>"
            f'<td style="{cell_style}">{word}</td>'
            f'<td style="{cell_style}">{page}</td>'
            f'<td style="{cell_style}">{section}</td>'
            f'<td style="{cell_style}">{sname}</td>'
            f'<td style="{cell_style}">{view_page_td}</td>'
            f'<td style="{cell_style}">{view_sec_td}</td>'
            f'<td style="{cell_style}">{save_sec_td}</td>'
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
    *,
    meta: dict | None = None,
    highlighted_pdf_key: str | None = None,
    sections: list[dict] | None = None,
) -> str:
    """
    Returns HTML summary.

    This is the *original* behavior with:
      - Section-level links (View Section / Save Section) using sections_index.
      - Page-level links (View Page) using highlighted_pdf_key.
      - Orange styling for headers.

    ONLY CHANGE: The project/meta card (Job Name, Bid Date, etc.)
    has been removed so the body is just:
      - Header
      - Manufacturer Terms table
      - Competitor Terms table
    """
    mfg_rows = manufacturer_rows or []
    comp_rows = competitor_rows or []
    meta = meta or {}
    sections = sections or []

    # --- Build section span index (same as before) ---
    sections_index: dict[str, tuple[int, int]] = {}

    for s in sections:
        sec = str(s.get("Section", "")).strip()
        if not sec:
            continue

        # tolerate slightly different key names for start/stop
        start = (
            s.get("Start Page")
            or s.get("Start")
            or s.get("Start_Page")
            or s.get("First Page")
        )
        stop = (
            s.get("Stop Page")
            or s.get("End Page")
            or s.get("Stop")
            or s.get("Last Page")
            or start
        )

        try:
            start_i = int(start)
        except Exception:
            # if we can't parse start, skip this row
            continue
        try:
            stop_i = int(stop)
        except Exception:
            stop_i = start_i

        if sec not in sections_index:
            sections_index[sec] = (start_i, stop_i)
        else:
            cur_start, cur_stop = sections_index[sec]
            sections_index[sec] = (min(cur_start, start_i), max(cur_stop, stop_i))

    # Logo (same as before)
    logo_tag = (
        f'<img alt="AYS" src="data:image/png;base64,{logo_base64}" '
        f'style="height:40px;vertical-align:middle;margin-right:10px" />'
    )

    # Tables with section-aware behavior (same idea as old version)
    mfg_table_html = format_html_table(
        mfg_rows,
        pdf_key=highlighted_pdf_key,
        sections_index=sections_index,
    )
    comp_table_html = format_html_table(
        comp_rows,
        pdf_key=highlighted_pdf_key,
        sections_index=sections_index,
    )

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
    .section-title {{ font-size:16px; font-weight:700; margin:16px 0 8px; color:#f97316; }}
    .muted {{ color:#555; }}
  </style>
</head>
<body>
  <div class="header">
    {logo_tag}
    <h1 class="h1">AYS Report</h1>
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
    pages = paginator.paginate(
        Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/"
    )

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
            name = key[len(prefix) :]
            if "/" in name:  # deeper children will appear as CommonPrefixes
                continue
            files.append(
                {
                    "name": name,
                    "key": key,
                    "size": obj.get("Size", 0),
                    "last_modified": (
                        obj.get("LastModified") or datetime.utcnow()
                    ).isoformat(),
                    "content_type": mimetypes.guess_type(name)[0]
                    or "application/octet-stream",
                }
            )

    # breadcrumbs
    parts = prefix.rstrip("/").split("/")
    crumbs, walk = [], []
    for part in parts:
        if not part:
            continue
        walk.append(part)
        pfx = "/".join(walk) + "/"
        crumbs.append(
            {
                "label": part,
                "prefix": pfx if pfx.startswith(BROWSER_ROOT) else BROWSER_ROOT,
            }
        )

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
        create_highlighted_only_pdf = callbacks[
            "create_highlighted_only_pdf"
        ]
        generate_email_body_cb = callbacks["generate_email_body"]
        logo_base64 = callbacks["logo_base64"]
        write_results_to_excel = callbacks.get("write_results_to_excel")

        # -------- NEW: normalize display name + submitted_at --------
        original_subject = payload.get("Subject") or "Untitled Project"

        # What the user "thinks" the file is called (for dashboard)
        display_name = (
            (payload.get("DisplayName") or payload.get("AttachmentName") or "document.pdf")
        ).strip()

        # When it was submitted (for folder uniqueness + dashboard)
        submitted_at = payload.get("SubmittedAt")

        project_id = payload.get("ProjectID") or make_project_id(original_subject)

        # On-disk safe filename (this is what we actually write to disk)
        secure_name = secure_filename(
            payload.get("AttachmentName") or "document.pdf"
        )

        # Internal doc slug for S3 folder: includes timestamp so duplicates never collide
        # e.g. "24_0039_-_Copy_20251208-013858"
        doc_folder = build_internal_doc_slug(display_name, submitted_at=submitted_at)

        # Project slug used in derived artifact names
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

            # Count pages up front
            pages_processed = get_page_count(raw_local)

            # Heavy processing
            results = process_pdf_file(raw_local)
            if not results or not results.get("results"):
                raise RuntimeError("Processing failed.")

            highlighted_name = results.get("filename")
            if not highlighted_name:
                raise RuntimeError("Highlighted PDF missing.")
            highlighted_local_orig = os.path.join(
                processed_folder, highlighted_name
            )
            if not os.path.isfile(highlighted_local_orig):
                raise RuntimeError("Highlighted PDF not found on disk.")

            # Derive names
            base_name = f"{project_slug}_{doc_folder}"

            # Derived PDFs in scratch
            only_high_local = os.path.join(
                work_root, f"{base_name}_only_highlights.pdf"
            )
            create_highlighted_only_pdf(
                highlighted_local_orig, results, only_high_local
            )

            high_full_local = os.path.join(
                work_root, f"{base_name}_highlighted.pdf"
            )
            shutil.copy2(highlighted_local_orig, high_full_local)

            # Optional tables
            if write_results_to_excel:
                excel_local = os.path.join(
                    work_root, f"tables_{doc_folder}.xlsx"
                )
                write_results_to_excel(results, excel_local)
                tables_local = os.path.join(
                    work_root, f"{base_name}_tables.xlsx"
                )
                shutil.copy2(excel_local, tables_local)

            # Build email summary (HTML+PDF)
            results_data = results.get("results", {})
            total_keywords = sum(
                len(v) for v in results_data.values() if v
            )

            def _extract_rows(bucket: str) -> list[dict]:
                out = []
                for r in (results_data.get(bucket) or []):
                    if isinstance(r, dict):
                        out.append(
                            {
                                "Word": str(r.get("Word", "")),
                                "Page": r.get("Page", ""),
                                "Section": r.get("Section", ""),
                                "Section Name": r.get("Section Name", ""),
                            }
                        )
                return out

            manufacturer_rows = _extract_rows("manufacturer")
            competitor_rows = _extract_rows("competitor")

            has_mfg, has_comp = bool(manufacturer_rows), bool(
                competitor_rows
            )
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

            # S3 prefixes – doc_folder is now timestamped & unique per submission
            upload_key = s3_key(
                S3_UPLOAD_PREFIX, project_id, doc_folder, "original.pdf"
            )
            result_prefix = s3_key(S3_RESULTS_PREFIX, project_id, doc_folder)
            highlighted_s3_key = s3_key(
                result_prefix, f"{base_name}_highlighted.pdf"
            )

            # Email HTML (with per-row links + customer project metadata)
            import sys

            sections_raw = results.get("sections")
            print(
                f"🔥 SECTIONS BEFORE EMAIL GENERATION:\n{sections_raw}",
                file=sys.stderr,
            )
            sys.stderr.flush()

            email_html_local = os.path.join(
                work_root, f"{base_name}_email_summary.html"
            )
            email_body_html = generate_email_body_cb(
                original_subject,
                total_keywords,
                manufacturer_rows,
                competitor_rows,
                recommendation,
                logo_base64,
                ays_id,
                meta=payload.get("Meta") or {},
                highlighted_pdf_key=highlighted_s3_key,
                sections=sections_raw or [],  # already extracted above
            )
            with open(email_html_local, "w", encoding="utf-8") as f:
                f.write(email_body_html)

            print(
                f"✅ Email summary written to: {email_html_local}",
                file=sys.stderr,
            )
            sys.stderr.flush()

            # PDF version of the email
            from xhtml2pdf import pisa

            email_pdf_local = os.path.join(
                work_root, f"{base_name}_email_summary.pdf"
            )
            with open(email_pdf_local, "wb") as f:
                pisa.CreatePDF(email_body_html, dest=f)

            # ZIP bundle
            zip_local = os.path.join(work_root, f"{base_name}_report.zip")
            with zipfile.ZipFile(zip_local, "w") as zipf:
                zipf.write(
                    high_full_local,
                    arcname=os.path.basename(high_full_local),
                )
                zipf.write(
                    only_high_local,
                    arcname=os.path.basename(only_high_local),
                )
                if tables_local and os.path.isfile(tables_local):
                    zipf.write(
                        tables_local,
                        arcname=os.path.basename(tables_local),
                    )
                zipf.write(
                    email_pdf_local,
                    arcname=os.path.basename(email_pdf_local),
                )
                zipf.write(
                    email_html_local,
                    arcname=os.path.basename(email_html_local),
                )

            # Upload S3 artifacts
            s3_upload_file(raw_local, upload_key)

            def _put(local_path: str, name: str) -> Dict[str, str]:
                key = s3_key(result_prefix, name)
                s3_upload_file(local_path, key)
                return {
                    "key": key,
                    "url": s3_presign_get(key, expires=3600),
                }

            s3_objects: Dict[str, Dict[str, str]] = {}
            s3_objects[os.path.basename(high_full_local)] = _put(
                high_full_local, os.path.basename(high_full_local)
            )
            s3_objects[os.path.basename(only_high_local)] = _put(
                only_high_local, os.path.basename(only_high_local)
            )
            if tables_local and os.path.isfile(tables_local):
                s3_objects[os.path.basename(tables_local)] = _put(
                    tables_local, os.path.basename(tables_local)
                )
            s3_objects[os.path.basename(email_pdf_local)] = _put(
                email_pdf_local, os.path.basename(email_pdf_local)
            )
            s3_objects[os.path.basename(email_html_local)] = _put(
                email_html_local, os.path.basename(email_html_local)
            )
            s3_objects[os.path.basename(zip_local)] = _put(
                zip_local, os.path.basename(zip_local)
            )

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
                "manufacturer_terms": [
                    r.get("Word", "") for r in manufacturer_rows if r.get("Word")
                ],
                "recommendation": subject_summary,
                "pages_processed": pages_processed,
                "sections": results.get("sections", []),  # keep passing through
                # NEW: pass through for dashboard logging
                "display_name": display_name,
                "submitted_at": submitted_at,
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
            df[c] = ""  # create empty if missing
            cols.append(c)
    return df[cols]

def update_project_meta_row(
    *,
    project_id: str,
    project_name: str | None = None,
    email: str | None = None,
    meta_fields: dict | None = None,
) -> bool:
    """
    Update project-level info (Project Name, Email, meta_* fields) for all rows
    belonging to a given Project ID in the dashboard workbook.

    Returns True if at least one row was updated, False otherwise.
    """
    if not project_id:
        return False

    if not os.path.exists(DASHBOARD_XLSX):
        # Nothing to update yet
        return False

    try:
        df = pd.read_excel(DASHBOARD_XLSX, dtype=str).fillna("")
    except Exception as e:
        logging.error(f"update_project_meta_row: failed to read dashboard: {e}")
        return False

    if "Project ID" not in df.columns:
        return False

    mask = df["Project ID"].astype(str) == str(project_id)
    if not mask.any():
        # No rows for this Project ID
        return False

    # Update basic project fields if provided
    if project_name is not None:
        df.loc[mask, "Project Name"] = project_name

    if email is not None:
        df.loc[mask, "Email"] = email

    # Update meta_* → visible columns (Bid Date, Address, Engineer, etc.)
    meta_fields = meta_fields or {}
    for k, v in meta_fields.items():
        col_name = k.replace("meta_", "").replace("_", " ").title()
        if col_name not in df.columns:
            df[col_name] = ""
        df.loc[mask, col_name] = v

    try:
        df.to_excel(DASHBOARD_XLSX, index=False)
        return True
    except Exception as e:
        logging.error(f"update_project_meta_row: failed to write dashboard: {e}")
        return False

def delete_project_doc_s3(project_id: str, doc_folder: str) -> dict:
    """
    Delete all S3 objects for a given document under:
        results/<project_id>/<doc_folder>/

    Returns:
        {
          "prefix": "<results/.../>",
          "deleted_count": int,
          "errors": [ ... ]
        }
    """
    if not project_id or not doc_folder:
        raise ValueError("project_id and doc_folder are required")

    # This matches how list_project_docs builds the folder prefix:
    #   f"results/<project_id>/<doc_folder>/..."
    prefix = s3_key(S3_RESULTS_PREFIX, project_id, doc_folder) + "/"

    deleted = []
    errors = []

    logging.info(
        "delete_project_doc_s3: starting delete; project_id=%s doc_folder=%s prefix=%s",
        project_id, doc_folder, prefix,
    )

    try:
        continuation_token = None

        while True:
            list_kwargs = {
                "Bucket": S3_BUCKET,
                "Prefix": prefix,
            }
            if continuation_token:
                list_kwargs["ContinuationToken"] = continuation_token

            resp = _s3.list_objects_v2(**list_kwargs)
            contents = resp.get("Contents", [])

            if not contents:
                # no more objects under this prefix
                break

            # Prepare batch delete list
            objects = [{"Key": obj["Key"]} for obj in contents if obj.get("Key")]
            if not objects:
                break

            # S3 DeleteObjects supports up to 1000 per call, chunk just in case
            for i in range(0, len(objects), 1000):
                chunk = {"Objects": objects[i:i + 1000], "Quiet": True}
                del_resp = _s3.delete_objects(Bucket=S3_BUCKET, Delete=chunk)
                deleted.extend([d["Key"] for d in del_resp.get("Deleted", [])])
                errors.extend(del_resp.get("Errors", []))

            if resp.get("IsTruncated"):
                continuation_token = resp.get("NextContinuationToken")
            else:
                break

        logging.info(
            "delete_project_doc_s3: finished; project_id=%s doc_folder=%s prefix=%s deleted=%d errors=%d",
            project_id, doc_folder, prefix, len(deleted), len(errors),
        )

        return {
            "prefix": prefix,
            "deleted_count": len(deleted),
            "errors": errors,
        }

    except Exception as exc:
        logging.exception(
            "delete_project_doc_s3: exception deleting doc; project_id=%s doc_folder=%s",
            project_id, doc_folder,
        )
        raise

def get_usage_stats() -> dict:
    """
    Returns aggregate usage stats:
      - total_submissions: number of dashboard rows
      - total_pages: sum of 'Pages Processed' column
    """
    if not os.path.exists(DASHBOARD_XLSX):
        return {"total_submissions": 0, "total_pages": 0}

    try:
        df = pd.read_excel(DASHBOARD_XLSX, dtype=str).fillna("")
    except Exception as e:
        logging.error(f"get_usage_stats: failed to read dashboard: {e}")
        return {"total_submissions": 0, "total_pages": 0}

    total_sub = len(df)

    if "Pages Processed" in df.columns:
        pages = pd.to_numeric(df["Pages Processed"], errors="coerce").fillna(0).sum()
    else:
        pages = 0

    return {
        "total_submissions": int(total_sub),
        "total_pages": int(pages),
    }

def get_page_count(pdf_path: str) -> int:
    """
    Safely get the number of pages in a PDF.
    Returns 0 on any error.
    """
    try:
        if not pdf_path or not os.path.isfile(pdf_path):
            return 0
        reader = PdfReader(pdf_path)
        return len(reader.pages)
    except Exception as e:
        logging.warning(f"get_page_count: failed for {pdf_path}: {e}")
        return 0

import re
import boto3

s3 = boto3.client("s3")

# helpers_async_s3_0_9.py (or whatever your helper module is)
import os

def get_next_available_name_local(base_name: str, ext: str, upload_folder: str) -> str:
    """
    Ensure a unique filename in upload_folder.

    Produces names like:
      24-0039 - Copy.pdf
      24-0039 - Copy - 2.pdf
      24-0039 - Copy - 3.pdf
    """
    base_name = (base_name or "").strip()
    if not base_name:
        base_name = "file"

    # First candidate: plain name
    candidate = f"{base_name}{ext}"
    full_path = os.path.join(upload_folder, candidate)
    if not os.path.exists(full_path):
        return candidate

    # Numbered variants: use ' - 2', ' - 3', etc.
    counter = 2
    while True:
        candidate = f"{base_name} - {counter}{ext}"
        full_path = os.path.join(upload_folder, candidate)
        if not os.path.exists(full_path):
            return candidate
        counter += 1


def build_internal_doc_slug(attachment_name: str, submitted_at: str | None = None) -> str:
    """
    Build a unique, S3-safe doc slug from the user-facing filename + timestamp.

    - attachment_name: e.g. '24-0039 - Copy.pdf'
    - submitted_at: ISO string or None. If None, use UTC now.

    Returns something like: '24_0039_-_Copy_20251208-013858'
    """
    if not attachment_name:
        attachment_name = "document.pdf"

    base, _ = os.path.splitext(attachment_name)
    base = (base or "document").strip()

    # Slugify the base – keep it predictable / URL-safe.
    slug_base = slugify(base, max_length=80) or "document"

    # Use submitted_at if provided; fallback to now.
    try:
        if submitted_at:
            dt = datetime.fromisoformat(submitted_at.replace("Z", ""))
        else:
            dt = datetime.utcnow()
    except Exception:
        dt = datetime.utcnow()

    ts = dt.strftime("%Y%m%d-%H%M%S")

    return f"{slug_base}_{ts}"










