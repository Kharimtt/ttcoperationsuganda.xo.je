"""
TT COPERATIONS UGANDA — Video Downloader Backend
FastAPI + yt-dlp, hardened for small free-tier hosts (Render, Railway, etc.)

Changes:
  • Duration limit REMOVED — any video length allowed.
  • File-size limit REMAINS (default 2000 MB) as the real guard.
  • Progress wording: user always sees "Processing…" not "Downloading…".
  • All earlier hardening preserved (semaphore, disk check, rate limit,
    background cleanup, silent yt-dlp, sanitized errors).
"""

import os
import sys
import time
import uuid
import shutil
import threading
import contextlib
import io
from pathlib import Path
from collections import defaultdict, deque

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
import yt_dlp

# ============================================================
#  CONFIG — env-tunable
# ============================================================
DOWNLOAD_DIR             = Path(os.environ.get("DOWNLOAD_DIR", "/tmp/downloads"))
MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", 2))
MAX_FILE_SIZE_MB         = int(os.environ.get("MAX_FILE_SIZE_MB", 2000))     # 2 GB
MAX_HEIGHT               = int(os.environ.get("MAX_HEIGHT", 1080))
JOB_TTL_SECONDS          = int(os.environ.get("JOB_TTL_SECONDS", 60 * 60))   # 1 hour
CLEANUP_INTERVAL_SECONDS = int(os.environ.get("CLEANUP_INTERVAL_SECONDS", 10 * 60))
MIN_FREE_DISK_MB         = int(os.environ.get("MIN_FREE_DISK_MB", 500))
MAX_ACTIVE_JOBS          = int(os.environ.get("MAX_ACTIVE_JOBS", 50))
RATE_LIMIT_PER_MINUTE    = int(os.environ.get("RATE_LIMIT_PER_MINUTE", 10))
ALLOWED_ORIGIN           = os.environ.get("ALLOWED_ORIGIN", "*")

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
#  JOB STATE
# ============================================================
jobs = {}
jobs_lock = threading.Lock()
download_semaphore = threading.Semaphore(MAX_CONCURRENT_DOWNLOADS)

_rate_limit_hits = defaultdict(deque)
_rate_limit_lock = threading.Lock()

# ============================================================
#  APP
# ============================================================
app = FastAPI(title="TT COPERATIONS Downloader")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[ALLOWED_ORIGIN] if ALLOWED_ORIGIN != "*" else ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

# ============================================================
#  USER-SAFE MESSAGES
# ============================================================
MSG_INVALID_URL  = "That link doesn't look right. Please paste a valid video URL."
MSG_UNSUPPORTED  = "Sorry, this video can't be downloaded right now. Please try a different link or platform."
MSG_NOT_FOUND    = "We couldn't find that video. Please check the link and try again."
MSG_NETWORK      = "The server is busy. Please try again in a moment."
MSG_GENERIC      = "Something went wrong. Please try again."
MSG_JOB_GONE     = "This download has expired. Please start a new one."
MSG_FILE_MISSING = "The file is no longer available. Please start a new download."
MSG_BUSY         = "The server is handling too many requests. Please wait a moment and try again."
MSG_DISK_FULL    = "The server is low on storage right now. Please try again in a few minutes."
MSG_RATE_LIMITED = "You're sending requests too quickly. Please wait a minute and try again."

def too_large_msg() -> str:
    return f"This file is larger than our {MAX_FILE_SIZE_MB}MB limit. Please try a lower quality."

class TooLarge(Exception):  pass
class DiskFull(Exception):  pass

def log_error(exc: Exception):
    print(f"[ERROR] {type(exc).__name__}: {exc}", flush=True)

def friendly_error(exc: Exception) -> str:
    if isinstance(exc, TooLarge):
        log_error(exc); return too_large_msg()
    if isinstance(exc, DiskFull):
        log_error(exc); return MSG_DISK_FULL

    log_error(exc)
    text = str(exc).lower()

    if any(k in text for k in (
        "unsupported", "unable to extract", "unable to download",
        "private", "sign in", "age", "not available",
        "no video", "video unavailable", "unexpected response",
    )):
        return MSG_UNSUPPORTED
    if any(k in text for k in ("not found", "404", "does not exist")):
        return MSG_NOT_FOUND
    if any(k in text for k in ("timed out", "timeout", "connection", "network")):
        return MSG_NETWORK
    if any(k in text for k in ("too many requests", "429", "rate limit")):
        return MSG_BUSY
    if any(k in text for k in ("no space left", "disk full", "enospc")):
        return MSG_DISK_FULL
    return MSG_GENERIC

# ============================================================
#  SILENCE yt-dlp
# ============================================================
@contextlib.contextmanager
def silence_ytdlp():
    old_out, old_err = sys.stdout, sys.stderr
    sys.stdout, sys.stderr = io.StringIO(), io.StringIO()
    try:
        yield
    finally:
        sys.stdout, sys.stderr = old_out, old_err

def ydl_opts(skip_download=True, extra=None):
    opts = {
        "quiet": True,
        "no_warnings": True,
        "no_color": True,
        "skip_download": skip_download,
        "noprogress": True,
        "logger": None,
        "socket_timeout": 60,
        "retries": 5,
        "fragment_retries": 5,
        "extractor_args": {
            "youtube": {
                "player_client": ["android", "ios", "web_safari", "web"],
            }
        },
    }
    if extra:
        opts.update(extra)
    return opts

# ============================================================
#  UTILITIES
# ============================================================
def get_client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"

def rate_limited(ip: str) -> bool:
    now = time.time()
    with _rate_limit_lock:
        hits = _rate_limit_hits[ip]
        while hits and now - hits[0] > 60:
            hits.popleft()
        if len(hits) >= RATE_LIMIT_PER_MINUTE:
            return True
        hits.append(now)
        return False

def enough_disk_space(min_free_mb: int = MIN_FREE_DISK_MB) -> bool:
    try:
        free_bytes = shutil.disk_usage(DOWNLOAD_DIR).free
        return (free_bytes / (1024 * 1024)) >= min_free_mb
    except Exception:
        return True

def touch_job(job_id, **updates):
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id].update(updates)
            jobs[job_id]["_updated"] = time.time()

# ============================================================
#  GLOBAL EXCEPTION HANDLER
# ============================================================
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    log_error(exc)
    return JSONResponse(status_code=500, content={"error": MSG_GENERIC})

# ============================================================
#  CLEANUP
# ============================================================
def cleanup_old_files(max_age_seconds=JOB_TTL_SECONDS):
    now = time.time()
    removed = 0
    for f in DOWNLOAD_DIR.iterdir():
        if f.is_file() and (now - f.stat().st_mtime) > max_age_seconds:
            try:
                f.unlink(); removed += 1
            except Exception:
                pass
    if removed:
        print(f"[CLEANUP] Removed {removed} stale file(s)")

def cleanup_stale_jobs():
    now = time.time()
    with jobs_lock:
        stale = [
            jid for jid, j in jobs.items()
            if (now - j.get("_updated", now)) > JOB_TTL_SECONDS
        ]
        for jid in stale:
            jobs.pop(jid, None)
            for f in DOWNLOAD_DIR.iterdir():
                if f.name.startswith(jid):
                    try:
                        f.unlink()
                    except Exception:
                        pass
    if stale:
        print(f"[CLEANUP] Dropped {len(stale)} stale job record(s)")

def cleanup_loop():
    while True:
        try:
            cleanup_old_files()
            cleanup_stale_jobs()
        except Exception as e:
            log_error(e)
        time.sleep(CLEANUP_INTERVAL_SECONDS)

cleanup_old_files()
threading.Thread(target=cleanup_loop, daemon=True).start()

# ============================================================
#  REQUEST MODELS
# ============================================================
class InfoRequest(BaseModel):
    url: str

class DownloadRequest(BaseModel):
    url: str
    format: str = "best"

# ============================================================
#  ROUTES
# ============================================================
@app.get("/health")
def health():
    with jobs_lock:
        active = sum(1 for j in jobs.values() if j["status"] in ("starting", "downloading"))
    return {
        "status": "ok",
        "active_downloads": active,
        "max_concurrent": MAX_CONCURRENT_DOWNLOADS,
        "max_file_size_mb": MAX_FILE_SIZE_MB,
        "max_height": MAX_HEIGHT,
    }

@app.post("/info")
def get_info(req: InfoRequest):
    try:
        url = (req.url or "").strip()
        if not url or not url.startswith(("http://", "https://")):
            return JSONResponse(status_code=400, content={"error": MSG_INVALID_URL})

        with silence_ytdlp():
            with yt_dlp.YoutubeDL(ydl_opts(skip_download=True)) as ydl:
                info = ydl.extract_info(url, download=False)

        title = info.get("title", "Untitled")
        formats = []
        seen = set()
        for f in info.get("formats", []):
            height = f.get("height")
            ext = f.get("ext", "mp4")
            if height and height not in seen and height <= MAX_HEIGHT:
                seen.add(height)
                formats.append({
                    "id": f"bestvideo[height<={height}]+bestaudio/best[height<={height}]",
                    "label": f"{height}p",
                    "meta": f"{ext.upper()} · HD" if height >= 720 else ext.upper(),
                })

        formats.append({
            "id": "bestaudio/best",
            "label": "Audio Only",
            "meta": "MP3",
        })

        formats.sort(
            key=lambda x: int(x["label"].replace("p", "")) if "p" in x["label"] else 0,
            reverse=True,
        )

        return {
            "title": title,
            "thumbnail": info.get("thumbnail"),
            "duration": info.get("duration"),
            "uploader": info.get("uploader"),
            "view_count": info.get("view_count"),
            "formats": formats[:6],
            "max_file_size_mb": MAX_FILE_SIZE_MB,
        }

    except Exception as e:
        return JSONResponse(status_code=400, content={"error": friendly_error(e)})

@app.post("/preview")
def get_preview(req: InfoRequest):
    try:
        url = (req.url or "").strip()
        if not url or not url.startswith(("http://", "https://")):
            return JSONResponse(status_code=400, content={"error": MSG_INVALID_URL})

        with silence_ytdlp():
            with yt_dlp.YoutubeDL(ydl_opts(skip_download=True)) as ydl:
                info = ydl.extract_info(url, download=False)

        return {
            "title": info.get("title", "Untitled"),
            "thumbnail": info.get("thumbnail"),
            "duration": info.get("duration"),
            "uploader": info.get("uploader"),
            "view_count": info.get("view_count"),
            "webpage_url": info.get("webpage_url", url),
        }

    except Exception as e:
        return JSONResponse(status_code=400, content={"error": friendly_error(e)})

@app.post("/download")
def start_download(req: DownloadRequest, request: Request):
    url = (req.url or "").strip()
    if not url or not url.startswith(("http://", "https://")):
        return JSONResponse(status_code=400, content={"error": MSG_INVALID_URL})

    ip = get_client_ip(request)
    if rate_limited(ip):
        return JSONResponse(status_code=429, content={"error": MSG_RATE_LIMITED})

    with jobs_lock:
        if len(jobs) >= MAX_ACTIVE_JOBS:
            return JSONResponse(status_code=503, content={"error": MSG_BUSY})

    if not enough_disk_space():
        return JSONResponse(status_code=503, content={"error": MSG_DISK_FULL})

    if not download_semaphore.acquire(blocking=False):
        return JSONResponse(status_code=503, content={"error": MSG_BUSY})

    job_id = str(uuid.uuid4())
    with jobs_lock:
        jobs[job_id] = {
            "status": "starting",
            "percent": 0,
            "stage": "Processing…",
            "file": None,
            "error": None,
            "_updated": time.time(),
        }

    threading.Thread(target=run_download, args=(job_id, url, req.format), daemon=True).start()
    return {"job_id": job_id}

@app.get("/progress/{job_id}")
def get_progress(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return JSONResponse(status_code=404, content={"error": MSG_JOB_GONE})
        response = {
            "status": job["status"],
            "percent": job["percent"],
            "stage": job["stage"],
        }
        if job["status"] == "done":
            response["download_url"] = f"/file/{job_id}"
        if job["status"] == "error":
            response["error"] = job["error"] or MSG_GENERIC
    return response

CHUNK_SIZE = 1024 * 1024

@app.get("/file/{job_id}")
def get_file(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return JSONResponse(status_code=404, content={"error": MSG_JOB_GONE})

    stored = job.get("file")
    if stored and Path(stored).exists():
        path = Path(stored)
    else:
        path = None
        for f in DOWNLOAD_DIR.iterdir():
            if f.name.startswith(job_id) and f.is_file():
                if f.suffix in (".part", ".ytdl", ".tmp"):
                    continue
                path = f
                break
        if not path:
            return JSONResponse(status_code=404, content={"error": MSG_FILE_MISSING})

    filename = path.name
    try:
        file_size = path.stat().st_size
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": MSG_FILE_MISSING})

    def file_iterator():
        try:
            with open(path, "rb") as fh:
                while True:
                    chunk = fh.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    yield chunk
            print(f"[STREAM DONE] {filename}")
        except Exception as e:
            print(f"[STREAM ERROR] {filename}: {e}")
            raise
        finally:
            try:
                if path.exists():
                    path.unlink()
                    print(f"[CLEANUP] Deleted {filename}")
            except Exception as e:
                print(f"[CLEANUP FAIL] {filename}: {e}")
            with jobs_lock:
                jobs.pop(job_id, None)

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Content-Length": str(file_size),
        "X-Accel-Buffering": "no",
    }

    return StreamingResponse(
        file_iterator(),
        media_type="application/octet-stream",
        headers=headers,
    )

@app.get("/debug/jobs")
def debug_jobs():
    with jobs_lock:
        return {"count": len(jobs), "jobs": jobs}

@app.get("/debug/files")
def debug_files():
    files = []
    for f in DOWNLOAD_DIR.iterdir():
        if f.is_file():
            files.append({
                "name": f.name,
                "size_mb": round(f.stat().st_size / (1024 * 1024), 2),
                "age_sec": round(time.time() - f.stat().st_mtime, 1),
            })
    return {"count": len(files), "files": files}

# ============================================================
#  BACKGROUND WORKER
# ============================================================
def run_download(job_id, url, fmt):
    try:
        # ---- Pre-flight: size check ----
        try:
            with silence_ytdlp():
                with yt_dlp.YoutubeDL(ydl_opts(skip_download=True)) as ydl:
                    preflight = ydl.extract_info(url, download=False)

            approx_bytes = preflight.get("filesize") or preflight.get("filesize_approx")
            if approx_bytes and (approx_bytes / (1024 * 1024)) > MAX_FILE_SIZE_MB:
                raise TooLarge(f"~{approx_bytes / (1024 * 1024):.0f}MB exceeds {MAX_FILE_SIZE_MB}MB")
        except TooLarge:
            raise
        except Exception:
            pass

        if not enough_disk_space():
            raise DiskFull("low disk before download")

        def progress_hook(d):
            try:
                if d["status"] == "downloading":
                    total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                    downloaded = d.get("downloaded_bytes", 0)
                    if total > 0:
                        pct = int(downloaded / total * 90)
                        # User always sees "Processing…"
                        touch_job(job_id, percent=pct, stage="Processing…", status="downloading")
                elif d["status"] == "finished":
                    touch_job(job_id, percent=92, stage="Finalizing…")
            except Exception:
                pass

        opts = ydl_opts(skip_download=False, extra={
            "outtmpl": str(DOWNLOAD_DIR / f"{job_id}.%(ext)s"),
            "format": fmt,
            "progress_hooks": [progress_hook],
            "merge_output_format": "mp4",
            "max_filesize": MAX_FILE_SIZE_MB * 1024 * 1024,
        })

        with silence_ytdlp():
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.extract_info(url, download=True)

        found_file = None
        for f in DOWNLOAD_DIR.iterdir():
            if f.name.startswith(job_id) and f.is_file():
                if f.suffix in (".part", ".ytdl", ".tmp"):
                    continue
                found_file = f
                break

        if not found_file:
            raise Exception("no file on disk after download")

        actual_mb = found_file.stat().st_size / (1024 * 1024)
        if actual_mb > MAX_FILE_SIZE_MB:
            found_file.unlink()
            raise TooLarge(f"final file {actual_mb:.0f}MB exceeds {MAX_FILE_SIZE_MB}MB")

        touch_job(job_id, file=str(found_file), percent=100, stage="Complete", status="done")
        print(f"[OK] Job {job_id} → {found_file.name} ({actual_mb:.2f} MB)")

    except Exception as e:
        touch_job(job_id, status="error", error=friendly_error(e), stage="Failed")
    finally:
        download_semaphore.release()

# ============================================================
#  RUN
# ============================================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, workers=1)
