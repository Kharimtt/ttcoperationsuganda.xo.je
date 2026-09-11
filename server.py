"""
TT COPERATIONS UGANDA — Video Downloader Backend
FastAPI + yt-dlp
Users NEVER see raw errors — all failures return friendly messages.
"""

import os
import sys
import time
import uuid
import threading
import contextlib
import io
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
import yt_dlp

# ============================================================
#  CONFIG
# ============================================================
DOWNLOAD_DIR = Path("/tmp/downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

jobs = {}

app = FastAPI(title="TT COPERATIONS Downloader")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

# ============================================================
#  USER-SAFE MESSAGES — ONLY these ever reach the browser
# ============================================================
MSG_INVALID_URL   = "That link doesn't look right. Please paste a valid video URL."
MSG_UNSUPPORTED   = "Sorry, this video can't be downloaded right now. Please try a different link or platform."
MSG_NOT_FOUND     = "We couldn't find that video. Please check the link and try again."
MSG_NETWORK       = "The server is busy. Please try again in a moment."
MSG_GENERIC       = "Something went wrong. Please try again."
MSG_JOB_GONE      = "This download has expired. Please start a new one."
MSG_FILE_MISSING  = "The file is no longer available. Please start a new download."
MSG_BUSY          = "The server is handling too many requests. Please wait a moment and try again."

def log_error(exc: Exception):
    """Print the full technical error to the server console ONLY."""
    print(f"[ERROR] {type(exc).__name__}: {exc}", flush=True)

def friendly_error(exc: Exception) -> str:
    """Return a short, user-safe message. Full error goes to logs only."""
    log_error(exc)
    text = str(exc).lower()

    if any(k in text for k in (
        "unsupported", "unable to extract", "unable to download",
        "private", "sign in", "age", "not available",
        "no video", "video unavailable", "unexpected response"
    )):
        return MSG_UNSUPPORTED
    if any(k in text for k in ("not found", "404", "does not exist")):
        return MSG_NOT_FOUND
    if any(k in text for k in ("timed out", "timeout", "connection", "network")):
        return MSG_NETWORK
    if any(k in text for k in ("too many requests", "429", "rate limit")):
        return MSG_BUSY
    return MSG_GENERIC

# ============================================================
#  SILENCE yt-dlp COMPLETELY
#  This prevents yt-dlp from printing to stdout/stderr, which
#  can leak into responses in some FastAPI configurations.
# ============================================================
@contextlib.contextmanager
def silence_ytdlp():
    """Redirect stdout/stderr during yt-dlp calls."""
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    sys.stdout = io.StringIO()
    sys.stderr = io.StringIO()
    try:
        yield
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr

def ydl_opts(skip_download=True, extra=None):
    """Shared yt-dlp options with mobile clients to reduce bot checks."""
    opts = {
        "quiet": True,
        "no_warnings": True,
        "no_color": True,
        "skip_download": skip_download,
        "noprogress": True,
        "logger": None,
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
#  GLOBAL EXCEPTION HANDLER
# ============================================================
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    log_error(exc)
    return JSONResponse(status_code=500, content={"error": MSG_GENERIC})

# ============================================================
#  CLEANUP ON STARTUP
# ============================================================
def cleanup_old_files(max_age_seconds=3600):
    now = time.time()
    removed = 0
    for f in DOWNLOAD_DIR.iterdir():
        if f.is_file() and (now - f.stat().st_mtime) > max_age_seconds:
            try:
                f.unlink()
                removed += 1
            except Exception:
                pass
    if removed:
        print(f"[CLEANUP] Removed {removed} stale file(s) on startup")

cleanup_old_files()

# ============================================================
#  REQUEST MODELS
# ============================================================
class InfoRequest(BaseModel):
    url: str

class DownloadRequest(BaseModel):
    url: str
    format: str = "best"

# ============================================================
#  ROUTE: /info
# ============================================================
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
            if height and height not in seen:
                seen.add(height)
                formats.append({
                    "id": f"bestvideo[height<={height}]+bestaudio/best[height<={height}]",
                    "label": f"{height}p",
                    "meta": f"{ext.upper()} · HD" if height >= 720 else ext.upper()
                })

        formats.append({
            "id": "bestaudio/best",
            "label": "Audio Only",
            "meta": "MP3"
        })

        formats.sort(
            key=lambda x: int(x["label"].replace("p", "")) if "p" in x["label"] else 0,
            reverse=True
        )

        return {
            "title": title,
            "thumbnail": info.get("thumbnail"),
            "duration": info.get("duration"),
            "uploader": info.get("uploader"),
            "view_count": info.get("view_count"),
            "formats": formats[:6]
        }

    except Exception as e:
        return JSONResponse(status_code=400, content={"error": friendly_error(e)})

# ============================================================
#  ROUTE: /preview
# ============================================================
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

# ============================================================
#  ROUTE: /download
# ============================================================
@app.post("/download")
def start_download(req: DownloadRequest):
    url = (req.url or "").strip()
    if not url or not url.startswith(("http://", "https://")):
        return JSONResponse(status_code=400, content={"error": MSG_INVALID_URL})

    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "status": "starting",
        "percent": 0,
        "stage": "Preparing…",
        "file": None,
        "error": None,
    }

    thread = threading.Thread(target=run_download, args=(job_id, url, req.format))
    thread.daemon = True
    thread.start()

    return {"job_id": job_id}

# ============================================================
#  ROUTE: /progress/{job_id}
# ============================================================
@app.get("/progress/{job_id}")
def get_progress(job_id: str):
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

# ============================================================
#  ROUTE: /file/{job_id}
# ============================================================
CHUNK_SIZE = 1024 * 1024

@app.get("/file/{job_id}")
def get_file(job_id: str):
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
    file_size = path.stat().st_size

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

# ============================================================
#  DEBUG ROUTES
# ============================================================
@app.get("/debug/jobs")
def debug_jobs():
    return {"count": len(jobs), "jobs": jobs}

@app.get("/debug/files")
def debug_files():
    files = []
    for f in DOWNLOAD_DIR.iterdir():
        if f.is_file():
            files.append({
                "name": f.name,
                "size_mb": round(f.stat().st_size / (1024 * 1024), 2),
                "age_sec": round(time.time() - f.stat().st_mtime, 1)
            })
    return {"count": len(files), "files": files}

# ============================================================
#  BACKGROUND WORKER
# ============================================================
def run_download(job_id, url, fmt):
    def progress_hook(d):
        try:
            if d["status"] == "downloading":
                total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                downloaded = d.get("downloaded_bytes", 0)
                if total > 0:
                    pct = int(downloaded / total * 90)
                    jobs[job_id]["percent"] = pct
                    jobs[job_id]["stage"] = "Downloading…"
                    jobs[job_id]["status"] = "downloading"
            elif d["status"] == "finished":
                jobs[job_id]["percent"] = 92
                jobs[job_id]["stage"] = "Processing…"
        except Exception:
            pass

    opts = ydl_opts(skip_download=False, extra={
        "outtmpl": str(DOWNLOAD_DIR / f"{job_id}.%(ext)s"),
        "format": fmt,
        "progress_hooks": [progress_hook],
        "merge_output_format": "mp4",
    })

    try:
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

        jobs[job_id]["file"] = str(found_file)
        jobs[job_id]["percent"] = 100
        jobs[job_id]["stage"] = "Complete"
        jobs[job_id]["status"] = "done"

        size_mb = round(found_file.stat().st_size / (1024 * 1024), 2)
        print(f"[OK] Job {job_id} → {found_file.name} ({size_mb} MB)")

    except Exception as e:
        log_error(e)
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = friendly_error(e)
        jobs[job_id]["stage"] = "Failed"

# ============================================================
#  RUN
# ============================================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
