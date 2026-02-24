from __future__ import annotations
import argparse
import logging
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

try:
    import orjson

    _USE_ORJSON = True
except ImportError:
    _USE_ORJSON = False

import json
import requests
from fastwarc.warc import ArchiveIterator, WarcRecordType
from fastwarc.stream_io import GZipStream, FileStream, PythonIOStreamAdapter

from .bloom import BloomConfig, BloomFilter
from .extract import canonicalize_url, fast_prefilter, strict_extract

try:
    import orjson

    _USE_ORJSON = True
except ImportError:
    _USE_ORJSON = False
    orjson = None


def _fast_json_dumps(obj, indent=None):
    """Fast JSON serialization using orjson if available."""
    if _USE_ORJSON and orjson:
        opts = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(obj, option=opts).decode("utf-8")
    return json.dumps(obj, ensure_ascii=False, indent=indent)


# -----------------------------
# Logging
# -----------------------------


def setup_logging(log_path: Path, verbose: bool) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("cc_pointer_miner")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(logging.DEBUG)

    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(fmt)
    sh.setLevel(logging.DEBUG if verbose else logging.INFO)

    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(sh)
    logger.propagate = False
    return logger


# -----------------------------
# Distributed sync (minimal)
# -----------------------------


def _download_bloom(
    sync_url: str, logger: logging.Logger
) -> tuple[Optional[BloomFilter], int]:
    """Download bloom filter from sync URL. Returns (bloom, version)."""
    try:
        if sync_url.startswith("http://") or sync_url.startswith("https://"):
            sess = get_http_session()
            resp = sess.get(sync_url, timeout=30, verify=_verify_ssl)
            if resp.status_code == 200:
                import tempfile
                import os

                with tempfile.NamedTemporaryFile(delete=False, suffix=".bloom") as f:
                    f.write(resp.content)
                    f.flush()
                    temp_path = f.name
                bloom = BloomFilter.load(temp_path)
                version = int(resp.headers.get("X-Bloom-Version", 1))
                os.unlink(temp_path)
                logger.debug(f"Downloaded bloom v{version} from {sync_url}")
                return bloom, version
        elif sync_url.startswith("s3://"):
            logger.warning("S3 sync not implemented - use HTTP endpoint")
    except Exception as e:
        logger.debug(f"No remote bloom found: {e}")
    return None, 0


def _upload_bloom(
    sync_url: str, bloom: BloomFilter, version: int, logger: logging.Logger
) -> bool:
    """Upload bloom filter to sync URL. Returns True if successful."""
    try:
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(delete=False, suffix=".bloom") as f:
            temp_path = f.name
            bloom.save(f.name)
            f.flush()
            with open(f.name, "rb") as f:
                data = f.read()
        os.unlink(temp_path)

        if sync_url.startswith("http://") or sync_url.startswith("https://"):
            sess = get_http_session()
            resp = sess.put(
                sync_url,
                data=data,
                headers={
                    "Content-Type": "application/octet-stream",
                    "X-Bloom-Version": str(version + 1),
                },
                timeout=30,
                verify=_verify_ssl,
            )
            if resp.status_code in (200, 201, 204):
                logger.debug(f"Uploaded bloom v{version + 1} to {sync_url}")
                return True
        elif sync_url.startswith("s3://"):
            logger.warning("S3 sync not implemented - use HTTP endpoint")
    except Exception as e:
        logger.warning(f"Failed to upload bloom: {e}")
    return False


# -----------------------------
# Progress / manifest
# -----------------------------


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def atomic_write_text(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


@dataclass
class ShardSpec:
    shard_id: int
    shard_count: int

    def owns(self, idx: int) -> bool:
        return (idx % self.shard_count) == self.shard_id


# -----------------------------
# Streams / WARC iteration
# -----------------------------

_http_session = None
_verify_ssl = True

_prefetch_cache = {}
_prefetch_thread = None


def _prefetch_warc_async(warc_path: str):
    """Background prefetch of a WARC file."""
    global _prefetch_cache
    try:
        if warc_path.startswith("http://") or warc_path.startswith("https://"):
            sess = get_http_session()
            resp = sess.get(warc_path, stream=True, timeout=120, verify=_verify_ssl)
            resp.raise_for_status()
            _prefetch_cache[warc_path] = resp
    except Exception:
        pass


def _prefetch_warc_sync(warc_path: str):
    """Synchronous prefetch - downloads and returns the response."""
    try:
        if warc_path.startswith("http://") or warc_path.startswith("https://"):
            sess = get_http_session()
            resp = sess.get(warc_path, stream=True, timeout=120, verify=_verify_ssl)
            resp.raise_for_status()
            return resp
    except Exception:
        pass
    return None


def get_http_session() -> requests.Session:
    global _http_session
    if _http_session is None:
        _http_session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=3,
        )
        _http_session.mount("https://", adapter)
        _http_session.mount("http://", adapter)
    return _http_session


class _HttpStreamWrapper:
    """File-like wrapper for HTTP WARC URLs; fastwarc has no built-in HTTPStream."""

    def __init__(self, url: str):
        self._url = url
        sess = get_http_session()
        self._resp = sess.get(url, stream=True, timeout=60, verify=_verify_ssl)
        self._resp.raise_for_status()
        self._resp.raw.decode_content = False  # GZipStream will decode
        self._raw = self._resp.raw
        self._pos = 0

    def read(self, size: int = -1) -> bytes:
        if size == -1:
            chunk = self._raw.read()
        else:
            chunk = self._raw.read(size)
        self._pos += len(chunk)
        return chunk

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = 0) -> int:
        if offset == 0 and whence == 0:
            return 0
        raise OSError("HTTP stream is not seekable")

    def close(self) -> None:
        self._resp.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


def open_stream(path_or_url: str, prefetched_response=None):
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        if prefetched_response is not None:
            prefetched_response.raw.decode_content = False
            return PythonIOStreamAdapter(
                _HttpStreamWrapperFromResponse(path_or_url, prefetched_response)
            )
        return PythonIOStreamAdapter(_HttpStreamWrapper(path_or_url))
    return FileStream(path_or_url)


class _HttpStreamWrapperFromResponse:
    """Wrapper that uses a prefetched response."""

    def __init__(self, url: str, response):
        self._url = url
        self._resp = response
        self._resp.raw.decode_content = False
        self._raw = self._resp.raw
        self._pos = 0

    def read(self, size: int = -1) -> bytes:
        if size == -1:
            chunk = self._raw.read()
        else:
            chunk = self._raw.read(size)
        self._pos += len(chunk)
        return chunk

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = 0) -> int:
        if offset == 0 and whence == 0:
            return 0
        raise OSError("HTTP stream is not seekable")

    def close(self) -> None:
        self._resp.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


def iter_response_records(warc_path: str, prefetched_response=None):
    stream = open_stream(warc_path, prefetched_response)
    gz = GZipStream(stream)
    it = ArchiveIterator(gz, record_types=WarcRecordType.response)
    for rec in it:
        yield rec


def get_http_status_and_mime(rec) -> tuple[Optional[int], Optional[str]]:
    status = None
    mime = None
    try:
        if rec.http_headers is not None:
            status = rec.http_headers.status_code
            mime = rec.http_headers.get("Content-Type")
            if mime:
                mime = mime.split(";")[0].strip().lower()
    except Exception:
        pass
    return status, mime


def get_warc_header(rec, name: str) -> Optional[str]:
    try:
        v = rec.headers.get(name)
        if v is None:
            return None
        if isinstance(v, bytes):
            return v.decode("utf-8", errors="ignore")
        return str(v)
    except Exception:
        return None


# -----------------------------
# Main runner: processes WARC files, with per-file commit for crash safety
# -----------------------------


def process_warc_file(
    *,
    crawl: str,
    warc_path: str,
    out_dir: Path,
    bloom: BloomFilter,
    logger: logging.Logger,
    max_scan: int,
    max_head: int,
    max_pii_scan: int,
    require_200: bool,
    require_html: bool,
    skip_noai: bool,
    strip_query: bool,
    accept_jsonld: bool,
    prefetched_response=None,
) -> dict:
    """Process a single WARC file.

    Crash safety:
      - Writes to a .part file
      - Only renames to .jsonl when complete
      - Bloom filter is only saved by the caller after successful completion
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    warc_name = Path(warc_path).name.replace("/", "_")
    part_path = out_dir / f"{warc_name}.jsonl.part"
    final_path = out_dir / f"{warc_name}.jsonl"

    # If already finished, skip
    if final_path.exists():
        return {
            "skipped": True,
            "warc": warc_path,
            "emitted": 0,
            "seen": 0,
            "prefilter": 0,
            "total": 0,
        }

    # If a stale part exists (from crash), overwrite it (safe because bloom wasn't committed for this file)
    if part_path.exists():
        part_path.unlink()

    total = 0
    prefiltered = 0
    emitted = 0
    seen_skipped = 0

    t0 = time.time()
    with part_path.open("wb", buffering=1024 * 1024) as out_f:
        for rec in iter_response_records(warc_path, prefetched_response):
            total += 1
            url = get_warc_header(rec, "WARC-Target-URI")
            if not url:
                continue

            status, mime = get_http_status_and_mime(rec)
            if require_200 and status != 200:
                continue
            if require_html and (mime != "text/html"):
                continue

            try:
                # Two-stage read: only read full body when prefilter passes (saves memory + work)
                chunk = rec.reader.read(max_scan)
                if not chunk:
                    continue
            except Exception:
                continue

            if not fast_prefilter(chunk, max_scan=max_scan):
                # Drain remainder so stream advances to next record
                try:
                    while rec.reader.read(256 * 1024):
                        pass
                except Exception:
                    pass
                continue
            prefiltered += 1

            # Dedup by canonical URL - check BEFORE expensive strict_extract
            canon = canonicalize_url(url, strip_query=strip_query)
            key = canon.encode("utf-8", errors="ignore")
            if key in bloom:
                seen_skipped += 1
                # Drain remainder so stream advances to next record
                try:
                    while rec.reader.read(256 * 1024):
                        pass
                except Exception:
                    pass
                continue

            # Read remainder for strict_extract (we need up to max_head / max_pii_scan)
            try:
                rest = rec.reader.read()
                body = chunk + rest
            except Exception:
                continue

            ex = strict_extract(
                body,
                max_head=max_head,
                max_pii_scan=max_pii_scan,
                accept_jsonld=accept_jsonld,
            )
            if not ex.ok:
                continue
            if skip_noai and ex.robots_noai:
                continue

            # Now add to bloom (only for accepted records)
            bloom.add(key)

            row = {
                "crawl": crawl,
                "warc_path": warc_path,
                "url": url,
                "url_canon": canon,
                "timestamp": get_warc_header(rec, "WARC-Date"),
                "mime": mime,
                "http_status": status,
                "warc_record_id": get_warc_header(rec, "WARC-Record-ID"),
                "license": {
                    "abbr": ex.abbr,
                    "version": ex.version,
                    "uri": ex.license_uri,
                    "location": ex.location,
                    "in_head": ex.in_head,
                    "disagreement": ex.disagreement,
                },
                "signals": {
                    "pii_suspected": ex.pii_suspected,
                    "robots_noai": ex.robots_noai,
                },
            }
            out_f.write((_fast_json_dumps(row) + "\n").encode("utf-8"))
            emitted += 1

    # Commit this warc output atomically
    part_path.replace(final_path)
    dt = time.time() - t0
    logger.info(
        "Finished WARC %s: total=%d prefilter=%d seen_skip=%d emitted=%d in %.1fs",
        warc_path,
        total,
        prefiltered,
        seen_skipped,
        emitted,
        dt,
    )
    return {
        "skipped": False,
        "warc": warc_path,
        "total": total,
        "prefilter": prefiltered,
        "seen_skip": seen_skipped,
        "emitted": emitted,
        "seconds": dt,
    }


def run(args: argparse.Namespace) -> int:
    global _verify_ssl
    _verify_ssl = not args.no_verify_ssl

    shard = ShardSpec(args.shard_id, args.shard_count)
    work_dir = Path(args.work_dir)
    state_dir = (
        work_dir / "state" / f"shard_{args.shard_id:03d}_of_{args.shard_count:03d}"
    )
    out_dir = (
        work_dir / "outputs" / f"shard_{args.shard_id:03d}_of_{args.shard_count:03d}"
    )
    logs_dir = work_dir / "logs"
    state_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    logger = setup_logging(
        logs_dir / f"shard_{args.shard_id:03d}.log", verbose=args.verbose
    )

    # Load warc list
    warc_list_path = Path(args.warc_list)
    warc_paths = [
        ln.strip()
        for ln in warc_list_path.read_text(encoding="utf-8").splitlines()
        if ln.strip() and not ln.strip().startswith("#")
    ]
    logger.info("Loaded %d WARC paths", len(warc_paths))

    # Shard selection
    shard_paths = [p for i, p in enumerate(warc_paths) if shard.owns(i)]
    logger.info(
        "Shard %d/%d assigned %d WARC files",
        args.shard_id,
        args.shard_count,
        len(shard_paths),
    )

    progress_path = state_dir / "progress.json"
    progress = load_json(
        progress_path,
        default={
            "done": {},
            "stats": {
                "emitted": 0,
                "warcs_done": 0,
                "total": 0,
                "prefiltered": 0,
                "seen_skip": 0,
            },
        },
    )

    done_set = set(progress.get("done", {}).keys())

    # Download all WARCs first if requested (huge speedup for sequential processing)
    if args.download_first:
        download_dir = work_dir / "downloads"
        download_dir.mkdir(parents=True, exist_ok=True)

        # Filter to pending WARCs
        pending_warcs = [w for w in shard_paths if w not in done_set]

        if pending_warcs:
            # Check which ones are already downloaded
            to_download = []
            for w in pending_warcs:
                local_path = download_dir / Path(w).name
                if not local_path.exists():
                    to_download.append(w)

            if to_download:
                logger.info(
                    "Downloading %d WARC files to %s (parallel=%d)...",
                    len(to_download),
                    download_dir,
                    args.download_workers,
                )

                def download_one(warc_url):
                    local_path = download_dir / Path(warc_url).name
                    if local_path.exists():
                        return warc_url, str(local_path)
                    try:
                        sess = get_http_session()
                        resp = sess.get(
                            warc_url, stream=True, timeout=300, verify=_verify_ssl
                        )
                        resp.raise_for_status()
                        with open(local_path, "wb") as f:
                            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                                f.write(chunk)
                        return warc_url, str(local_path)
                    except Exception as e:
                        logger.warning(f"Failed to download {warc_url}: {e}")
                        return warc_url, warc_url  # Fall back to remote

                # Download in parallel
                with ThreadPoolExecutor(max_workers=args.download_workers) as executor:
                    futures = {executor.submit(download_one, w): w for w in to_download}
                    for future in as_completed(futures):
                        warc_url, local_path = future.result()
                        # Replace URL with local path
                        for i, w in enumerate(shard_paths):
                            if w == warc_url:
                                shard_paths[i] = local_path
                                break

                logger.info("Download complete, processing locally...")

    # Pre-decompress if requested
    if args.pre_decompress and shard_paths:
        decompress_dir = work_dir / "decompressed"
        decompress_dir.mkdir(parents=True, exist_ok=True)

        # Find WARCs that need decompression
        to_decompress = []
        for w in shard_paths:
            if w.startswith("http"):
                continue  # Skip remote URLs
            uncompressed = decompress_dir / Path(w).stem
            if not uncompressed.exists():
                to_decompress.append(w)

        if to_decompress:
            import gzip
            import shutil

            logger.info(
                "Decompressing %d WARC files to %s (parallel=%d)...",
                len(to_decompress),
                decompress_dir,
                args.decompress_workers,
            )

            def decompress_one(warc_path):
                uncompressed = decompress_dir / Path(warc_path).stem
                if uncompressed.exists():
                    return warc_path, str(uncompressed)
                try:
                    with gzip.open(warc_path, "rb") as f_in:
                        with open(uncompressed, "wb") as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    return warc_path, str(uncompressed)
                except Exception as e:
                    logger.warning(f"Failed to decompress {warc_path}: {e}")
                    return warc_path, warc_path

            with ThreadPoolExecutor(max_workers=args.decompress_workers) as executor:
                futures = {executor.submit(decompress_one, w): w for w in to_decompress}
                for future in as_completed(futures):
                    warc_url, decompressed_path = future.result()
                    for i, w in enumerate(shard_paths):
                        if w == warc_url:
                            shard_paths[i] = decompressed_path
                            break

            logger.info("Decompression complete, processing...")

    # For download-first, we've now replaced URLs with local paths

    # Bloom filter (obligatory)
    bloom_path = state_dir / "seen_urls.bloom"

    # For parallel processing, we'll collect results and merge
    all_stats = []
    final_bloom = None

    if args.workers > 1:
        # Parallel processing: split WARCs into chunks for each worker
        pending_warcs = [w for w in shard_paths if w not in done_set]
        if pending_warcs:
            # Split WARCs among workers
            chunk_size = max(1, len(pending_warcs) // args.workers)
            chunks = [
                pending_warcs[i : i + chunk_size]
                for i in range(0, len(pending_warcs), chunk_size)
            ]

            def process_chunk(warc_paths_chunk):
                # Each worker gets its own bloom filter and logger
                cfg = BloomConfig(
                    capacity=args.bloom_capacity,
                    error_rate=args.bloom_error_rate,
                    seed=args.bloom_seed.encode("utf-8"),
                )
                chunk_bloom = BloomFilter(cfg)
                chunk_logger = logging.getLogger(f"cc_pointer_miner.worker")
                chunk_stats = []

                for warc_path in warc_paths_chunk:
                    stats = process_warc_file(
                        crawl=args.crawl,
                        warc_path=warc_path,
                        out_dir=out_dir,
                        bloom=chunk_bloom,
                        logger=chunk_logger,
                        max_scan=args.max_scan,
                        max_head=args.max_head,
                        max_pii_scan=args.max_pii_scan,
                        require_200=args.require_200,
                        require_html=args.require_html,
                        skip_noai=args.skip_noai,
                        strip_query=args.strip_query,
                        accept_jsonld=not args.reject_jsonld,
                    )
                    chunk_stats.append(stats)
                return chunk_stats, chunk_bloom

            logger.info(
                "Starting %d workers for %d WARCs", args.workers, len(pending_warcs)
            )
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
                for future in as_completed(futures):
                    chunk_stats, chunk_bloom = future.result()
                    all_stats.extend(chunk_stats)
                    if final_bloom is None:
                        final_bloom = chunk_bloom
                    else:
                        final_bloom.merge(chunk_bloom)

            # Update progress with all stats
            for stats in all_stats:
                if not stats.get("skipped", False):
                    progress["done"][stats["warc"]] = stats
                    progress["stats"]["emitted"] = int(
                        progress["stats"].get("emitted", 0)
                    ) + int(stats.get("emitted", 0))
                    progress["stats"]["warcs_done"] = (
                        int(progress["stats"].get("warcs_done", 0)) + 1
                    )
                    progress["stats"]["total"] = int(
                        progress["stats"].get("total", 0)
                    ) + int(stats.get("total", 0))
                    progress["stats"]["prefiltered"] = int(
                        progress["stats"].get("prefiltered", 0)
                    ) + int(stats.get("prefilter", 0))
                    progress["stats"]["seen_skip"] = int(
                        progress["stats"].get("seen_skip", 0)
                    ) + int(stats.get("seen_skip", 0))

            # Save merged bloom
            if final_bloom:
                final_bloom.save(bloom_path)
            atomic_write_text(progress_path, _fast_json_dumps(progress, indent=2))
    else:
        # Sequential processing (original code)
        if bloom_path.exists():
            bloom = BloomFilter.load(bloom_path)
            logger.info("Loaded bloom filter from %s", bloom_path)
        else:
            cfg = BloomConfig(
                capacity=args.bloom_capacity,
                error_rate=args.bloom_error_rate,
                seed=args.bloom_seed.encode("utf-8"),
            )
            bloom = BloomFilter(cfg)
            logger.info(
                "Initialized bloom filter: capacity=%d error_rate=%g",
                cfg.capacity,
                cfg.error_rate,
            )

        # Sync bloom filter from remote if configured
        sync_version = 0
        if args.sync_url:
            remote_bloom, sync_version = _download_bloom(args.sync_url, logger)
            if remote_bloom is not None:
                bloom = remote_bloom
                logger.info(
                    "Synced bloom filter from remote (version %d)", sync_version
                )

        prefetch_response = None

        for i, warc_path in enumerate(shard_paths):
            if warc_path in done_set:
                continue

            # Sync bloom before processing if configured (handle race: retry if stale)
            if args.sync_url:
                for _ in range(3):
                    remote_bloom, remote_version = _download_bloom(
                        args.sync_url, logger
                    )
                    if remote_bloom is not None and remote_version >= sync_version:
                        bloom = remote_bloom
                        sync_version = remote_version
                        break
                    elif remote_bloom is None:
                        break
                    time.sleep(1)  # Wait for other worker to finish

            # Use prefetched response if available, otherwise fetch fresh
            use_response = prefetch_response
            prefetch_response = None
            prefetch_warc_path = None
            prefetch_thread = None
            result_holder = {}

            # Start prefetching next WARC in background while processing current
            if i + 1 < len(shard_paths):
                next_warc = shard_paths[i + 1]
                if next_warc not in done_set and (
                    next_warc.startswith("http://") or next_warc.startswith("https://")
                ):
                    prefetch_warc_path = next_warc

                    # Start background thread
                    def prefetch_task():
                        result_holder["resp"] = _prefetch_warc_sync(prefetch_warc_path)

                    prefetch_thread = threading.Thread(target=prefetch_task)
                    prefetch_thread.start()

            # Process WARC; output commits per file
            stats = process_warc_file(
                crawl=args.crawl,
                warc_path=warc_path,
                out_dir=out_dir,
                bloom=bloom,
                logger=logger,
                max_scan=args.max_scan,
                max_head=args.max_head,
                max_pii_scan=args.max_pii_scan,
                require_200=args.require_200,
                require_html=args.require_html,
                skip_noai=args.skip_noai,
                strip_query=args.strip_query,
                accept_jsonld=not args.reject_jsonld,
                prefetched_response=use_response,
            )

            # Wait for prefetch thread to complete and get result
            if prefetch_thread is not None:
                prefetch_thread.join()
                prefetch_response = result_holder.get("resp")

            # Only mark done + save bloom if successful
            if not stats.get("skipped", False):
                progress["done"][warc_path] = stats
                progress["stats"]["emitted"] = int(
                    progress["stats"].get("emitted", 0)
                ) + int(stats.get("emitted", 0))
                progress["stats"]["warcs_done"] = (
                    int(progress["stats"].get("warcs_done", 0)) + 1
                )
                progress["stats"]["total"] = int(
                    progress["stats"].get("total", 0)
                ) + int(stats.get("total", 0))
                progress["stats"]["prefiltered"] = int(
                    progress["stats"].get("prefiltered", 0)
                ) + int(stats.get("prefilter", 0))
                progress["stats"]["seen_skip"] = int(
                    progress["stats"].get("seen_skip", 0)
                ) + int(stats.get("seen_skip", 0))

                # Persist bloom after each WARC (checkpoint)
                bloom.save(bloom_path)

                # Sync bloom to remote if configured
                if args.sync_url:
                    _upload_bloom(args.sync_url, bloom, sync_version, logger)
                    sync_version += 1

                # Persist progress after each WARC (checkpoint)
                atomic_write_text(progress_path, _fast_json_dumps(progress, indent=2))

    total = progress["stats"].get("total", 0)
    emitted = progress["stats"].get("emitted", 0)
    prefiltered = progress["stats"].get("prefiltered", 0)
    seen_skip = progress["stats"].get("seen_skip", 0)
    warcs_done = progress["stats"].get("warcs_done", 0)

    # Classification:
    # - total: all records in Common Crawl WARCs
    # - likely: passed prefilter (need strict extraction)
    # - confirmed: passed strict extraction (emitted)
    # - unprocessed: filtered out before prefilter (not CC HTML, wrong status, etc.)
    total = progress["stats"].get("total", 0)
    likely = progress["stats"].get("prefiltered", 0)
    confirmed = emitted
    unprocessed = total - likely

    logger.info(
        "All assigned WARC files processed. Total emitted=%d warcs_done=%d",
        emitted,
        warcs_done,
    )
    logger.info(
        "Classification: total=%d, likely=%d (%.2f%%), confirmed=%d (%.2f%%), unprocessed=%d (%.2f%%)",
        total,
        likely,
        (likely / total * 100) if total > 0 else 0,
        confirmed,
        (confirmed / total * 100) if total > 0 else 0,
        unprocessed,
        (unprocessed / total * 100) if total > 0 else 0,
    )
    return 0


def build_argparser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="cc-pointer-miner",
        description="Pointer-only CC license miner (strict, fast, resumable).",
    )
    ap.add_argument("--crawl", required=True, help="Crawl ID e.g. CC-MAIN-2025-10")
    ap.add_argument(
        "--warc-list",
        required=True,
        help="Text file listing WARC paths/URLs (one per line)",
    )
    ap.add_argument(
        "--work-dir", default="work", help="Directory for outputs/state/logs"
    )
    ap.add_argument(
        "--shard-id", type=int, default=0, help="Shard id [0..shard-count-1]"
    )
    ap.add_argument("--shard-count", type=int, default=1, help="Total number of shards")
    ap.add_argument("--verbose", action="store_true", help="Verbose logging")

    # Performance / filters
    ap.add_argument(
        "--max-scan", type=int, default=128_000, help="Bytes scanned for fast prefilter"
    )
    ap.add_argument(
        "--max-head", type=int, default=256_000, help="Max bytes used to extract head"
    )
    ap.add_argument(
        "--max-pii-scan",
        type=int,
        default=128_000,
        help="Bytes scanned for PII heuristics",
    )
    ap.add_argument(
        "--require-200", action="store_true", help="Only accept HTTP 200 responses"
    )
    ap.add_argument(
        "--require-html", action="store_true", help="Only accept Content-Type text/html"
    )
    ap.add_argument(
        "--skip-noai",
        action="store_true",
        help="Skip pages with noai/noimageai signals in head",
    )
    ap.add_argument(
        "--strip-query",
        action="store_true",
        help="Strip query from URLs for dedup key (more aggressive)",
    )
    ap.add_argument(
        "--reject-jsonld",
        action="store_true",
        help="Reject JSON-LD license evidence (stricter)",
    )

    # Bloom filter params (obligatory)
    ap.add_argument(
        "--bloom-capacity",
        type=int,
        default=50_000_000,
        help="Expected unique URLs per shard (size the bloom; required for performance)",
    )
    ap.add_argument(
        "--bloom-error-rate", type=float, default=1e-6, help="Bloom false positive rate"
    )
    ap.add_argument(
        "--bloom-seed",
        type=str,
        default="cc_pointer_miner",
        help="Seed for bloom hashing (stable)",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel workers (default: 1, useful for I/O-bound workloads)",
    )
    ap.add_argument(
        "--no-verify-ssl",
        action="store_true",
        help="Skip SSL certificate verification (faster for HTTPS)",
    )
    ap.add_argument(
        "--sync-url",
        type=str,
        default=None,
        help="URL for shared bloom/progress sync (e.g., s3://bucket/path, https://server/). "
        "If set, workers sync bloom filter across machines.",
    )
    ap.add_argument(
        "--download-first",
        action="store_true",
        help="Download all WARCs to local disk first, then process (much faster for sequential processing)",
    )
    ap.add_argument(
        "--download-workers",
        type=int,
        default=4,
        help="Number of parallel downloads when --download-first is used",
    )
    ap.add_argument(
        "--pre-decompress",
        action="store_true",
        help="Decompress WARC files before processing (2-3x faster processing, requires 10x disk space)",
    )
    ap.add_argument(
        "--decompress-workers",
        type=int,
        default=4,
        help="Number of parallel decompression workers",
    )

    return ap


def main(argv: Optional[List[str]] = None) -> int:
    ap = build_argparser()
    args = ap.parse_args(argv)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
