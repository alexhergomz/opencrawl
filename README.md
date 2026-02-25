# OpenCrawl

**OpenCrawl** is a high-performance, distributed miner for extracting Creative Commons and open licensed content from Common Crawl WARC files.

## Features

- **Pointer-only output**: No document text stored, just JSONL pointers + metadata
- **Strict & ethical**: Only accepts strong license evidence from `<head>` (link/meta tags, JSON-LD)
- **Bloom filter dedup**: Memory-efficient deduplication across URLs
- **Crash-resumable**: Per-WARC checkpoints, safe restarts
- **Distributed**: Run multiple workers across machines with shared bloom sync
- **Fast**: Streaming architecture, multiprocessing, connection pooling, prefilter optimization

## Supported Licenses

OpenCrawl targets ethical, permissive licenses:

- **Creative Commons**: CC0, CC-BY, CC-BY-SA, CC-BY-NC, CC-BY-NC-SA, CC-BY-ND, CC-BY-NC-ND (all versions)
- **Public Domain**: PD, CC0, No Rights Reserved
- **Open Database License**: ODbL

## Installation

```bash
git clone https://github.com/alexhergomz/opencrawl.git
cd opencrawl
pip install -r requirements.txt
```

**Note**: If you encounter a `UnicodeEncodeError` when running the CLI (e.g., in environments with ASCII-only terminal encoding), ensure your terminal supports UTF-8 or set `PYTHONIOENCODING=utf-8`.

## Quick Start

1. Create a WARC list file:

```bash
# List of WARC URLs (one per line)
curl -sL "https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-10/segments/1707947473347.0/warc/CC-MAIN-20240220211055-20240221001055-00000.warc.gz" > warc_list.txt
```

2. Run the miner:

```bash
python opencrawl_cli.py \
  --crawl CC-MAIN-2024-10 \
  --warc-list warc_list.txt \
  --work-dir work \
  --shard-id 0 --shard-count 10 \
  --require-200 --require-html --skip-noai \
  --workers 8
```

3. Output is in `work/outputs/shard_000_of_010/*.jsonl`

## Output Format

Each line is a JSON object:

```json
{
  "crawl": "CC-MAIN-2024-10",
  "warc_path": "https://data.commoncrawl.org/...",
  "url": "https://example.com/page",
  "url_canon": "https://example.com/page",
  "timestamp": "2024-02-20T21:10:55Z",
  "mime": "text/html",
  "http_status": 200,
  "warc_record_id": "<urn:uuid:...>",
  "license": {
    "abbr": "cc-by",
    "version": "4.0",
    "uri": "https://creativecommons.org/licenses/by/4.0/",
    "location": "head_link",
    "in_head": true,
    "disagreement": false
  },
  "signals": {
    "pii_suspected": false,
    "robots_noai": false
  }
}
```

## Performance

| Configuration | Time/WARC | Notes |
|-------------|-----------|-------|
| Network streaming | ~100s | I/O bound |
| Local .gz files | ~17s | 6x faster |
| Local files + 8 workers | ~5-7s | 19x faster |

- ~35k records per WARC
- ~1000-6000 records/second throughput
- Bloom filter: ~6MB per 50M URLs

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--crawl` | Crawl ID (e.g., CC-MAIN-2024-10) | (required) |
| `--warc-list` | Path to WARC list file | (required) |
| `--work-dir` | Working directory | `work` |
| `--shard-id` | Shard ID (0 to shard-count-1) | 0 |
| `--shard-count` | Total number of shards | 1 |
| `--workers` | Number of parallel workers | 1 |
| `--require-200` | Only accept HTTP 200 responses | No |
| `--require-html` | Only accept text/html | No |
| `--skip-noai` | Skip pages with noai/noimageai robots meta | No |
| `--reject-jsonld` | Reject JSON-LD license evidence | No |
| `--bloom-capacity` | Expected unique URLs per shard | 50M |
| `--sync-url` | HTTP URL for shared bloom sync | None |
| `--no-verify-ssl` | Skip SSL verification | No |
| `--download-first` | Download all WARCs before processing | No |
| `--download-workers` | Parallel download workers | 4 |
| `--pre-decompress` | Decompress WARC files before processing | No |
| `--decompress-workers` | Parallel decompression workers | 4 |

## Distributed Processing

Run multiple workers across machines:

```bash
# Worker 1 (machine 1)
python opencrawl_cli.py --shard-id 0 --shard-count 4 --sync-url http://server/bloom.bloom ...

# Worker 2 (machine 2)
python opencrawl_cli.py --shard-id 1 --shard-count 4 --sync-url http://server/bloom.bloom ...
```

For the sync URL, you need a simple HTTP server:

```bash
# On a shared server
mkdir -p /var/www/html/crawl
python -m http.server 8080 --directory /var/www/html/crawl
# Use: --sync-url http://yourserver:8080/crawl/shared.bloom
```

## Architecture

1. **Streaming**: WARC records processed one at a time, low memory
2. **Two-stage read**: First 128KB for fast prefilter (string ops, not regex), full body only if passed
3. **Bloom dedup**: Check canonical URL before expensive extraction
4. **Multiprocessing**: True parallelism via ProcessPoolExecutor
5. **Checkpoint per WARC**: Safe crash recovery

## Future Features

Planned enhancements:

- [ ] Support more open licenses (MIT, Apache, GPL variants)
- [ ] Domain allowlist/denylist filtering
- [ ] CDX index integration for exact byte offsets
- [ ] HyperLogLog for unique URL/domain metrics
- [ ] S3/GCS bucket support for distributed sync
- [ ] Output format options (CSV, Parquet)
- [ ] License classification confidence scores
- [ ] Integration with Common Crawl CDX API for smarter WARC selection

## License

MIT License - See LICENSE file.

## Credits

- Built on [FastWARC](https://github.com/chatnoir-eu/fastwarc) for high-performance WARC parsing
- ASCII banner generated with [pyfiglet](https://github.com/pwaller/pyfiglet)
