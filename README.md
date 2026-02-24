# OpenCrawl

**OpenCrawl** is a high-performance, distributed miner for extracting Creative Commons licensed content from Common Crawl WARC files.

## Features

- **Pointer-only output**: No document text stored, just JSONL pointers + metadata
- **Strict & ethical**: Only accepts strong license evidence from `<head>` (link/meta tags, JSON-LD)
- **Bloom filter dedup**: Memory-efficient deduplication across URLs
- **Crash-resumable**: Per-WARC checkpoints, safe restarts
- **Distributed**: Run multiple workers across machines with shared bloom sync
- **Fast**: Streaming architecture, two-stage reading, connection pooling

## Installation

```bash
git clone https://github.com/alexhergomz/opencrawl.git
cd opencrawl
pip install -e .
# Or just: pip install fastwarc requests
```

## Quick Start

1. Create a WARC list file:

```bash
# List of WARC URLs (one per line)
echo "https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-10/segments/.../warc/..." > warc_list.txt
```

2. Run the miner:

```bash
python -m opencrawl \
  --crawl CC-MAIN-2024-10 \
  --warc-list warc_list.txt \
  --work-dir work \
  --shard-id 0 --shard-count 10 \
  --require-200 --require-html
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

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--crawl` | Crawl ID (e.g., CC-MAIN-2024-10) | (required) |
| `--warc-list` | Path to WARC list file | (required) |
| `--work-dir` | Working directory | `work` |
| `--shard-id` | Shard ID (0 to shard-count-1) | 0 |
| `--shard-count` | Total number of shards | 1 |
| `--require-200` | Only accept HTTP 200 responses | No |
| `--require-html` | Only accept text/html | No |
| `--skip-noai` | Skip pages with noai/noimageai robots meta | No |
| `--reject-jsonld` | Reject JSON-LD license evidence | No |
| `--bloom-capacity` | Expected unique URLs per shard | 50M |
| `--workers` | Number of parallel workers | 1 |
| `--sync-url` | HTTP URL for shared bloom sync | None |
| `--no-verify-ssl` | Skip SSL verification | No |

## Distributed Processing

Run multiple workers across machines:

```bash
# Worker 1 (machine 1)
python -m opencrawl --shard-id 0 --shard-count 4 --sync-url http://server/bloom.bloom ...

# Worker 2 (machine 2)
python -m opencrawl --shard-id 1 --shard-count 4 --sync-url http://server/bloom.bloom ...

# Worker 3 (machine 3)
python -m opencrawl --shard-id 2 --shard-count 4 --sync-url http://server/bloom.bloom ...

# Worker 4 (machine 4)
python -m opencrawl --shard-id 3 --shard-count 4 --sync-url http://server/bloom.bloom ...
```

For the sync URL, you need a simple HTTP server:

```bash
# On a shared server
mkdir -p /var/www/html/crawl
python -m http.server 8080 --directory /var/www/html/crawl
# Use: --sync-url http://yourserver:8080/crawl/shared.bloom
```

## Performance

- ~90 seconds per WARC (~35k records) on typical network
- ~400 records/second throughput
- Network I/O bound (~20-25% CPU)
- Bloom filter: ~6MB per 50M URLs

## Architecture

1. **Streaming**: WARC records processed one at a time, low memory
2. **Two-stage read**: First 128KB for fast prefilter, full body only if passed
3. **Bloom dedup**: Check canonical URL before expensive extraction
4. **Checkpoint per WARC**: Safe crash recovery

## License

MIT License - See LICENSE file.

## Credits

Built on top of [FastWARC](https://github.com/chatnoir-eu/fastwarc) for high-performance WARC parsing.
