"""Tests for cc_pointer_miner.runner"""
import tempfile
from pathlib import Path
import pytest
from cc_pointer_miner.runner import (
    ShardSpec,
    load_json,
    atomic_write_text,
    build_argparser,
    main,
)


def test_shard_spec_owns():
    shard = ShardSpec(shard_id=0, shard_count=4)
    assert shard.owns(0) is True
    assert shard.owns(4) is True
    assert shard.owns(1) is False
    shard1 = ShardSpec(shard_id=1, shard_count=4)
    assert shard1.owns(1) is True
    assert shard1.owns(5) is True


def test_load_json_missing_default():
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "missing.json"
        assert load_json(p, default={"x": 1}) == {"x": 1}


def test_load_json_exists():
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "data.json"
        p.write_text('{"a": 2}', encoding="utf-8")
        assert load_json(p, default={}) == {"a": 2}


def test_atomic_write_text():
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "out.txt"
        atomic_write_text(p, "hello")
        assert p.read_text() == "hello"
        # .tmp should be gone
        assert not (p.with_suffix(p.suffix + ".tmp")).exists()


def test_build_argparser():
    ap = build_argparser()
    args = ap.parse_args([
        "--crawl", "CC-MAIN-2025-10",
        "--warc-list", "warc_list.txt",
    ])
    assert args.crawl == "CC-MAIN-2025-10"
    assert args.warc_list == "warc_list.txt"
    assert args.shard_id == 0
    assert args.shard_count == 1
    assert args.bloom_capacity == 50_000_000
    assert args.require_200 is False
    args2 = ap.parse_args([
        "--crawl", "X", "--warc-list", "L",
        "--require-200", "--skip-noai", "--bloom-capacity", "1000",
    ])
    assert args2.require_200 is True
    assert args2.skip_noai is True
    assert args2.bloom_capacity == 1000


def test_cli_help():
    """CLI --help exits 0."""
    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])
    assert exc_info.value.code == 0


def test_run_empty_warc_list_completes():
    """With empty warc list for this shard, run() completes without error."""
    with tempfile.TemporaryDirectory() as d:
        work = Path(d) / "work"
        warc_list = Path(d) / "warc_list.txt"
        warc_list.write_text("", encoding="utf-8")
        ap = build_argparser()
        args = ap.parse_args([
            "--crawl", "CC-MAIN-2025-10",
            "--warc-list", str(warc_list),
            "--work-dir", str(work),
            "--shard-id", "0", "--shard-count", "1",
            "--bloom-capacity", "1000",
        ])
        from cc_pointer_miner.runner import run
        exit_code = run(args)
        assert exit_code == 0
