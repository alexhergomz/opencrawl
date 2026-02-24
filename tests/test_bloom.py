"""Tests for opencrawl.bloom"""
import tempfile
from pathlib import Path
import pytest
from opencrawl.bloom import BloomConfig, BloomFilter


def test_bloom_add_and_contains():
    cfg = BloomConfig(capacity=1000, error_rate=1e-5)
    bf = BloomFilter(cfg)
    bf.add(b"https://example.com/page1")
    bf.add(b"https://example.com/page2")
    assert b"https://example.com/page1" in bf
    assert b"https://example.com/page2" in bf
    # Unlikely to be false negative; may rarely be false positive
    assert b"https://example.com/other" not in bf


def test_bloom_save_and_load():
    cfg = BloomConfig(capacity=5000, error_rate=1e-6, seed=b"test_seed")
    bf = BloomFilter(cfg)
    bf.add(b"url1")
    bf.add(b"url2")
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "test.bloom"
        bf.save(path)
        loaded = BloomFilter.load(path)
        assert loaded.config.capacity == cfg.capacity
        assert loaded.config.seed == cfg.seed
        assert b"url1" in loaded
        assert b"url2" in loaded


def test_bloom_load_mismatch_raises():
    cfg = BloomConfig(capacity=1000, error_rate=1e-6)
    bf = BloomFilter(cfg)
    bf.add(b"x")
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "test.bloom"
        bf.save(path)
        # Loading with different capacity should still work (params in file);
        # but if we corrupt n_bytes in file, load should raise.
        raw = path.read_bytes()
        # Truncate bits so n_bytes doesn't match
        first_newline = raw.index(b"\n")
        header = raw[:first_newline]
        bits = raw[first_newline + 1 :]
        path.write_bytes(header + b"\n" + bits[: len(bits) // 2])
        with pytest.raises(ValueError, match="mismatch|corrupt"):
            BloomFilter.load(path)
