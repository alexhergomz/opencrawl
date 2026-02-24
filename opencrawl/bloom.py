from __future__ import annotations
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


def _blake_u64(data: bytes, key: bytes) -> int:
    # 64-bit hash using BLAKE2b; fast and in stdlib.
    h = hashlib.blake2b(data, digest_size=8, key=key)
    return int.from_bytes(h.digest(), "little", signed=False)


@dataclass
class BloomConfig:
    capacity: int
    error_rate: float = 1e-6
    seed: bytes = b"cc_pointer_miner"

    def params(self) -> tuple[int, int]:
        # m bits, k hashes
        n = max(1, int(self.capacity))
        p = min(max(self.error_rate, 1e-12), 1e-2)
        m = int(math.ceil(-n * math.log(p) / (math.log(2) ** 2)))
        k = int(max(1, round((m / n) * math.log(2))))
        return m, k


class BloomFilter:
    """Simple, fast Bloom filter with on-disk persistence.

    Notes:
      - False positives possible; false negatives not (unless file corrupted).
      - Persistence stores full bitset and config metadata.
    """

    def __init__(self, config: BloomConfig):
        self.config = config
        self.m_bits, self.k = config.params()
        self.n_bytes = (self.m_bits + 7) // 8
        self.bits = bytearray(self.n_bytes)
        # Precompute per-hash keys to avoid repeated allocations.
        self._keys = [
            hashlib.blake2b(
                config.seed + i.to_bytes(2, "little"), digest_size=16
            ).digest()
            for i in range(self.k)
        ]

    def _indexes(self, item: bytes):
        # Use double-hashing-like scheme but with keyed blake2b for stability.
        for key in self._keys:
            yield _blake_u64(item, key) % self.m_bits

    def add(self, item: bytes) -> None:
        for idx in self._indexes(item):
            byte_i = idx >> 3
            bit_i = idx & 7
            self.bits[byte_i] |= 1 << bit_i

    def add_many(self, items: Iterable[bytes]) -> int:
        """Add multiple items. Returns count of items added (not already in filter)."""
        added = 0
        for item in items:
            if item not in self:
                self.add(item)
                added += 1
        return added

    def filter_new(self, items: Iterable[bytes]) -> list[bytes]:
        """Return only items not already in the filter, adding them as a batch."""
        new_items = []
        for item in items:
            if item not in self:
                new_items.append(item)
                self.add(item)
        return new_items

    def merge(self, other: "BloomFilter") -> None:
        """Merge another bloom filter into this one (bitwise OR)."""
        if self.m_bits != other.m_bits or self.k != other.k:
            raise ValueError("Cannot merge bloom filters with different parameters")
        for i in range(len(self.bits)):
            self.bits[i] |= other.bits[i]

    def __contains__(self, item: bytes) -> bool:
        for idx in self._indexes(item):
            byte_i = idx >> 3
            bit_i = idx & 7
            if not (self.bits[byte_i] & (1 << bit_i)):
                return False
        return True

    def save(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        meta = {
            "capacity": self.config.capacity,
            "error_rate": self.config.error_rate,
            "seed_hex": self.config.seed.hex(),
            "m_bits": self.m_bits,
            "k": self.k,
            "n_bytes": self.n_bytes,
        }
        tmp = path.with_suffix(path.suffix + ".tmp")
        with tmp.open("wb") as f:
            header = (json.dumps(meta) + "\n").encode("utf-8")
            f.write(header)
            f.write(self.bits)
        tmp.replace(path)

    @classmethod
    def load(cls, path: str | Path) -> "BloomFilter":
        path = Path(path)
        with path.open("rb") as f:
            header = f.readline().decode("utf-8", errors="replace").strip()
            try:
                meta = json.loads(header)
            except json.JSONDecodeError:
                # Backward compat: old files used str(dict) and were loaded with eval
                meta = eval(header, {"__builtins__": {}})
            bits = f.read()

        cfg = BloomConfig(
            capacity=int(meta["capacity"]),
            error_rate=float(meta["error_rate"]),
            seed=bytes.fromhex(meta["seed_hex"]),
        )
        bf = cls(cfg)
        # Basic sanity
        if (
            int(meta["m_bits"]) != bf.m_bits
            or int(meta["k"]) != bf.k
            or int(meta["n_bytes"]) != bf.n_bytes
        ):
            raise ValueError(
                "Bloom filter parameters mismatch; delete the bloom file or use matching config."
            )
        if len(bits) != bf.n_bytes:
            raise ValueError("Bloom filter bitset size mismatch/corrupt file.")
        bf.bits[:] = bits
        return bf
