from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Optional, Tuple
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

# --- Fast needles / regexes (bytes) ---
CC_URL_RE = re.compile(
    rb"https?://creativecommons\.org/(licenses|publicdomain)/[^\s\"\'<>]+",
    re.IGNORECASE,
)
HEAD_RE = re.compile(rb"<head\b[^>]*>(.*?)</head\s*>", re.IGNORECASE | re.DOTALL)
LINK_REL_LICENSE_RE = re.compile(
    rb"<link\b[^>]*\brel\s*=\s*[\"']?license[\"']?[^>]*>",
    re.IGNORECASE,
)
META_TAG_RE = re.compile(rb"<meta\b[^>]*>", re.IGNORECASE)
HREF_RE = re.compile(rb"\bhref\s*=\s*([\"'])(.*?)\1", re.IGNORECASE | re.DOTALL)
CONTENT_RE = re.compile(rb"\bcontent\s*=\s*([\"'])(.*?)\1", re.IGNORECASE | re.DOTALL)

JSONLD_BLOCK_RE = re.compile(
    rb"<script\b[^>]*type\s*=\s*([\"'])application/ld\+json\1[^>]*>(.*?)</script\s*>",
    re.IGNORECASE | re.DOTALL,
)
LICENSE_KEY_RE = re.compile(rb"\"license\"\s*:\s*\"(.*?)\"", re.IGNORECASE | re.DOTALL)

EMAIL_RE = re.compile(rb"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE_RE = re.compile(rb"\b(\+?\d[\d\-\s().]{7,}\d)\b")

ROBOTS_META_RE = re.compile(
    rb"<meta\b[^>]*\bname\s*=\s*([\"'])robots\1[^>]*>", re.IGNORECASE
)
NOAI_RE = re.compile(rb"(noai|noimageai)", re.IGNORECASE)

# Parse CC URI (trailing slash optional; many sites omit it)
CC_PARSE_RE = re.compile(
    r"^https?://creativecommons\.org/(licenses|publicdomain)/([^/]+)/([^/]+)/?",
    re.IGNORECASE,
)


def canonicalize_url(url: str, strip_query: bool = False) -> str:
    """Canonicalize URL for dedup.

    Conservative defaults:
      - Lowercase scheme/host
      - Remove fragment
      - Optionally strip query entirely
      - Otherwise strip common tracking params
    """
    try:
        parts = urlsplit(url)
    except Exception:
        return url

    scheme = (parts.scheme or "http").lower()
    netloc = (parts.netloc or "").lower()

    path = parts.path or "/"

    query = parts.query or ""
    if strip_query:
        query = ""
    else:
        # Remove common tracking params
        q = [
            (k, v)
            for (k, v) in parse_qsl(query, keep_blank_values=True)
            if k.lower()
            not in {
                "utm_source",
                "utm_medium",
                "utm_campaign",
                "utm_term",
                "utm_content",
                "gclid",
                "fbclid",
                "msclkid",
            }
        ]
        query = urlencode(q, doseq=True)

    # drop fragment always
    return urlunsplit((scheme, netloc, path, query, ""))


def fast_prefilter(html: bytes, max_scan: int) -> bool:
    chunk = html[:max_scan]
    low = chunk.lower()
    if CC_URL_RE.search(low):
        return True
    # Strong hints
    if b"creativecommons" in low and b"license" in low:
        return True
    if b'rel="license"' in low or b"rel='license'" in low:
        return True
    if b"application/ld+json" in low and b'"license"' in low:
        return True
    return False


def extract_head(html: bytes, max_head: int) -> bytes:
    prefix = html[:max_head]
    m = HEAD_RE.search(prefix)
    if m:
        return m.group(1)
    end = prefix.lower().find(b"</head")
    if end != -1:
        return prefix[:end]
    return prefix


def detect_noai(head: bytes) -> bool:
    # Treat noai/noimageai anywhere in head as signal
    if NOAI_RE.search(head):
        return True
    if ROBOTS_META_RE.search(head) and b"noai" in head.lower():
        return True
    return False


def parse_cc_uri(uri: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    m = CC_PARSE_RE.match(uri)
    if not m:
        return None, None, None
    kind = m.group(1).lower()
    abbr = m.group(2).lower()
    version = m.group(3)
    return f"cc-{abbr}", version, kind


@dataclass
class ExtractResult:
    ok: bool
    license_uri: Optional[str] = None
    abbr: Optional[str] = None
    version: Optional[str] = None
    location: Optional[str] = None
    in_head: bool = False
    disagreement: bool = False
    pii_suspected: bool = False
    robots_noai: bool = False


def strict_extract(
    html: bytes, max_head: int, max_pii_scan: int, accept_jsonld: bool = True
) -> ExtractResult:
    res = ExtractResult(ok=False)

    scan = html[:max_pii_scan]
    if EMAIL_RE.search(scan) or PHONE_RE.search(scan):
        res.pii_suspected = True
        return res

    head = extract_head(html, max_head)
    res.robots_noai = detect_noai(head)

    candidates = []  # (uri, location)

    # 1) <link rel="license" href="...">
    for lm in LINK_REL_LICENSE_RE.finditer(head):
        tag = lm.group(0)
        hm = HREF_RE.search(tag)
        if hm:
            href = hm.group(2).strip()
            uri = href.decode("utf-8", errors="ignore")
            if "creativecommons.org" in uri.lower():
                candidates.append((uri, "head_link"))

    # 2) meta tags: try content= first, then raw cc url search
    for mm in META_TAG_RE.finditer(head):
        tag = mm.group(0)
        cm = CONTENT_RE.search(tag)
        if cm and b"creativecommons.org" in cm.group(2).lower():
            uri = cm.group(2).decode("utf-8", errors="ignore").strip()
            candidates.append((uri, "head_meta"))
        else:
            cc = CC_URL_RE.search(tag)
            if cc:
                uri = cc.group(0).decode("utf-8", errors="ignore")
                candidates.append((uri, "head_meta"))

    # 3) JSON-LD (optional)
    if accept_jsonld:
        for _, block in JSONLD_BLOCK_RE.findall(head):
            for lic_m in LICENSE_KEY_RE.finditer(block):
                uri = lic_m.group(1).decode("utf-8", errors="ignore")
                if "creativecommons.org" in uri.lower():
                    candidates.append((uri, "head_jsonld"))
            cc = CC_URL_RE.search(block)
            if cc:
                uri = cc.group(0).decode("utf-8", errors="ignore")
                candidates.append((uri, "head_jsonld"))

    if not candidates:
        return res

    # De-dup, prefer higher confidence locations
    pref = {"head_link": 0, "head_meta": 1, "head_jsonld": 2}
    best = {}
    for uri, loc in candidates:
        if uri not in best or pref.get(loc, 99) < pref.get(best[uri], 99):
            best[uri] = loc

    parsed = []
    for uri, loc in best.items():
        abbr, ver, _kind = parse_cc_uri(uri)
        if abbr is None or ver is None:
            continue
        if abbr == "cc-unknown":
            continue
        parsed.append((uri, abbr, ver, loc))

    if not parsed:
        return res

    key_set = {(abbr, ver) for _, abbr, ver, _ in parsed}
    if len(key_set) > 1:
        res.disagreement = True
        return res

    uri, abbr, ver, loc = parsed[0]
    res.ok = True
    res.license_uri = uri
    res.abbr = abbr
    res.version = ver
    res.location = loc
    res.in_head = True
    return res
