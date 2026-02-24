"""Tests for opencrawl.extract"""
import pytest
from opencrawl.extract import (
    canonicalize_url,
    fast_prefilter,
    extract_head,
    detect_noai,
    parse_cc_uri,
    strict_extract,
)


class TestCanonicalizeUrl:
    def test_lowercase_scheme_and_host(self):
        assert canonicalize_url("HTTPS://Example.COM/Path") == "https://example.com/Path"

    def test_strip_fragment(self):
        assert canonicalize_url("http://a.com/p#section") == "http://a.com/p"

    def test_strip_tracking_params(self):
        u = canonicalize_url("http://a.com/p?utm_source=x&keep=1&utm_medium=y")
        assert "utm_source" not in u and "utm_medium" not in u and "keep=1" in u

    def test_strip_query_option(self):
        assert canonicalize_url("http://a.com/p?q=1", strip_query=True) == "http://a.com/p"

    def test_default_scheme(self):
        # urlsplit with no scheme may leave scheme empty; code uses "http" if no scheme
        assert canonicalize_url("http://a.com/") == "http://a.com/"


class TestFastPrefilter:
    def test_cc_url_positive(self):
        assert fast_prefilter(b"See https://creativecommons.org/licenses/by/4.0/", 5000) is True
        assert fast_prefilter(b"x https://creativecommons.org/publicdomain/zero/1.0/ y", 5000) is True

    def test_hints_positive(self):
        assert fast_prefilter(b'<link rel="license" href="...">', 5000) is True
        assert fast_prefilter(b"creativecommons license", 5000) is True
        assert fast_prefilter(b'application/ld+json {"license":"https://creativecommons.org/..."}', 5000) is True

    def test_negative(self):
        assert fast_prefilter(b"<html><body>No license here</body></html>", 5000) is False

    def test_respects_max_scan(self):
        html = b"x" * 100_000 + b"https://creativecommons.org/licenses/by/4.0/"
        assert fast_prefilter(html, max_scan=128_000) is True
        assert fast_prefilter(html, max_scan=50_000) is False


class TestExtractHead:
    def test_full_head(self):
        html = b"<html><head><title>X</title><meta charset=utf-8></head><body></body></html>"
        head = extract_head(html, 100_000)
        assert b"<title>X</title>" in head and b"<body>" not in head

    def test_no_closing_head_tag(self):
        html = b"<html><head><meta charset=utf-8>"
        head = extract_head(html, 1000)
        assert b"<meta" in head


class TestDetectNoai:
    def test_noai_in_content(self):
        assert detect_noai(b'<meta name="robots" content="noai, noindex">') is True
        assert detect_noai(b"noimageai") is True

    def test_negative(self):
        assert detect_noai(b"<meta name=\"robots\" content=\"index, follow\">") is False


class TestParseCcUri:
    def test_license_with_trailing_slash(self):
        abbr, ver, kind = parse_cc_uri("https://creativecommons.org/licenses/by/4.0/")
        assert abbr == "cc-by" and ver == "4.0" and kind == "licenses"

    def test_license_without_trailing_slash(self):
        """Many sites omit trailing slash; we should still parse."""
        abbr, ver, kind = parse_cc_uri("https://creativecommons.org/licenses/by/4.0")
        assert abbr == "cc-by" and ver == "4.0" and kind == "licenses"

    def test_public_domain(self):
        abbr, ver, kind = parse_cc_uri("https://creativecommons.org/publicdomain/zero/1.0/")
        assert abbr == "cc-zero" and ver == "1.0" and kind == "publicdomain"

    def test_invalid_returns_none(self):
        assert parse_cc_uri("https://example.com/") == (None, None, None)
        assert parse_cc_uri("https://creativecommons.org/licenses/") == (None, None, None)


class TestStrictExtract:
    def test_link_rel_license(self):
        html = b"""<!DOCTYPE html><html><head>
<link rel="license" href="https://creativecommons.org/licenses/by/4.0/">
</head><body></body></html>"""
        r = strict_extract(html, max_head=100000, max_pii_scan=100000)
        assert r.ok is True
        assert r.abbr == "cc-by" and r.version == "4.0"
        assert r.location == "head_link"

    def test_meta_content(self):
        html = b"""<!DOCTYPE html><html><head>
<meta name="license" content="https://creativecommons.org/licenses/by-sa/4.0/">
</head><body></body></html>"""
        r = strict_extract(html, max_head=100000, max_pii_scan=100000)
        assert r.ok is True
        assert r.abbr == "cc-by-sa"

    def test_jsonld(self):
        html = b"""<!DOCTYPE html><html><head>
<script type="application/ld+json">{"@context":"https://schema.org","license":"https://creativecommons.org/licenses/by/4.0/"}</script>
</head><body></body></html>"""
        r = strict_extract(html, max_head=100000, max_pii_scan=100000, accept_jsonld=True)
        assert r.ok is True
        assert r.location == "head_jsonld"

    def test_reject_jsonld_when_disabled(self):
        html = b"""<!DOCTYPE html><html><head>
<script type="application/ld+json">{"license":"https://creativecommons.org/licenses/by/4.0/"}</script>
</head><body></body></html>"""
        r = strict_extract(html, max_head=100000, max_pii_scan=100000, accept_jsonld=False)
        assert r.ok is False

    def test_pii_suspected_skipped(self):
        html = b"""<!DOCTYPE html><html><head>
<link rel="license" href="https://creativecommons.org/licenses/by/4.0/">
</head><body>Contact: foo@example.com</body></html>"""
        r = strict_extract(html, max_head=100000, max_pii_scan=100000)
        assert r.ok is False
        assert r.pii_suspected is True

    def test_no_license_returns_not_ok(self):
        r = strict_extract(b"<html><head></head><body></body></html>", max_head=10000, max_pii_scan=10000)
        assert r.ok is False

    def test_uri_without_trailing_slash_accepted(self):
        html = b"""<!DOCTYPE html><html><head>
<link rel="license" href="https://creativecommons.org/licenses/by-nc/4.0">
</head><body></body></html>"""
        r = strict_extract(html, max_head=100000, max_pii_scan=100000)
        assert r.ok is True
        assert r.abbr == "cc-by-nc" and r.version == "4.0"
