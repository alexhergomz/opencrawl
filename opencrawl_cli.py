#!/usr/bin/env python3
"""
OpenCrawl - Creative Commons license miner for Common Crawl
"""

import sys
from opencrawl.runner import main


def _make_banner() -> str:
    try:
        from pyfiglet import Figlet

        big = Figlet(font="ansi_shadow").renderText("OPENCRAWL").rstrip("\n")
    except Exception:
        big = "OPENCRAWL"

    return f"""
┌──────────────────────────────────────────────────────────────────────────────┐
{big}
>>>  O P E N C R A W L  <<<
Creative Commons License Miner for Common Crawl
└──────────────────────────────────────────────────────────────────────────────┘
""".lstrip("\n")


BANNER = _make_banner()


def run():
    try:
        print(BANNER)
    except UnicodeEncodeError:
        print("OpenCrawl - Creative Commons License Miner for Common Crawl")
    raise SystemExit(main())


if __name__ == "__main__":
    run()
