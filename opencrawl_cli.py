#!/usr/bin/env python3
"""
OpenCrawl - Creative Commons license miner for Common Crawl
"""

import sys
from opencrawl.runner import main

BANNER = """
╔═══════════════════════════════════════════════════════════════════════╗
║                                                                       ║
║   ██████╗ ███████╗████████╗██████╗  ██████╗                       ║
║   ██╔══██╗██╔════╝╚══██╔══╝██╔══██╗██╔═══██╗                      ║
║   ██████╔╝█████╗     ██║   ██████╔╝██║   ██║                      ║
║   ██╔══██╗██╔══╝     ██║   ██╔══██╗██║   ██║                      ║
║   ██║  ██║███████╗   ██║   ██║  ██║╚██████╔╝                      ║
║   ╚═╝  ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝ ╚═════╝                       ║
║                                                                       ║
║   >>>  O P E N C R A W L  <<<                                         ║
║   Creative Commons License Miner for Common Crawl                     ║
║                                                                       ║
╚═══════════════════════════════════════════════════════════════════════╝
"""


def run():
    print(BANNER)
    raise SystemExit(main())


if __name__ == "__main__":
    run()
