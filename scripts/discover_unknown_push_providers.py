"""
Discover candidate push notification providers not in the known list by extracting
domains from push-related files that have no known provider.
Reads deduplicated paths and file-to-providers.json; outputs candidate domains
ranked by frequency to candidate-unknown-providers.json.
"""

import json
import logging
import os
import re
import sys
from collections import defaultdict
from urllib.parse import urlparse

# Allow importing from same package (scripts/)
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)
from detect_push_providers import (
    DEDUPLICATED_PATH,
    DETECTION_OUTPUT_DIR,
    OUTPUT_DIR,
    is_push_related,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_TO_PROVIDERS_PATH = os.path.join(DETECTION_OUTPUT_DIR, "file-to-providers.json")

# URL pattern: http(s) or protocol-relative //host/path; stop at whitespace, quote, or common delimiters
URL_PATTERN = re.compile(r"https?://[^\s\"'<>)\]\},;]+|//[^\s\"'<>)\]\},;]+")


# Domains to exclude (generic CDNs, infra, browser APIs)
EXCLUDE_DOMAIN_SUBSTRINGS = (
    "google.",
    "googleapis.",
    "cloudflare",
    "w3.org",
    "w3c.",
    "mozilla.",
    "npm.",
    "jsdelivr",
    "unpkg.",
    "cdnjs.",
    "gstatic.",
    "facebook.",
    "facebook.net",
    "doubleclick",
    "googletagmanager",
    "googlesyndication",
    "youtube.",
    "google-analytics",
    "segment.",
    "segment.io",
    "amazonaws.",
    "cloudfront.",
    "azure.",
    "azureedge.",
    "fastly.",
    "akamai",
    "jsdelivr",
    "bootstrapcdn",
    "jquery",
    "polyfill",
    "sentry.io",
    "sentry-cdn",
    "static.parastorage",  # Wix
    "github.",
    "github.com",
    "apache.",
    "apache.org",
    "angular.",
    "angular.io",
    "stackoverflow.",
    "microsoft.",
    "firebase",
    "nist.gov",
    "turktelekom",
    "tinyurl",
    "vietgiaitri.com",
    "bit.ly",
)


def load_deduplicated(path: str) -> list[str]:
    with open(path, "r") as f:
        return json.load(f)


def load_file_to_providers(path: str) -> dict[str, list[str]]:
    with open(path, "r") as f:
        return json.load(f)


def extract_urls_from_content(content: str) -> list[str]:
    """Return list of URLs found in content (absolute and protocol-relative)."""
    urls: list[str] = []
    for m in URL_PATTERN.finditer(content):
        raw = m.group(0).rstrip(".,;:)")
        if raw.startswith("//"):
            raw = "https:" + raw
        urls.append(raw)
    return urls


def hostname_from_url(url: str) -> str | None:
    try:
        parsed = urlparse(url)
        return parsed.netloc.lower() if parsed.netloc else None
    except Exception:
        return None


def is_excluded_domain(hostname: str) -> bool:
    if not hostname or hostname.startswith("localhost") or "." not in hostname:
        return True
    if (
        "(" in hostname
        or ")" in hostname
        or " " in hostname
        or hostname.startswith(".")
    ):
        return True
    hl = hostname.lower()
    for exc in EXCLUDE_DOMAIN_SUBSTRINGS:
        if exc.lower() in hl:
            return True
    return False


def main() -> None:
    if not os.path.isfile(DEDUPLICATED_PATH):
        logger.error("Deduplicated list not found: %s", DEDUPLICATED_PATH)
        return
    if not os.path.isfile(FILE_TO_PROVIDERS_PATH):
        logger.error("File-to-providers not found: %s", FILE_TO_PROVIDERS_PATH)
        return

    paths = load_deduplicated(DEDUPLICATED_PATH)
    file_to_providers = load_file_to_providers(FILE_TO_PROVIDERS_PATH)
    paths_with_known = set(file_to_providers.keys())
    logger.info(
        "Loaded %d deduplicated paths; %d with a known provider",
        len(paths),
        len(paths_with_known),
    )

    domain_count: dict[str, int] = defaultdict(int)
    domain_example_urls: dict[str, list[str]] = defaultdict(list)
    domain_files: dict[str, set[str]] = defaultdict(set)
    max_examples_per_domain = 3
    push_related_no_provider_count = 0

    for rel_path in paths:
        if rel_path in paths_with_known:
            continue
        full_path = os.path.join(OUTPUT_DIR, rel_path)
        if not os.path.isfile(full_path):
            continue
        try:
            with open(full_path, "rb") as f:
                raw = f.read()
            content = raw.decode("utf-8", errors="replace")
        except OSError:
            continue
        if not content.strip():
            continue
        if not is_push_related(content):
            continue
        push_related_no_provider_count += 1
        urls = extract_urls_from_content(content)
        seen_in_file: set[str] = set()
        for url in urls:
            host = hostname_from_url(url)
            if not host or is_excluded_domain(host):
                continue
            domain_count[host] += 1
            domain_files[host].add(rel_path)
            if (
                host not in seen_in_file
                and len(domain_example_urls[host]) < max_examples_per_domain
            ):
                domain_example_urls[host].append(url)
                seen_in_file.add(host)

    logger.info(
        "Push-related files with no known provider: %d", push_related_no_provider_count
    )

    # Build output: list of { domain, count, example_urls } sorted by count desc
    candidates = []
    for domain, count in sorted(domain_count.items(), key=lambda x: -x[1]):
        candidates.append(
            {
                "domain": domain,
                "count": count,
                "example_urls": domain_example_urls.get(domain, [])[
                    :max_examples_per_domain
                ],
                "files": sorted(domain_files.get(domain, set())),
            }
        )

    out_path = os.path.join(DETECTION_OUTPUT_DIR, "candidate-unknown-providers.json")
    domains_only_path = os.path.join(
        DETECTION_OUTPUT_DIR, "candidate-unknown-providers-domains.json"
    )
    os.makedirs(DETECTION_OUTPUT_DIR, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(
            {
                "push_related_no_provider_files": push_related_no_provider_count,
                "candidates": candidates,
            },
            f,
            indent=2,
        )
    domains_list = sorted([c["domain"] for c in candidates], key=str.lower)
    with open(domains_only_path, "w") as f:
        json.dump(domains_list, f, indent=2)
    logger.info("Wrote %s (%d candidate domains)", out_path, len(candidates))
    logger.info("Wrote %s", domains_only_path)
    if candidates:
        logger.info(
            "Top 10 by count: %s", [(c["domain"], c["count"]) for c in candidates[:10]]
        )


if __name__ == "__main__":
    main()
