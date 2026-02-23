"""
Visit each domain from candidate-unknown-providers-domains.json, record whether
the visit succeeded, then check page content (lowercased) for push-related strings
in priority order: "web push notification", "web push", "notification", "push".
Output per-domain results and aggregate stats.
"""

import json
import logging
import os
import sys
from tqdm import tqdm

import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
DETECTION_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output", "push-provider-detection")
DOMAINS_PATH = os.path.join(
    DETECTION_OUTPUT_DIR, "candidate-unknown-providers-domains.json"
)

# Priority order: first match wins
CONTENT_STRINGS = [
    "web push notification",
    "web push",
    "notification",
    "push",
]

REQUEST_TIMEOUT = 15
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/115.0",
}


def load_domains(path: str) -> list[str]:
    with open(path, "r") as f:
        return json.load(f)


def visit_domain(domain: str) -> tuple[bool, int | None, str]:
    """
    Fetch https://{domain}. Return (success, status_code_or_None, response_text_or_error).
    """
    url = f"https://{domain}" if "://" not in domain else domain
    if not url.startswith("http"):
        url = "https://" + domain
    try:
        r = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT)
        return True, r.status_code, r.text
    except requests.exceptions.RequestException as e:
        return False, None, str(e)


def first_matching_string(content_lower: str) -> str | None:
    """Return the first of CONTENT_STRINGS found in content_lower, or None."""
    for s in CONTENT_STRINGS:
        if s in content_lower:
            return s
    return None


def main() -> None:
    if not os.path.isfile(DOMAINS_PATH):
        logger.error("Domains file not found: %s", DOMAINS_PATH)
        return

    domains = load_domains(DOMAINS_PATH)
    logger.info("Loaded %d domains", len(domains))

    results: list[dict] = []
    visit_ok = 0
    visit_fail = 0
    match_counts: dict[str, int] = {s: 0 for s in CONTENT_STRINGS}
    match_counts["none"] = 0

    for i, domain in enumerate(tqdm(domains)):
        success, status_code, content_or_error = visit_domain(domain)
        entry = {
            "domain": domain,
            "visit_worked": success,
            "status_code": status_code,
        }
        if success:
            visit_ok += 1
            content_lower = content_or_error.lower()
            found = first_matching_string(content_lower)
            entry["content_match"] = found
            if found:
                match_counts[found] += 1
            else:
                match_counts["none"] += 1
        else:
            visit_fail += 1
            entry["error"] = content_or_error[:200]
        results.append(entry)

    stats = {
        "total_domains": len(domains),
        "visit_worked": visit_ok,
        "visit_failed": visit_fail,
        "content_match_counts": match_counts,
    }

    results_with_match = [r for r in results if r.get("visit_worked") and r.get("content_match")]
    out_path = os.path.join(DETECTION_OUTPUT_DIR, "domain-visit-results.json")
    stats_path = os.path.join(DETECTION_OUTPUT_DIR, "domain-visit-stats.json")
    matched_path = os.path.join(DETECTION_OUTPUT_DIR, "domain-visit-matched-domains.json")
    os.makedirs(DETECTION_OUTPUT_DIR, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2)
    with open(matched_path, "w") as f:
        json.dump(results_with_match, f, indent=2)

    logger.info("Wrote %s", out_path)
    logger.info("Wrote %s", stats_path)
    logger.info("Wrote %s (%d domains with content match)", matched_path, len(results_with_match))
    logger.info(
        "Stats: total=%d, visit_worked=%d, visit_failed=%d",
        len(domains),
        visit_ok,
        visit_fail,
    )
    logger.info("Content matches (priority order): %s", match_counts)


if __name__ == "__main__":
    main()
