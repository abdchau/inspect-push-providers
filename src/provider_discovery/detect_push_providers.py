"""
Detect push notification providers (e.g. OneSignal, Braze) in the contents of
deduplicated files from output/ssdeep-comparison/deduplicated.json.
Reads dataset/known-providers.json and outputs file-to-providers mapping and summary.
"""

import json
import logging
import os
import re

from tqdm import tqdm

from constants import ALL_OUT_PATH, DATASET_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paths (run from project root)
DETECTION_OUTPUT_DIR = os.path.join(ALL_OUT_PATH, "push-provider-detection")
DEDUPLICATED_PATH = os.path.join(ALL_OUT_PATH, "ssdeep-comparison", "deduplicated.json")
KNOWN_PROVIDERS_PATH = os.path.join(DATASET_PATH, "known-providers.json")


def load_deduplicated(path: str) -> list[str]:
    """Load list of deduplicated file paths from JSON."""
    with open(path, "r") as f:
        return json.load(f)


def load_known_providers(path: str) -> list[str]:
    """Load list of known provider identifiers from JSON."""
    with open(path, "r") as f:
        return json.load(f)


def is_push_related(content: str) -> bool:
    """
    Return True if content shows signs of handling push notifications
    (e.g. push event listeners, PushManager, showNotification).
    """
    c = content.lower()
    if "addEventListener" in c and "push" in c:
        return True
    if "pushmanager" in c:
        return True
    if "pushsubscription" in c or "push subscription" in c:
        return True
    if "shownotification" in c:
        return True
    if "notificationclick" in c:
        return True
    if "pushevent" in c:
        return True
    return False


def _is_whole_word(content: str, needle: str) -> bool:
    """True if needle appears in content as a whole word (not part of a larger word)."""
    escaped = re.escape(needle)
    return bool(re.search(r"\b" + escaped + r"\b", content, re.IGNORECASE))


def detect_providers_in_file(content: str, providers: list[str]) -> list[str]:
    """
    Return list of provider names that appear in content (case-insensitive)
    as whole words only (not as part of a larger identifier).
    Also matches common domain form (provider.com) when provider has no dots.
    """
    found: list[str] = []
    for p in providers:
        if _is_whole_word(content, p):
            found.append(p)
            continue
        # Optional domain: e.g. onesignal -> onesignal.com (as whole word)
        if "." not in p and _is_whole_word(content, p + ".com"):
            found.append(p)
    return found


def main() -> None:
    if not os.path.isfile(DEDUPLICATED_PATH):
        logger.error("Deduplicated list not found: %s", DEDUPLICATED_PATH)
        return
    if not os.path.isfile(KNOWN_PROVIDERS_PATH):
        logger.error("Known providers not found: %s", KNOWN_PROVIDERS_PATH)
        return

    paths = load_deduplicated(DEDUPLICATED_PATH)
    providers = load_known_providers(KNOWN_PROVIDERS_PATH)
    logger.info("Loaded %d paths and %d known providers", len(paths), len(providers))

    file_to_providers: dict[str, list[str]] = {}
    missing: list[str] = []
    push_related_files = 0
    not_push_related_files = 0

    for rel_path in tqdm(paths):
        full_path = os.path.join(ALL_OUT_PATH, rel_path)
        if not os.path.isfile(full_path):
            missing.append(rel_path)
            file_to_providers[rel_path] = []
            continue
        try:
            with open(full_path, "rb") as f:
                raw = f.read()
            content = raw.decode("utf-8", errors="replace")
        except OSError:
            file_to_providers[rel_path] = []
            continue
        if not content.strip():
            file_to_providers[rel_path] = []
            continue
        if not is_push_related(content):
            not_push_related_files += 1
            file_to_providers[rel_path] = []
            continue
        push_related_files += 1
        detected = detect_providers_in_file(content, providers)
        file_to_providers[rel_path] = detected

    if missing:
        logger.warning("Missing files: %d", len(missing))

    os.makedirs(DETECTION_OUTPUT_DIR, exist_ok=True)
    file_to_providers_path = os.path.join(
        DETECTION_OUTPUT_DIR, "file-to-providers.json"
    )
    file_to_providers_with_hits = {
        p: plist for p, plist in file_to_providers.items() if plist
    }
    with open(file_to_providers_path, "w") as f:
        json.dump(file_to_providers_with_hits, f, indent=2)
    logger.info("Wrote %s", file_to_providers_path)

    # Summary: per-provider count; files_with_any/files_with_none apply only to push-related files
    provider_count: dict[str, int] = {p: 0 for p in providers}
    files_with_any = 0
    for rel_path, plist in file_to_providers.items():
        if plist:
            files_with_any += 1
            for p in plist:
                provider_count[p] = provider_count.get(p, 0) + 1
    files_with_none = push_related_files - files_with_any

    nonzero = [(p, c) for p, c in provider_count.items() if c > 0]
    per_provider_count_nonzero = dict(sorted(nonzero, key=lambda x: -x[1]))

    summary = {
        "per_provider_count": provider_count,
        "per_provider_count_nonzero": per_provider_count_nonzero,
        "push_related_files": push_related_files,
        "not_push_related_files": not_push_related_files,
        "files_with_at_least_one_provider": files_with_any,
        "files_with_no_provider": files_with_none,
        "total_files": len(file_to_providers),
        "missing_files": len(missing),
    }
    summary_path = os.path.join(DETECTION_OUTPUT_DIR, "provider-summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    logger.info("Wrote %s", summary_path)

    logger.info("Push-related files: %d", push_related_files)
    logger.info("Not push-related files: %d", not_push_related_files)
    logger.info("Files with at least one provider: %d", files_with_any)
    logger.info("Files with no provider: %d", files_with_none)
    if per_provider_count_nonzero:
        logger.info("Per-provider counts (non-zero): %s", per_provider_count_nonzero)


if __name__ == "__main__":
    main()
