"""
Compare files in output/unknown-providers/ for similarity using ssdeep fuzzy hashing.
Reads output/unknown-providers-index.json (URL -> index number; None = not downloaded)
for URL mapping. Only successfully downloaded files (non-null index) are included.
Emits pairs, clusters, and a deduplicated file list.
"""

import json
import logging
import os
import sys
from itertools import combinations

import ssdeep
from tqdm import tqdm

from constants import ALL_OUT_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default paths (run from project root)
SSDEEP_OUTPUT_DIR = os.path.join(ALL_OUT_PATH, "ssdeep-comparison")
PROVIDERS_DIR = os.path.join(ALL_OUT_PATH, "unknown-providers")
INDEX_PATH = os.path.join(ALL_OUT_PATH, "unknown-providers-index.json")

# Similarity threshold: >= 90 = near-duplicate, include in clusters
SIMILARITY_THRESHOLD = 90


def load_index(index_path: str) -> tuple[dict[str, list[str]], set[str]]:
    """
    Load URL -> index from unknown-providers-index.json. Index is an int (file is
    unknown-providers/{index}.js) or None (download failed; skipped).
    Returns (path_to_urls, set of normalized paths) for successfully downloaded files only.
    """
    with open(index_path, "r") as f:
        index = json.load(f)
    path_to_urls: dict[str, list[str]] = {}
    for url, value in index.items():
        if value is None:
            continue
        # value is the index number; file is unknown-providers/{index}.js
        rel_path = f"unknown-providers/{value}.js"
        if rel_path not in path_to_urls:
            path_to_urls[rel_path] = []
        path_to_urls[rel_path].append(url)
    return path_to_urls, set(path_to_urls.keys())


def hash_all_files(
    providers_dir: str, path_to_urls: dict[str, list[str]]
) -> tuple[dict[str, str], list[str]]:
    """
    Hash every .js file under providers_dir. Use path_to_urls to know which paths exist;
    also scan directory for any .js not in records.
    Returns (path -> ssdeep_hash, list of paths that could not be hashed).
    """
    path_to_hash: dict[str, str] = {}
    no_hash: list[str] = []

    # Collect all .js paths: from records (relative to output) and from disk
    all_paths = set(path_to_urls.keys())
    if os.path.isdir(providers_dir):
        for name in os.listdir(providers_dir):
            if name.endswith(".js"):
                rel = f"unknown-providers/{name}"
                all_paths.add(rel)

    for rel_path in tqdm(sorted(all_paths), desc="Hashing files"):
        full_path = os.path.join(ALL_OUT_PATH, rel_path)
        if not os.path.isfile(full_path):
            no_hash.append(rel_path)
            continue
        try:
            with open(full_path, "rb") as f:
                data = f.read()
            if not data:
                no_hash.append(rel_path)
                continue
            h = ssdeep.hash(data)
            if not h:
                no_hash.append(rel_path)
                continue
            path_to_hash[rel_path] = h
        except ssdeep.InternalError:
            no_hash.append(rel_path)
        except OSError:
            no_hash.append(rel_path)

    return path_to_hash, no_hash


def compare_hashes(
    path_to_hash: dict[str, str],
    path_to_urls: dict[str, list[str]],
    threshold: int,
) -> list[dict]:
    """
    Compare all pairs of hashes; return list of pairs with score >= threshold.
    Each item: { "file_a": path, "file_b": path, "score": int, "urls_a": [...], "urls_b": [...] }.
    """
    paths = list(path_to_hash.keys())
    pairs: list[dict] = []
    for path_a, path_b in tqdm(
        combinations(paths, 2),
        total=(len(paths) * (len(paths) - 1)) // 2,
        desc="Comparing hashes",
    ):
        score = ssdeep.compare(path_to_hash[path_a], path_to_hash[path_b])
        if score >= threshold:
            pairs.append(
                {
                    "file_a": path_a,
                    "file_b": path_b,
                    "score": score,
                    "urls_a": path_to_urls.get(path_a, []),
                    "urls_b": path_to_urls.get(path_b, []),
                }
            )
    return pairs


def build_clusters(pairs: list[dict], path_to_urls: dict[str, list[str]]) -> list[dict]:
    """
    Build clusters from pairs using union-find. Each cluster has representative, members, urls.
    Representative = first path when sorted (deterministic).
    """
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        if x not in parent:
            parent[x] = x
        # Iterative find with path compression to avoid RecursionError on large sets
        stack: list[str] = []
        while parent[x] != x:
            stack.append(x)
            x = parent[x]
        root = x
        for node in stack:
            parent[node] = root
        return root

    def union(x: str, y: str) -> None:
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    for p in pairs:
        union(p["file_a"], p["file_b"])

    roots: dict[str, list[str]] = {}
    for path in parent:
        r = find(path)
        if r not in roots:
            roots[r] = []
        roots[r].append(path)

    clusters = []
    for rep, members in roots.items():
        members_sorted = sorted(members)
        representative = members_sorted[0]
        all_urls = []
        for m in members_sorted:
            all_urls.extend(path_to_urls.get(m, []))
        clusters.append(
            {
                "representative": representative,
                "members": members_sorted,
                "urls": list(dict.fromkeys(all_urls)),
            }
        )
    return clusters


def build_deduplicated_list(
    path_to_hash: dict[str, str],
    clusters: list[dict],
) -> list[str]:
    """
    One path per cluster (representative). Files that were never in any pair
    are their own cluster (single member).
    """
    in_cluster = set()
    for c in clusters:
        for m in c["members"]:
            in_cluster.add(m)
    dedup = [c["representative"] for c in clusters]
    for path in sorted(path_to_hash.keys()):
        if path not in in_cluster:
            dedup.append(path)
    return sorted(dedup)


def main() -> None:
    threshold = SIMILARITY_THRESHOLD
    if len(sys.argv) > 1:
        try:
            threshold = int(sys.argv[1])
        except ValueError:
            pass

    if not os.path.isfile(INDEX_PATH):
        logger.error("Index not found: %s", INDEX_PATH)
        sys.exit(1)

    path_to_urls, _ = load_index(INDEX_PATH)
    path_to_hash, no_hash = hash_all_files(PROVIDERS_DIR, path_to_urls)

    os.makedirs(SSDEEP_OUTPUT_DIR, exist_ok=True)

    if no_hash:
        no_hash_path = os.path.join(SSDEEP_OUTPUT_DIR, "ssdeep-no-hash.json")
        with open(no_hash_path, "w") as f:
            json.dump(no_hash, f, indent=2)
        logger.info(
            "Wrote %d paths that could not be hashed to %s", len(no_hash), no_hash_path
        )

    pairs = compare_hashes(path_to_hash, path_to_urls, threshold)
    clusters = build_clusters(pairs, path_to_urls)
    deduplicated = build_deduplicated_list(path_to_hash, clusters)

    # Log cluster stats
    sizes = [len(c["members"]) for c in clusters]
    size_dist: dict[int, int] = {}
    for s in sizes:
        size_dist[s] = size_dist.get(s, 0) + 1
    logger.info(
        "Cluster size distribution: %s",
        dict(sorted(size_dist.items())),
    )
    if clusters:
        max_size = max(sizes)
        largest = next(c for c in clusters if len(c["members"]) == max_size)
        logger.info(
            "Largest cluster: %d members (representative: %s)",
            max_size,
            largest["representative"],
        )
    files_in_clusters = sum(len(c["members"]) for c in clusters)
    singletons = len(path_to_hash) - files_in_clusters
    logger.info(
        "Files in clusters: %d; singletons (no similar match): %d",
        files_in_clusters,
        singletons,
    )

    pairs_path = os.path.join(SSDEEP_OUTPUT_DIR, "ssdeep-pairs.json")
    clusters_path = os.path.join(SSDEEP_OUTPUT_DIR, "ssdeep-clusters.json")
    dedup_path = os.path.join(SSDEEP_OUTPUT_DIR, "deduplicated.json")

    with open(pairs_path, "w") as f:
        json.dump(pairs, f, indent=2)
    with open(clusters_path, "w") as f:
        json.dump(clusters, f, indent=2)
    with open(dedup_path, "w") as f:
        json.dump(deduplicated, f, indent=2)

    logger.info("Threshold: %s", threshold)
    logger.info("Hashed: %d files", len(path_to_hash))
    logger.info("Pairs (score >= %s): %d", threshold, len(pairs))
    logger.info("Clusters: %d", len(clusters))
    logger.info("Deduplicated file count: %d", len(deduplicated))
    logger.info("Wrote %s", pairs_path)
    logger.info("Wrote %s", clusters_path)
    logger.info("Wrote %s", dedup_path)


if __name__ == "__main__":
    main()
