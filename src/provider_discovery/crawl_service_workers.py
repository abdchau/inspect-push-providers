import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm
import requests
import re
from urllib.parse import urlparse
import logging

from constants import ALL_OUT_PATH, DATASET_PATH


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CrawlResult:
    SUCCESS = 0
    ALREADY_CRAWLED = 1
    FAIL = 2


def download_with_record(
    urls: list[str], out_path: str = os.path.join(ALL_OUT_PATH, "unknown-providers")
):
    try:
        with open(os.path.join(ALL_OUT_PATH, "unknown-providers-index.json"), "r") as f:
            crawled_index = json.load(f)
    except FileNotFoundError:
        crawled_index = {}

    count = len([k for k in crawled_index if crawled_index[k] is not None])
    urls_to_fetch = [u for u in urls if u not in crawled_index]
    index_path = os.path.join(ALL_OUT_PATH, "unknown-providers-index.json")
    lock = Lock()

    os.makedirs(out_path, exist_ok=True)

    def process_result(url, crawl_result, content):
        nonlocal count
        with lock:
            if crawl_result == CrawlResult.ALREADY_CRAWLED:
                return

            if crawl_result == CrawlResult.SUCCESS:
                path = os.path.join(out_path, f"{count}.js")
                with open(path, "wb") as f:
                    f.write(content)
                crawled_index[url] = count
                count += 1
            elif crawl_result == CrawlResult.FAIL:
                crawled_index[url] = None
            # ALREADY_CRAWLED: not possible for urls_to_fetch
            with open(index_path, "w") as f:
                json.dump(crawled_index, f, indent=2)

    with ThreadPoolExecutor(max_workers=30) as executor:
        future_to_url = {
            executor.submit(fetch_url, url, crawled_index): url for url in urls_to_fetch
        }
        for future in tqdm(as_completed(future_to_url), total=len(future_to_url)):
            url = future_to_url[future]
            try:
                crawl_result, content = future.result()
                process_result(url, crawl_result, content)
            except Exception as e:
                logger.exception(e)
                process_result(url, CrawlResult.FAIL, None)

    return count


def fetch_url(url, crawled_index):
    if url in crawled_index:
        return (CrawlResult.ALREADY_CRAWLED, None)
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            return (CrawlResult.SUCCESS, response.content)
        else:
            logger.debug(f"{url} is not valid")
            logger.debug(response.status_code)
    except requests.exceptions.RequestException as e:
        logger.debug(f"{url} is not valid")
        logger.debug(e)
    return (CrawlResult.FAIL, None)


def matches_pattern(pattern, string):
    return bool(re.search(pattern, string))


def check_for_static_or_cdn():
    with open(
        os.path.join(
            DATASET_PATH, "serviceworkers_origins_urls_and_imported_scripts.json"
        ),
        "r",
    ) as f:
        data = json.load(f)

    static_or_cdn = set()
    no_static_or_cdn = set()

    not_interesting = {
        "webpush",
        "toolbox",
        "workbox",
        "dosugbar",
        "glideapp",
        "superstatic",
        "imghaste",
        "jsdelivr",
        "firebase",
        "cloudflare",
        "static.im-cdn.com/mjc/storefront/",
        "cdn-my.promizer.com/api/public/sdk/platforms/",
        "netlify",
    }

    regexes = [
        # glideapp service workers
        # r"https://app.*/static/js/sw-common-[0-9a-f]{40}.js",
        r"/static/js/sw-common-[0-9a-f]{40}.js",
    ]

    for website in tqdm(data):
        domain = urlparse(website).netloc

        for sw in data[website]:

            for script in data[website][sw]:
                script = script.lower()
                flag = False
                for not_interesting_ in not_interesting:
                    if not_interesting_ in script:
                        flag = True
                        break

                for regex in regexes:
                    if matches_pattern(regex, script):
                        flag = True
                        break

                if flag:
                    continue

                if "static" in script or "cdn" in script:
                    static_or_cdn.add(script)
                else:
                    no_static_or_cdn.add(script)

    logger.info(f"Static or CDN: {len(static_or_cdn)}")
    logger.info(f"No static or CDN: {len(no_static_or_cdn)}")

    with open(os.path.join(ALL_OUT_PATH, "static_or_cdn.json"), "w") as f:
        json.dump(sorted(static_or_cdn), f, indent=2)

    with open(os.path.join(ALL_OUT_PATH, "no_static_or_cdn.json"), "w") as f:
        json.dump(sorted(no_static_or_cdn), f, indent=2)


def remove_known_providers():
    with open(os.path.join(ALL_OUT_PATH, "static_or_cdn.json"), "r") as f:
        static_or_cdn_sws = set(json.load(f))

    with open(os.path.join(ALL_OUT_PATH, "no_static_or_cdn.json"), "r") as f:
        no_static_or_cdn_sws = set(json.load(f))

    with open(os.path.join(DATASET_PATH, "known-providers.json"), "r") as f:
        known_providers = json.load(f)

    known_providers.sort()
    instances_of_providers = {k: 0 for k in known_providers}
    instances_of_providers["unknown"] = 0

    no_known_provider = set()
    for sw in tqdm(static_or_cdn_sws.union(no_static_or_cdn_sws)):
        flag = False
        for provider in known_providers:
            if provider in sw:
                instances_of_providers[provider] = (
                    instances_of_providers.get(provider, 0) + 1
                )
                flag = True
                break
        if not flag:
            instances_of_providers["unknown"] += 1
            no_known_provider.add(sw)

    # logger.info(f"Instances of providers: {instances_of_providers}")
    with open(os.path.join(ALL_OUT_PATH, "instances_of_providers.json"), "w") as f:
        json.dump(instances_of_providers, f, indent=2)

    with open(os.path.join(ALL_OUT_PATH, "no_known_provider.json"), "w") as f:
        json.dump(sorted(no_known_provider), f, indent=2)


def download_unknown_providers():
    with open(os.path.join(ALL_OUT_PATH, "no_known_provider.json"), "r") as f:
        urls = json.load(f)

    count = download_with_record(urls)
    logger.info(f"Downloaded {count} unknown providers")


def main():
    check_for_static_or_cdn()
    remove_known_providers()
    download_unknown_providers()
