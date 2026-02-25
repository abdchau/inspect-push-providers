import json
import os
import pandas as pd
import logging
import re
import requests

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from urllib.parse import urlparse

out_path = "./data/"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ALL_OUT_PATH = "./output/"
DATASET_PATH = "./dataset/"


class CrawlResult:
    SUCCESS = 0
    ALREADY_CRAWLED = 1
    FAIL = 2


def new():
    with open(
        os.path.join(
            ALL_OUT_PATH, "serviceworkers_origins_urls_and_imported_scripts.json"
        ),
        "r",
    ) as f:
        data = json.load(f)

    # logger.info(data)

    # df = pd.DataFrame(data)

    # logger.info(df.head())

    new_data = {}
    for key in list(data.keys())[:100]:
        new_data[key] = data[key]

    with open(os.path.join(ALL_OUT_PATH, "new.json"), "w") as f:
        json.dump(new_data, f, indent=2)


def download_with_record(
    urls: list[str], out_path: str = f".{ALL_OUT_PATH}/unknown-providers/"
):
    try:
        with open(os.path.join(ALL_OUT_PATH, "unknown-providers-index.json"), "r") as f:
            crawled_index = json.load(f)
    except FileNotFoundError:
        crawled_index = {}

    count = len(crawled_index)

    # try:
    #     last_downloaded = list(records.keys())[-1]
    #     resume_idx = urls.index(last_downloaded) + 1
    # except IndexError:
    #     last_downloaded = None
    #     resume_idx = 0

    resume_idx = count

    for url in tqdm(urls[resume_idx:]):
        crawl_result = download_file(
            url,
            os.path.join(out_path, f"{count}.js"),
            crawled_index,
        )
        if crawl_result == CrawlResult.SUCCESS:
            crawled_index[url] = count
            count += 1

        elif crawl_result == CrawlResult.ALREADY_CRAWLED:
            continue
        elif crawl_result == CrawlResult.FAIL:
            crawled_index[url] = None

        with open(os.path.join(ALL_OUT_PATH, "unknown-providers-index.json"), "w") as f:
            json.dump(crawled_index, f, indent=2)

    return count


def download_file(url, path, crawled_index):
    logger.debug(f"Downloading {url} to {path}")
    if url in crawled_index:
        logger.debug(f"{url} already crawled")
        return CrawlResult.ALREADY_CRAWLED

    if os.path.exists(path):
        # logger.error(f"{path} already exists")
        raise FileExistsError(f"{path} already exists")

    os.makedirs(os.path.dirname(path), exist_ok=True)

    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            with open(path, "wb") as f:
                f.write(response.content)
            return CrawlResult.SUCCESS
        else:
            logger.debug(f"{url} is not valid")
            logger.debug(response.status_code)
            # exit()
    except requests.exceptions.RequestException as e:
        logger.debug(f"{url} is not valid")
        logger.debug(e)
        # exit()

    return CrawlResult.FAIL


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


def check():
    with open(os.path.join(ALL_OUT_PATH, "new.json"), "r") as f:
        data = json.load(f)

    for website in tqdm(data):
        domain = urlparse(website).netloc

        for sw in data[website]:
            download_file(
                sw,
                os.path.join(
                    out_path,
                    domain,
                    urlparse(sw).path.lstrip("/"),
                    "downloaded-sw.js",
                ),
            )

            for i in range(len(data[website][sw])):
                script = data[website][sw][i]
                logger.info(
                    f"Downloading {script} to {os.path.join(out_path, domain, urlparse(sw).path.lstrip('/'), f'script_{i}.js')}"
                )
                download_file(
                    script,
                    os.path.join(
                        out_path,
                        domain,
                        urlparse(sw).path.lstrip("/"),
                        f"script_{i}.js",
                    ),
                )

        # exit()


def main():
    # new()
    # check_for_static_or_cdn()
    # remove_known_providers()
    download_unknown_providers()


if __name__ == "__main__":
    with logging_redirect_tqdm():
        main()
# df.to_csv('serviceworkers_origins_urls_and_imported_scripts.csv', index=False)
