import logging

from tqdm.contrib.logging import logging_redirect_tqdm
from urllib.parse import urlparse

from provider_discovery import (
    check_domain_push_content,
    compare_ssdeep,
    crawl_service_workers,
    detect_push_providers,
    discover_unknown_push_providers,
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    crawl_service_workers.main()
    compare_ssdeep.main()
    detect_push_providers.main()
    discover_unknown_push_providers.main()
    check_domain_push_content.main()


if __name__ == "__main__":
    with logging_redirect_tqdm():
        main()
