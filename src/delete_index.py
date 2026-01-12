"""
Setup OpenSearch.

references
- https://subro.mokuren.ne.jp/0930.html
"""

import json
from pathlib import Path

import numpy as np
from opensearchpy import OpenSearch, helpers


def main():
    # create clients
    host = "localhost"
    port = 9200
    client = OpenSearch(
        hosts=[{"host": host, "port": port}],
        http_compress=True,
        use_ssl=False,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
    )

    index_name = "products"
    response = client.indices.delete(index=index_name)
    print(response)


if __name__ == "__main__":
    main()
