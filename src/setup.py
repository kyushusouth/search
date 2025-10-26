"""
Setup OpenSearch.

references
- https://subro.mokuren.ne.jp/0930.html
"""

import json
from pathlib import Path

from opensearchpy import OpenSearch, helpers


def main():
    root_dir = Path("~/dev/search").expanduser()

    # load_data
    with open(root_dir.joinpath("data", "products.json"), "r") as f:
        products_data = json.load(f)

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

    # create index
    index_name = "products"
    if not client.indices.exists(index=index_name):
        client.indices.create(
            index=index_name,
            body={
                "settings": {
                    "index": {
                        "number_of_shards": 1,
                        "number_of_replicas": 1,
                        "analysis": {
                            "analyzer": {
                                "kuromoji_normalize": {
                                    "char_filter": ["icu_normalizer"],
                                    "tokenizer": "kuromoji_tokenizer",
                                    "filter": [
                                        "kuromoji_baseform",
                                        "kuromoji_part_of_speech",
                                        "cjk_width",
                                        "ja_stop",
                                        "kuromoji_stemmer",
                                        "lowercase",
                                    ],
                                }
                            }
                        },
                    }
                },
                "mappings": {
                    "properties": {
                        "title": {"type": "text"},
                        "description": {
                            "type": "text",
                            "term_vector": "with_positions_offsets",
                        },
                        "bullet_point": {"type": "text"},
                        "brand": {"type": "keyword"},
                        "color": {"type": "text"},
                        "locale": {"type": "keyword"},
                    }
                },
            },
        )

    # add document
    succeeded = []
    failed = []
    for success, item in helpers.parallel_bulk(
        client,
        actions=products_data,
        chunk_size=10,
        raise_on_error=False,
        raise_on_exception=False,
        max_chunk_bytes=20 * 1024 * 1024,
        request_timeout=60,
    ):
        if success:
            succeeded.append(item)
        else:
            failed.append(item)

    if len(failed) > 0:
        print(f"There were {len(failed)} errors:")
        for item in failed:
            print(f"{item['index']['error']}: {item['index']['exception']}")

    if len(succeeded) > 0:
        print(f"Bulk-inserted {len(succeeded)} items.")


if __name__ == "__main__":
    main()
