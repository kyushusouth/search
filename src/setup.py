"""
Setup OpenSearch.

references
- https://subro.mokuren.ne.jp/0930.html
"""

import json
import pickle
from pathlib import Path

from opensearchpy import OpenSearch, helpers


def main():
    root_dir = Path("~/dev/search").expanduser()

    # load_data
    with open(root_dir.joinpath("data", "products.json"), "r") as f:
        products_data = json.load(f)

    with open(root_dir.joinpath("data", "product_embs.pickle"), "rb") as f:
        embs = pickle.load(f)

    for i, product in enumerate(products_data):
        product["embedding"] = embs[i]

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
                        "knn": True,
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
                        "embedding": {
                            "type": "knn_vector",
                            "dimension": 3072,
                            "method": {
                                "name": "hnsw",
                                "space_type": "l2",
                                "engine": "faiss",
                                "parameters": {
                                    "ef_search": 100,
                                    "ef_construction": 100,
                                    "m": 16,
                                },
                            },
                            "mode": "on_disk",
                        },
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

    # create search-pipeline for hybrid search
    pipeline_id = "nlp-search-pipeline"

    pipeline_doby = {
        "description": "Post processor for hybrid search",
        "phase_results_processors": [
            {
                "normalization-processor": {
                    "normalization": {"technique": "min_max"},
                    "combination": {
                        "technique": "arithmetic_mean",
                        "parameters": {"weights": [0.3, 0.7]},
                    },
                }
            }
        ],
    }

    response = client.search_pipeline.put(id=pipeline_id, body=pipeline_doby)
    print(response)


if __name__ == "__main__":
    main()
