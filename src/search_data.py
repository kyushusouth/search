import pickle
from pathlib import Path

from opensearchpy import OpenSearch


def main():
    root_dir = Path("~/dev/search").expanduser()

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

    print("全文検索")
    query = {"query": {"match": {"title": "Women"}}}
    response = client.search(index=index_name, body=query)
    hits = response["hits"]["hits"]
    for hit in hits:
        print(hit["_source"]["title"])

    with open(root_dir.joinpath("data", "product_embs.pickle"), "rb") as f:
        embs = pickle.load(f)

    print("\n")
    print("ベクトル検索")
    query = {"query": {"knn": {"embedding": {"vector": embs[0], "k": 3}}}}
    response = client.search(index=index_name, body=query)
    hits = response["hits"]["hits"]
    for hit in hits:
        print(hit["_source"]["title"])

    print("\n")
    print("ハイブリッド検索")
    query = {
        "query": {
            "hybrid": {
                "queries": [
                    {"match": {"title": "Women"}},
                    {"knn": {"embedding": {"vector": embs[0], "k": 3}}},
                ]
            }
        }
    }
    response = client.search(
        index=index_name, body=query, params={"search_pipeline": "nlp-search-pipeline"}
    )
    hits = response["hits"]["hits"]
    for hit in hits:
        print(hit["_source"]["title"])

    print("\n")
    print("フィルタリング + ベクトル検索")
    query = {
        "query": {
            "knn": {
                "embedding": {
                    "vector": embs[0],
                    "k": 3,
                    "filter": {
                        "bool": {
                            "should": [
                                {"range": {"rating": {"gte": 9}}},
                            ]
                        }
                    },
                }
            }
        }
    }
    response = client.search(index=index_name, body=query)
    hits = response["hits"]["hits"]
    for hit in hits:
        print(hit["_source"]["title"])


if __name__ == "__main__":
    main()
