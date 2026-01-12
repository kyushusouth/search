from opensearchpy import OpenSearch


def main():
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

    query = {"query": {"match": {"title": "コケ"}}}
    response = client.search(index=index_name, body=query)
    hits = response["hits"]["hits"]
    for hit in hits:
        print(hit["_source"]["title"])


if __name__ == "__main__":
    main()
