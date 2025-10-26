"""
Prepare japanese text data for information retrieval.

references
- https://zenn.dev/nak6/scraps/48acc7faf20056
- https://github.com/amazon-science/esci-data
"""

import json
from pathlib import Path

import pandas as pd


def main():
    root_dir = Path("~/dev/search").expanduser()

    df_products = pd.read_parquet(
        root_dir.joinpath("data", "shopping_queries_dataset_products.parquet")
    )
    df_products = df_products.sample(10)

    index_name = "products"
    results = []
    for row in df_products.itertuples():
        results.append(
            {
                "_index": index_name,
                "_id": row.product_id,
                "title": row.product_title,
                "description": row.product_description,
                "bullet_point": row.product_bullet_point,
                "brand": row.product_brand,
                "color": row.product_color,
                "locale": row.product_locale,
            },
        )

    with open(root_dir.joinpath("data", "products.json"), "w") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    main()
