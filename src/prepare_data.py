"""
Prepare japanese text data for information retrieval.

references
- https://zenn.dev/nak6/scraps/48acc7faf20056
- https://github.com/amazon-science/esci-data
"""

import json
import pickle
import random
from pathlib import Path

import pandas as pd
from google import genai
from google.genai import types


def preprocess_text(x: str | None) -> str:
    if x is not None:
        return x
    return ""


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
                "rating": random.randint(1, 10),
            },
        )

    with open(root_dir.joinpath("data", "products.json"), "w") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    client = genai.Client()

    contents = [
        f"{preprocess_text(result['title']) + ' ' + preprocess_text(result['description']) + ' ' + preprocess_text(result['bullet_point'])}"
        for result in results
    ]
    response = client.models.embed_content(
        model="gemini-embedding-001",
        contents=contents,
        config=types.EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT"),
    )
    embs = [emb.values for emb in response.embeddings]

    with open(root_dir.joinpath("data", "product_embs.pickle"), "wb") as f:
        pickle.dump(embs, f)


if __name__ == "__main__":
    main()
