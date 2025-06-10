import logging

from langchain_community.document_loaders import DataFrameLoader

import config
from vector_db.loaders import create_vector_db, get_embedding_model
from vector_db.notices_nace import fetch_nace_labels

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)
config.setup()


def build_vector_db():
    labels = fetch_nace_labels()

    docs = DataFrameLoader(labels, page_content_column="LABEL").load()

    emb_model = get_embedding_model("Qwen/Qwen3-Embedding-8B")

    _ = create_vector_db(docs, emb_model)


# === Entry Point === #
if __name__ == "__main__":
    build_vector_db()
