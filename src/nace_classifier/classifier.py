import json
from pathlib import Path
from typing import List

from langchain.schema import Document
from langfuse import Langfuse
from langfuse.openai import AsyncOpenAI

from vector_db.loaders import get_vector_db

from .models import Activity


class NACEClassifier:
    def __init__(
        self,
        llm_client: AsyncOpenAI,
        model: str = "mistral-small3.2:latest",
    ):
        self.client = llm_client
        self.model = model
        self.db = get_vector_db("challenge-mne")
        self.prompt_template = Langfuse().get_prompt("nace-classifier", label="production")

        mapping_file = Path(__file__).parent / "mapping.json"
        with mapping_file.open(encoding="utf-8") as f:
            self.mapping = json.load(f)

    def _format_documents(self, docs: List[Document]) -> (str, str):
        proposed_codes = "\n\n".join(f"========\n{doc.page_content}" for doc in docs)
        list_codes = ", ".join(f"'{doc.metadata['CODE']}'" for doc in docs)
        return proposed_codes, list_codes

    async def classify(self, activity_description: str, top_k: int = 8) -> Activity:
        """
        Classify a company's main activity into a NACE code.
        """
        query = (
            "Instruct: Given a description of a company, retrieve relevant NACE codes "
            f"that describe its activities.\nQuery: {activity_description}"
        )

        docs = await self.db.asimilarity_search(query, k=top_k)
        proposed_codes, list_codes = self._format_documents(docs)

        messages = self.prompt_template.compile(
            activity=activity_description,
            proposed_codes=proposed_codes,
            list_proposed_codes=list_codes,
        )

        response = await self.client.beta.chat.completions.parse(
            name="activity_classifier",
            model=self.model,
            messages=messages,
            response_format=Activity,
            temperature=0.1,
        )

        parsed = response.choices[0].message.parsed
        parsed.code = f"{self.mapping[parsed.code]}{parsed.code}"
        return parsed
