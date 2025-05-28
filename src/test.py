import asyncio
import logging
import os

import logfire
from agents import Agent, Runner, function_tool, set_trace_processors
from agents.extensions.models.litellm_model import LitellmModel
from agents.model_settings import ModelSettings
from googlesearch import search as google_search
from langchain_core.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field, HttpUrl, ValidationError

import config

logger = logging.getLogger(__name__)


config.setup()
set_trace_processors([])

# Configure logfire instrumentation.
logfire.configure(
    service_name="my_agent_service",
    send_to_logfire=False,
)

# This method automatically patches the OpenAI Agents SDK to send logs via OTLP to Langfuse.
logfire.instrument_openai_agents()

MODEL = "devstral:latest"

model = LitellmModel(
    model=f"openai/{MODEL}", base_url=os.environ["OPENAI_API_BASE_URL"], api_key=os.environ["OPENAI_API_KEY"]
)


class SearchResult(BaseModel):
    url: HttpUrl = Field(..., description="Website URL")
    title: str = Field(..., description="Website title")
    description: str = Field(..., description="Website description")


parser = PydanticOutputParser(pydantic_object=SearchResult)


@function_tool
def web_search(query: str) -> SearchResult:
    logger.info(f"Searching Google for '{query}'")
    raw_results = list(
        google_search(
            query,
            num_results=10,
            proxy=None,
            advanced=True,
            sleep_interval=0,
            region=None,
        )
    )

    if not raw_results:
        logger.warning(f"No Google results for '{query}'")
        return []

    results = []
    for r in raw_results:
        try:
            result = SearchResult(url=r.url, title=r.title, description=r.description)
            results.append(result)
        except ValidationError as e:
            logger.warning(f"Skipping invalid Google result URL: {r.url} ({e.errors()[0]['msg']})")

    return results


PROMPT = (
    "You are a research agent. You will be given a Multinational Enterprise as input. You have to make"
    " a pertinent web search in order to find the annual report for this company for 2024 in the PDF format. "
    "You have access to the tool `web_search` in order to make web research. From the web search results, return"
    "the more likely to be the annual report 2024 of the Multinational Enterprise."
    "Output **only** a single JSON object conforming to:\n"
    """```json\n{\n"url": "<Website URL>",\n"title": "<Website title>",\n"description": "<Website description>"\n}\n```"""
)

agent = Agent(
    model=model,
    name="Search agent",
    instructions=PROMPT,
    tools=[web_search],
    model_settings=ModelSettings(tool_choice="required"),
)


async def main():
    result = await Runner.run(agent, input="Multinational Enterprise : HUSQVARNA AB")
    print(result.final_output)
    return parser.parse(result.final_output)


if __name__ == "__main__":
    x = asyncio.run(main())
