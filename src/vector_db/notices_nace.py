import logging
import string
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import SKOS

logger = logging.getLogger(__name__)

BASE_URL = "http://data.europa.eu/ux2/nace2"
XKOS = Namespace("http://rdf-vocabulary.ddialliance.org/xkos#")
EN = "en"


def get_rdf_graph(url: str) -> Optional[Graph]:
    """Fetch RDF/XML content from a URL and return a parsed rdflib.Graph."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        graph = Graph()
        graph.parse(data=response.text, format="xml")
        logger.debug(f"Parsed RDF graph for {url}")
        return graph
    except requests.RequestException as e:
        logger.error(f"Request failed for {url}: {e}")
    except Exception as e:
        logger.error(f"Failed to parse RDF from {url}: {e}")
    return None


def get_first_lang_literal(graph: Graph, subject: URIRef, predicate: URIRef, lang: str = EN) -> Optional[str]:
    """Return the first matching literal in the desired language."""
    return next(
        (str(o) for o in graph.objects(subject, predicate) if isinstance(o, Literal) and o.language == lang), None
    )


def extract_notes(graph: Graph, subject: URIRef) -> Dict[str, Optional[str]]:
    """Extract all expected English-language notes from the RDF subject."""
    return {
        "preferred_label": get_first_lang_literal(graph, subject, SKOS.prefLabel)[2:].strip(),
        "core_note": get_first_lang_literal(graph, subject, XKOS.coreContentNote),
        "alt_note": get_first_lang_literal(graph, subject, XKOS.additionalContentNote),
        "exclusion_note": get_first_lang_literal(graph, subject, XKOS.exclusionNote),
    }


def fetch_nace_metadata() -> Dict[str, Dict[str, Any]]:
    results = {"Section": {}, "Division": {}}

    # --- Sections A-Z --- #
    for section_code in string.ascii_uppercase:
        url = f"{BASE_URL}/{section_code}"
        logger.info(f"Fetching Section {section_code}")
        graph = get_rdf_graph(url)
        if not graph:
            continue

        for subj in graph.subjects(SKOS.notation, Literal(section_code)):
            notes = extract_notes(graph, subj)
            results["Section"][section_code] = {"identifier": str(subj), **notes}
            break

    # --- Divisions 01-99 --- #
    for i in range(1, 100):
        division_code = f"{i:02d}"
        url = f"{BASE_URL}/{division_code}"
        logger.info(f"Fetching Division {division_code}")
        graph = get_rdf_graph(url)
        if not graph:
            continue

        for subj in graph.subjects(SKOS.notation, Literal(division_code)):
            notes = extract_notes(graph, subj)
            broader_uri = next(graph.objects(subj, SKOS.broader), None)
            broader_code = broader_uri.split("/")[-1] if broader_uri else None

            results["Division"][division_code] = {"identifier": str(subj), "broader": broader_code, **notes}
            break

    return results


def format_nace_labels(nace_data: Dict[str, Dict[str, Dict[str, str]]]) -> pd.DataFrame:
    """
    Build a DataFrame containing formatted labels for each NACE division,
    including notes from both the division and its parent section.
    """

    def format_notes(source: Dict[str, str]) -> str:
        return "\n".join(note for key in ["core_note", "alt_note"] if (note := source.get(key)))

    labels: List[Dict[str, str]] = []

    for div_code, div in nace_data.get("Division", {}).items():
        sec = nace_data.get("Section", {}).get(div.get("broader", ""), {})

        division_notes = format_notes(div)
        section_notes = format_notes(sec)

        label = "\n".join(
            [
                f"## Division - **{div_code}** - {div.get('preferred_label', '')}",
                "",
                division_notes,
                "",
                f"### Parent Section - **{div.get('broader', '')}** - {sec.get('preferred_label', '')}",
                "",
                section_notes[:773],  # Limit to 773 characters in order not to produce too large prompts
            ]
        ).strip()

        labels.append({"LABEL": label, "CODE": div_code})

    return pd.DataFrame(labels)


def fetch_nace_labels() -> pd.DataFrame:
    """
    Fetch NACE metadata and return a DataFrame with formatted labels.
    """
    # Fetching NACE data from https://showvoc.op.europa.eu/#/datasets/ESTAT_Statistical_Classification_of_Economic_Activities_in_the_European_Community_Rev._2/data
    nace_data = fetch_nace_metadata()

    # Formatting NACE labels
    labels = format_nace_labels(nace_data)
    return labels
