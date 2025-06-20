from extractors.models import ExtractedInfo


def merge_extracted_infos(*sources: list[ExtractedInfo]) -> list[ExtractedInfo]:
    merged = {}

    for source in sources:
        if not source:
            continue
        for item in source:
            key = item.variable

            # If we haven't seen this variable, store it
            if key not in merged:
                merged[key] = item
            else:
                current = merged[key]

                # Special rule for EMPLOYEES from WIKIPEDIA. (For employees, wikipedia is often a more reliable ad up to date source)
                if key == "EMPLOYEES":
                    if "wikipedia" in item.source_url and item.year and item.year >= 2024:
                        merged[key] = item
                        continue
                    elif "wikipedia" in current.source_url and current.year and current.year >= 2024:
                        # Keep existing if it already satisfies the rule
                        continue

                # Default merging logic by year
                if item.year and (current.year is None or item.year > current.year):
                    merged[key] = item
                elif item.year == current.year:
                    continue  # Keep the existing item

    return list(merged.values())


def deduplicate_by_latest_year(infos: list[ExtractedInfo]) -> list[ExtractedInfo]:
    latest = {}
    for item in infos:
        var = item.variable
        if var not in latest or item.year > latest[var].year:
            latest[var] = item
    return list(latest.values())
