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
                # Compare years
                if item.year and (merged[key].year is None or item.year > merged[key].year):
                    merged[key] = item
                elif item.year == merged[key].year:
                    # Keep the first occurrence (keep existing)
                    continue

    return list(merged.values())


def deduplicate_by_latest_year(infos: list[ExtractedInfo]) -> list[ExtractedInfo]:
    latest = {}
    for item in infos:
        var = item.variable
        if var not in latest or item.year > latest[var].year:
            latest[var] = item
    return list(latest.values())
