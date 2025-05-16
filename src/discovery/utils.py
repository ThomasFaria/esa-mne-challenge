import re

WORDS_TO_REMOVE = [
    "GEBR",
    "PUBLIC LIMITED COMPANY",
    "PLC",
    "AKTIEBOLAGET",
    "PARTICIPATIONS",
    "AG",
    "TOVARNA ZDRAVIL",
    "NOVO MESTO",
    "ZHEJIANG",
    "COMPAGNIE GENERALE DES ETABLISSEMENTS",
]


def clean_mne_name(name: str) -> str:
    """
    Cleans and standardizes a multinational enterprise (MNE) name for better matching
    against sources like Yahoo Finance.

    Steps performed:
    1. Removes any content within parentheses.
    2. Removes dots.
    3. Removes stopwords such as "GROUP", "HOLDING", etc. (provided by caller).
    4. Normalizes whitespace.
    5. Converts starting "L " to "L'" (e.g., French corporate names like "L Oreal" → "L'Oreal").

    Parameters:
    ----------
    name : str
        The original company name string to be cleaned.

    Returns:
    -------
    str
        A cleaned, standardized company name suitable for search queries.
    """
    # Step 1: Remove text within parentheses
    cleaned_name = re.sub(r"\([^)]*\)", "", name)

    # Step 2: Remove dots
    cleaned_name = re.sub(r"\.", "", cleaned_name)

    # Step 3: Remove specified stopwords
    pattern = r"\b(?:" + "|".join(WORDS_TO_REMOVE) + r")\b"
    cleaned_name = re.sub(pattern, "", cleaned_name, flags=re.IGNORECASE)

    # Step 4: Normalize whitespace
    cleaned_name = re.sub(r"\s+", " ", cleaned_name).strip()

    # Step 5: Replace starting "L " with "L'"
    cleaned_name = re.sub(r"^L\s+", "L'", cleaned_name)

    # Step 6: Replace "SOCIETA PER AZIONI" with "s.p.a."
    cleaned_name = re.sub(r"\bSOCIETA\s+PER\s+AZIONI\b", "s.p.a.", cleaned_name, flags=re.IGNORECASE)

    # Step 7: Replace " DD" with " D D"
    cleaned_name = re.sub(r"\s\bDD\b", " D D", cleaned_name, flags=re.IGNORECASE)

    # Step 7: Replace "MERCK GROUP" with "MERCK KGAA"
    cleaned_name = re.sub(r"\bMERCK GROUP\b", "MERCK KGAA", cleaned_name, flags=re.IGNORECASE)
    return cleaned_name
