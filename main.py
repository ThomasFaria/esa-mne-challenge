import logging

import pandas as pd

from src.common.data import get_file_system
from src.discovery.fetcher import AnnualReportFetcher
from src.discovery.paths import DATA_DISCOVERY_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)

fs = get_file_system()

with fs.open(DATA_DISCOVERY_PATH) as f:
    df = pd.read_csv(f, sep=";")

mnes = df["NAME"].unique().tolist()

fetcher = AnnualReportFetcher()

report = fetcher.fetch_for(mnes[43])

report
