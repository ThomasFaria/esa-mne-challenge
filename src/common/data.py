import logging
import os
from typing import List

import pandas as pd
import s3fs

logger = logging.getLogger(__name__)


def get_file_system(token=None) -> s3fs.S3FileSystem:
    """
    Creates and returns an S3 file system instance using the s3fs library.

    Parameters:
    -----------
    token : str, optional
        A temporary security token for session-based authentication. This is optional and
        should be provided when using session-based credentials.

    Returns:
    --------
    s3fs.S3FileSystem
        An instance of the S3 file system configured with the specified endpoint and
        credentials, ready to interact with S3-compatible storage.

    """

    options = {
        "client_kwargs": {"endpoint_url": f"https://{os.environ['AWS_S3_ENDPOINT']}"},
        "key": os.environ["AWS_ACCESS_KEY_ID"],
        "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
    }

    if token is not None:
        options["token"] = token

    return s3fs.S3FileSystem(**options)


def load_mnes(path: str, sep: str = ";") -> List[str]:
    fs = get_file_system()
    try:
        with fs.open(path) as f:
            df = pd.read_csv(f, sep=sep)
        mnes = df.loc[:, ["ID", "NAME"]].drop_duplicates().to_dict(orient="records")
        logger.info(f"Loaded {len(mnes)} MNEs from {path}")
        return mnes
    except Exception:
        logger.exception(f"Failed to load MNEs from {path}")
        raise


def generate_discovery_submission(reports):
    """
    Generate a submission DataFrame combining FIN_REP and duplicated OTHER types.

    Parameters:
    - reports: list of pydantic or dict-like objects with attributes: mne_id, mne_name, pdf_url, year.
    """
    # Create initial DataFrame from reports
    fin_rep = pd.DataFrame([r.dict() for r in reports]).rename(
        columns={
            "mne_id": "ID",
            "mne_name": "NAME",
            "pdf_url": "SRC",
            "year": "REFYEAR",
        }
    )
    fin_rep["TYPE"] = "FIN_REP"
    fin_rep = fin_rep[["ID", "NAME", "TYPE", "SRC", "REFYEAR"]]

    # Create OTHER rows by duplicating ID, NAME, TYPE
    other = pd.concat([fin_rep[["ID", "NAME", "TYPE"]]] * 5, ignore_index=True)
    other["TYPE"] = "OTHER"

    # Combine and sort final submission
    submission = pd.concat([fin_rep, other], ignore_index=True).sort_values(by=["ID", "TYPE"]).reset_index(drop=True)

    # Format REFYEAR column
    submission["REFYEAR"] = submission["REFYEAR"].astype("Int64")

    # Export to CSV
    submission.to_csv("data/discovery/discovery.csv", sep=";", index=False)

    return submission
