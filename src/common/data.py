import logging
import os
from typing import List, Union

import pandas as pd
import s3fs

from extraction.models import ExtractedInfo
from fetchers.models import AnnualReport, OtherSources

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


def pad_to_five(group):
    missing = 5 - len(group)
    if missing > 0:
        filler = pd.DataFrame([{}] * missing)
        return pd.concat([group, filler], ignore_index=True)
    return group


def generate_discovery_submission(mne_infos: List[List[Union[AnnualReport, OtherSources]]]) -> pd.DataFrame:
    """
    Generate a submission DataFrame combining FIN_REP and OTHER types.

    Parameters:
    - reports: list of pydantic or dict-like objects with attributes: mne_id, mne_name, pdf_url, year.
    """
    # Create initial DataFrame from reports
    fin_rep = pd.DataFrame(
        [src.model_dump() for sources in mne_infos for src in sources if isinstance(src, AnnualReport)]
    ).rename(
        columns={
            "mne_id": "ID",
            "mne_name": "NAME",
            "pdf_url": "SRC",
            "year": "REFYEAR",
        }
    )

    fin_rep["TYPE"] = "FIN_REP"
    fin_rep = fin_rep[["ID", "NAME", "TYPE", "SRC", "REFYEAR"]]

    other_src = pd.DataFrame(
        [src.model_dump() for sources in mne_infos for src in sources if isinstance(src, OtherSources)]
    ).rename(
        columns={
            "mne_id": "ID",
            "mne_name": "NAME",
            "url": "SRC",
        }
    )

    other_src["TYPE"] = "OTHER"
    other_src = other_src[
        [
            "ID",
            "NAME",
            "TYPE",
            "SRC",
        ]
    ]

    # Pad other sources to 5 entries per group
    other_src = (
        other_src.groupby(["ID", "NAME", "TYPE"], group_keys=False)
        .apply(
            lambda g: pad_to_five(g.drop(columns=["ID", "NAME", "TYPE"])).assign(
                ID=g.iloc[0]["ID"], NAME=g.iloc[0]["NAME"], TYPE=g.iloc[0]["TYPE"]
            )
        )
        .reset_index(drop=True)
        .merge(fin_rep.loc[:, ["ID", "REFYEAR"]], on="ID", how="right")
        .loc[:, ["ID", "NAME", "TYPE", "SRC", "REFYEAR"]]
    )
    other_src.loc[other_src["SRC"].isna(), "REFYEAR"] = pd.NA

    # Combine and sort final submission
    submission = (
        pd.concat([fin_rep, other_src], ignore_index=True).sort_values(by=["ID", "TYPE"]).reset_index(drop=True)
    )

    # Format REFYEAR column
    submission["REFYEAR"] = submission["REFYEAR"].astype("Int64")

    # Export to CSV
    submission.to_csv("data/discovery/discovery.csv", sep=";", index=False)

    return submission


def generate_extraction_submission(mne_infos: List[ExtractedInfo]) -> pd.DataFrame:
    extraction = pd.DataFrame(
        [info.model_dump() for infos in mne_infos for info in infos if isinstance(info, ExtractedInfo)]
    ).rename(
        columns={
            "mne_id": "ID",
            "mne_name": "NAME",
            "variable": "VARIABLE",
            "source_url": "SRC",
            "value": "VALUE",
            "currency": "CURRENCY",
            "year": "REFYEAR",
        }
    )

    complete_rows = []
    for (mne_id, mne_name), group in extraction.groupby(["ID", "NAME"]):
        existing_variables = set(group["VARIABLE"])

        for variable in ["COUNTRY", "EMPLOYEES", "TURNOVER", "ASSETS", "WEBSITE", "ACTIVITY"]:
            if variable in existing_variables:
                # Keep the first matching row for the variable
                row = group[group["VARIABLE"] == variable].iloc[0].to_dict()
                complete_rows.append(row)
            else:
                # Create a row with NaNs except for ID, NAME, VARIABLE
                empty_row = {
                    "ID": mne_id,
                    "NAME": mne_name,
                    "VARIABLE": variable,
                    "SRC": pd.NA,
                    "VALUE": pd.NA,
                    "CURRENCY": pd.NA,
                    "REFYEAR": pd.NA,
                }
                complete_rows.append(empty_row)

    # Build the completed DataFrame
    submission = pd.DataFrame(complete_rows)

    submission.loc[submission["SRC"].isna(), "REFYEAR"] = pd.NA

    # Format REFYEAR column
    submission["REFYEAR"] = submission["REFYEAR"].astype("Int64")

    # Export to CSV
    submission.to_csv("data/extraction/extraction.csv", sep=";", index=False)
    return submission
