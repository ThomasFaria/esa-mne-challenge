from typing import Optional

from pydantic import BaseModel, Field, HttpUrl


class AnnualReport(BaseModel):
    mne_id: int = Field(..., description="Company identifier")
    mne_name: str = Field(..., description="Company name")
    pdf_url: Optional[HttpUrl] = Field(..., description="Direct link to PDF")
    year: Optional[int] = Field(..., description="Fiscal year of annual financial report")


class OtherSources(BaseModel):
    mne_id: int = Field(..., description="Company identifier")
    mne_name: str = Field(..., description="Company name")
    source_name: str = Field(..., description="Name of the source (e.g., Google, Wikipedia, Yahoo)")
    url: HttpUrl = Field(..., description="Website URL")
    year: int = Field(..., description="Reference year of the source")
    mne_website: Optional[HttpUrl] = Field(None, description="Company website URL")
    mne_national_id: Optional[str] = Field(None, description="Company National ID")
    mne_activity: Optional[str] = Field(None, description="Company NACE code")


class SearchResult(BaseModel):
    url: HttpUrl = Field(..., description="Website URL")
    title: str = Field(..., description="Website title")
    description: str = Field(..., description="Website description")
