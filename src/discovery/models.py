from typing import Optional

from pydantic import BaseModel, Field, HttpUrl


class AnnualReport(BaseModel):
    mne_id: int = Field(..., description="Company identifier")
    mne_name: str = Field(..., description="Company name")
    pdf_url: Optional[HttpUrl] = Field(..., description="Direct link to PDF")
    year: Optional[int] = Field(..., description="Fiscal year of annual financial report")


class SearchResult(BaseModel):
    url: HttpUrl = Field(..., description="Website URL")
    title: str = Field(..., description="Website title")
    description: str = Field(..., description="Website description")
