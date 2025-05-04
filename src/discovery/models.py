from typing import Optional

from pydantic import BaseModel, Field, HttpUrl


class AnnualReport(BaseModel):
    mne_name: str = Field(..., description="Company name")
    mne_id: str = Field(..., description="Company identifier")
    year: Optional[int] = Field(..., description="Fiscal year of annual financial report")
    pdf_url: Optional[HttpUrl] = Field(..., description="Direct link to PDF")
