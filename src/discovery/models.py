from typing import Optional

from pydantic import BaseModel, Field


class AnnualReport(BaseModel):
    mne_id: int = Field(..., description="Company identifier")
    mne_name: str = Field(..., description="Company name")
    pdf_url: Optional[str] = Field(..., description="Direct link to PDF")
    year: Optional[int] = Field(..., description="Fiscal year of annual financial report")
