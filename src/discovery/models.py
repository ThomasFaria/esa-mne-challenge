from pydantic import BaseModel, Field, HttpUrl


class AnnualReport(BaseModel):
    mne: str = Field(..., description="Company name")
    year: int = Field(..., description="Fiscal year of report")
    url: HttpUrl = Field(..., description="Direct link to PDF")
