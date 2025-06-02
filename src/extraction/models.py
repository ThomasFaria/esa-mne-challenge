from typing import Optional

from pydantic import BaseModel, Field, HttpUrl


class ExtractedInfo(BaseModel):
    mne_id: int = Field(..., description="Company identifier")
    mne_name: str = Field(..., description="Company name")
    variable: str = Field(
        ..., description="Variable name ('Country', 'Employees', 'Turnover', 'Assets', 'Website', 'Activity')"
    )
    source_url: HttpUrl = Field(..., description="Source URL")
    value: str = Field(..., description="Extracted value")
    currency: Optional[str] = Field(None, description="Currency of the value, if applicable")
    year: Optional[int] = Field(..., description="Fiscal year of the extracted information")
