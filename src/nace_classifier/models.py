from pydantic import BaseModel, Field


class Activity(BaseModel):
    code: str = Field(..., description="The selected NACE code")
