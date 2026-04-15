from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class HealthResponse(BaseModel):
    status: str = Field(examples=["ok"])
    database: str = Field(examples=["reachable"])


class StagingFlexiOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    org_call_id: str | None = None
    charging_id: str | None = None
    record_sequence_number: str | None = None
    record_opening_time: str | None = None
    served_msisdn: str | None = None
    ftp_filename: str | None = None
    source_file: str | None = Field(default=None, alias="_source_file")
    ingested_at: datetime | None = Field(default=None, alias="_ingested_at")


class StagingIccOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    org_call_id: str | None = None
    call_reference: str | None = None
    call_sta_time: str | None = None
    call_type: str | None = None
    used_duration: float | None = None
    source_file: str | None = Field(default=None, alias="_source_file")
    ingested_at: datetime | None = Field(default=None, alias="_ingested_at")


class UsageDailyOut(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    date_key: int | None = None
    usage_date: date | None = None
    call_type_code: str | None = None
    event_count: int
    total_used_duration: Decimal | None = None
