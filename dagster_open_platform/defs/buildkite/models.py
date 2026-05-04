from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class Job:
    id: str
    build_id: str
    extracted_at: datetime

    type: str
    name: str | None
    step_key: str | None
    group_key: str | None
    state: str | None
    command: str | None
    soft_failed: bool | None
    exit_status: int | None
    retried: bool | None
    retries_count: int | None
    created_at: datetime | None
    scheduled_at: datetime | None
    runnable_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    expired_at: datetime | None


@dataclass(frozen=True)
class Build:
    id: str
    extracted_at: datetime

    pipeline__id: str
    pipeline__slug: str
    pipeline__name: str

    url: str
    web_url: str
    number: int
    state: str
    blocked: bool
    cancel_reason: str | None
    message: str
    commit: str
    branch: str
    source: str
    created_at: datetime | None
    scheduled_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    ai_assessment: dict[str, Any] = field(default_factory=dict)
    jobs: list[Job] = field(default_factory=list)
