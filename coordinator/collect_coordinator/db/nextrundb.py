from datetime import datetime, timedelta
from typing import Tuple

from sqlalchemy import String, JSON, DATETIME, Index, BOOLEAN
from sqlalchemy import select, Select
from sqlalchemy.orm import Mapped, mapped_column

from collect_coordinator.db.database import DbEntity
from collect_coordinator.job_coordinator import Json
from collect_coordinator.util import utc


class NextRun(DbEntity):
    __tablename__ = "next_runs"

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    next_run: Mapped[datetime] = mapped_column(DATETIME, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DATETIME, nullable=True)
    in_progress: Mapped[bool] = mapped_column(BOOLEAN, nullable=False, default=False)
    some_data: Mapped[Json] = mapped_column(JSON, nullable=True)

    # all indices of this table
    idx_next_run = Index("idx_next_run", next_run)
    idx_too_long_in_progress = Index("idx_too_long_in_progress", started_at, in_progress)

    def __repr__(self) -> str:
        return (
            f"NextRun({self.tenant_id!r}, {self.name!r}, {self.next_run!r}, {self.started_at!r}, {self.in_progress!r})"
        )


def runs_to_start() -> Select[Tuple[NextRun]]:
    return select(NextRun).where((NextRun.next_run <= utc()) & (NextRun.in_progress == False))


def running_for_too_long(too_long: timedelta) -> Select[Tuple[NextRun]]:
    return select(NextRun).where((NextRun.started_at <= (utc() - too_long)) & (NextRun.in_progress == True))
