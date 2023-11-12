from datetime import datetime, timedelta
from typing import Optional

from beanie import UpdateResponse
from beanie.odm.enums import SortDirection
from pydantic import Field

from beanie_batteries_queue import Task, State


class ScheduledTask(Task):
    run_at: datetime = Field(default_factory=datetime.utcnow)
    interval: Optional[int] = None

    @classmethod
    async def pop(cls) -> Optional["ScheduledTask"]:
        """
        Get the first scheduled task from the queue that is due to run and reschedule it if needed
        :return:
        """
        task = None
        now = datetime.utcnow()
        find_query = cls.make_find_query()
        find_query["$and"].append(
            {"run_at": {"$lte": now}}
        )  # Only select tasks that are due
        found_task = (
            await cls.find(find_query, fetch_links=True)
            .sort(
                [
                    ("run_at", SortDirection.ASCENDING),
                    ("priority", SortDirection.DESCENDING),
                    ("created_at", SortDirection.ASCENDING),
                ]
            )
            .first_or_none()
        )

        if found_task is not None:
            task = await cls.find_one(
                {"_id": found_task.id, "state": State.CREATED}
            ).update(
                {"$set": {"state": State.RUNNING}},
                response_type=UpdateResponse.NEW_DOCUMENT,
            )

            # Reschedule task if it has an interval
            if task and task.interval is not None:
                new_time = now + timedelta(seconds=task.interval)
                await cls(
                    **task.model_dump(exclude={"_id", "run_at", "state"}),
                    run_at=new_time
                ).save()

            if task is None:
                task = await cls.pop()

        return task
