import asyncio
from datetime import datetime
from enum import Enum
from typing import Type, Optional

from beanie import Document
from beanie.odm.enums import SortDirection
from beanie.odm.queries.update import UpdateResponse
from pydantic import Field
from pymongo import DESCENDING, ASCENDING


class State(str, Enum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class Priority(Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


class Queue:
    def __init__(self, task_model: Type["Task"], sleep_time: int = 1):
        self.task_model = task_model
        self.sleep_time = sleep_time

    def __aiter__(self):
        return self

    async def __anext__(self):
        task = await self.task_model.pop()
        while task is None:
            await asyncio.sleep(self.sleep_time)
            task = await self.task_model.pop()
        return task


class Task(Document):
    state: State = State.CREATED
    priority: Priority = Priority.MEDIUM
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        indexes = [
            [
                ("state", ASCENDING),
                ("priority", DESCENDING),
                ("created_at", ASCENDING),
            ]
        ]

    async def push(self):
        await self.save()

    @classmethod
    async def pop(cls) -> Optional["Task"]:
        """
        Get the first task from the queue
        :return:
        """
        task = None
        found_task = (
            await cls.find({"state": State.CREATED})
            .sort(
                [
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
            # check if this task was not taken by another worker
            if task is None:
                task = await cls.pop()
        return task

    @classmethod
    async def is_empty(cls) -> bool:
        """
        Check if there are no tasks in the queue
        :return:
        """
        return await cls.find_one({"state": State.CREATED}) is None

    @classmethod
    def queue(cls, sleep_time: int = 1):
        """
        Get queue iterator
        :param sleep_time:
        :return:
        """
        return Queue(cls, sleep_time=sleep_time)

    async def finish(self):
        """
        Mark task as finished
        :return:
        """
        self.state = State.FINISHED
        await self.save()

    async def fail(self):
        """
        Mark task as failed
        :return:
        """
        self.state = State.FAILED
        await self.save()
