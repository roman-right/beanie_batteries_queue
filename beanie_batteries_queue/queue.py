import asyncio
from datetime import datetime
from enum import Enum
from typing import Type

from beanie import Document
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

    async def push(self):
        await self.save()

    @classmethod
    async def pop(cls):
        task = await cls.find({"state": State.CREATED}).sort(
            [("priority", DESCENDING),
             ("created_at", ASCENDING)]).first_or_none()
        if task is not None:
            task = await cls.find_one(
                {"_id": task.id, "state": State.CREATED}).update(
                {"$set": {"state": State.RUNNING}},
                response_type=UpdateResponse.NEW_DOCUMENT)
        return task

    @classmethod
    async def is_empty(cls) -> bool:
        return await cls.find_one({"state": State.CREATED}) is None

    @classmethod
    def queue(cls, sleep_time: int = 1):
        return Queue(cls, sleep_time=sleep_time)

    async def finish(self):
        self.state = State.FINISHED
        await self.save()

    async def fail(self):
        self.state = State.FAILED
        await self.save()
