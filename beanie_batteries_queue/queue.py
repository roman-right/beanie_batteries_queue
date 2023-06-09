import asyncio
from datetime import datetime
from enum import Enum
from typing import Type, Optional, Dict, ClassVar

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


class DependencyType(str, Enum):
    ALL_OF = "ALL_OF"
    ANY_OF = "ANY_OF"
    DIRECT = "DIRECT"


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
    _dependency_fields: ClassVar[Optional[Dict[str, DependencyType]]] = None

    class Settings:
        indexes = [
            [
                ("state", ASCENDING),
                ("priority", DESCENDING),
                ("created_at", ASCENDING),
            ],
            # expire after 1 day
            [("created_at", ASCENDING), ("expireAfterSeconds", 86400)],
        ]

    @classmethod
    async def custom_init(cls):
        for name, field in cls.__fields__.items():
            if field.field_info.extra.get("dependency_type"):
                if cls._dependency_fields is None:
                    cls._dependency_fields = {}
                cls._dependency_fields[name] = field.field_info.extra[
                    "dependency_type"
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
        find_query = cls.make_find_query()
        found_task = (
            await cls.find(find_query, fetch_links=True)
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
    def make_find_query(cls):
        queries = [{"state": State.CREATED}]
        if cls._dependency_fields is not None:
            for (
                dependency_field,
                dependency_type,
            ) in cls._dependency_fields.items():
                queries.append(
                    cls.make_dependency_query(
                        dependency_field, dependency_type
                    )
                )
        return {"$and": queries}

    @staticmethod
    def make_dependency_query(
        dependency_field: str, dependency_type: DependencyType
    ):
        if dependency_type == DependencyType.ALL_OF:
            # TODO this looks tricky
            return {
                dependency_field: {
                    "$not": {"$elemMatch": {"state": {"$ne": State.FINISHED}}}
                }
            }
        elif dependency_type == DependencyType.ANY_OF:
            return {
                "$or": [
                    {dependency_field: {"$size": 0}},
                    {
                        dependency_field: {
                            "$elemMatch": {"state": State.FINISHED}
                        }
                    },
                ]
            }
        elif dependency_type == DependencyType.DIRECT:
            return {
                "$or": [
                    {dependency_field: None},
                    {f"{dependency_field}.state": {"$eq": State.FINISHED}},
                ]
            }

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
