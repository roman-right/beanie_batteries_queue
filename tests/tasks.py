from time import sleep
from typing import List, Optional

from beanie import Link
from beanie.odm.registry import DocsRegistry
from pydantic import Field

from beanie_batteries_queue import Task, DependencyType
from beanie_batteries_queue.scheduled_task import ScheduledTask


class SimpleTask(Task):
    s: str

    async def run(self):
        print("Run simple task")
        self.s = self.s.upper()
        await self.save()
        print(DocsRegistry._registry)


class FailingTask(Task):
    s: str

    async def run(self):
        raise Exception("Failing task")


class AnotherSimpleTask(Task):
    s: str

    async def run(self):
        self.s = self.s.upper()
        await self.save()


class SimpleTaskWithLongProcessingTime(Task):
    s: str

    async def run(self):
        sleep(5)  # blocking operation
        self.s = self.s.upper()
        await self.save()


class TaskWithDirectDependency(Task):
    s: str
    direct_dependency: Link[SimpleTask] = Field(
        dependency_type=DependencyType.DIRECT
    )


class TaskWithAllOfDependency(Task):
    s: str
    all_of_dependency: List[Link[SimpleTask]] = Field(
        dependency_type=DependencyType.ALL_OF
    )


class TaskWithAnyOfDependency(Task):
    s: str
    any_of_dependency: List[Link[SimpleTask]] = Field(
        dependency_type=DependencyType.ANY_OF
    )


class TaskWithOptionalDependency(Task):
    s: str
    optional_dependency: Optional[Link[SimpleTask]] = Field(
        default=None, dependency_type=DependencyType.DIRECT
    )


class TaskWithOptionalAllOfDependency(Task):
    s: str
    optional_all_of_dependency: Optional[List[Link[SimpleTask]]] = Field(
        default=None, dependency_type=DependencyType.ALL_OF
    )


class TaskWithOptionalAnyOfDependency(Task):
    s: str
    optional_any_of_dependency: Optional[List[Link[SimpleTask]]] = Field(
        default=None, dependency_type=DependencyType.ANY_OF
    )


class SimpleScheduledTask(ScheduledTask):
    s: str


class ScheduledTaskWithInterval(ScheduledTask):
    s: str
    interval: int = 5
