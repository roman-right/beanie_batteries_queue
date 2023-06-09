from typing import List, Optional

from beanie import Link
from pydantic import Field

from beanie_batteries_queue.queue import Task, DependencyType


class SimpleTask(Task):
    s: str


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
        dependency_type=DependencyType.DIRECT
    )


class TaskWithOptionalAllOfDependency(Task):
    s: str
    optional_all_of_dependency: Optional[List[Link[SimpleTask]]] = Field(
        dependency_type=DependencyType.ALL_OF
    )


class TaskWithOptionalAnyOfDependency(Task):
    s: str
    optional_any_of_dependency: Optional[List[Link[SimpleTask]]] = Field(
        dependency_type=DependencyType.ANY_OF
    )
