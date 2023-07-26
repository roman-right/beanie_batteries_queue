import pytest

import asyncio

from beanie_batteries_queue import State, Priority
from tests.tasks import (
    SimpleTask,
    TaskWithDirectDependency,
    TaskWithAllOfDependency,
    TaskWithAnyOfDependency,
    TaskWithOptionalDependency,
    TaskWithOptionalAllOfDependency,
    TaskWithOptionalAnyOfDependency,
)


class TestGeneralCases:
    async def test_simple_pipeline(self):
        task = SimpleTask(s="test")
        await task.push()

        async for task in SimpleTask.queue():
            assert task.s == "test"
            await task.finish()
            break

        task = await SimpleTask.find_one({"s": "test"})
        assert task.state == State.FINISHED

    async def test_simple_pipeline_anext(self):
        async def create_task():
            simple_task = SimpleTask(s="test")
            await simple_task.push()

        async def create_slow_task():
            asyncio.sleep(2)
            await create_task()

        await create_task()

        cntr: int = 0
        async for task in SimpleTask.queue():
            cntr += 1
            await task.finish()
            slow_task = asyncio.create_task(create_slow_task())

            if cntr == 2:
                await task.finish()
                break

        await slow_task

    async def test_simple_pipeline_failed(self):
        task = SimpleTask(s="test")
        await task.push()

        async for task in SimpleTask.queue():
            assert task.s == "test"
            await task.fail()
            break

        task = await SimpleTask.find_one({"s": "test"})
        assert task.state == State.FAILED

    @pytest.mark.parametrize(
        "priority1,priority2,result_s",
        [
            (Priority.LOW, Priority.MEDIUM, "test2"),
            (Priority.MEDIUM, Priority.LOW, "test1"),
            (Priority.MEDIUM, Priority.MEDIUM, "test1"),
        ],
    )
    async def test_multiple_tasks_simple_order(
        self, priority1, priority2, result_s
    ):
        task1 = SimpleTask(s="test1", priority=priority1)
        await task1.push()
        task2 = SimpleTask(s="test2", priority=priority2)
        await task2.push()

        async for task in SimpleTask.queue():
            assert task.s == result_s
            await task.finish()
            break

        async for task in SimpleTask.queue():
            assert task.s != result_s
            await task.finish()
            break

    async def test_if_queue_is_empty(self):
        await SimpleTask(s="test").push()
        await SimpleTask(s="test").push()

        assert not await SimpleTask.is_empty()

        await SimpleTask.pop()
        assert not await SimpleTask.is_empty()

        await SimpleTask.pop()
        assert await SimpleTask.is_empty()

    async def test_direct_dependency(self):
        simple_task_1 = SimpleTask(s="test1")
        await simple_task_1.push()

        task = TaskWithDirectDependency(
            s="test", direct_dependency=simple_task_1
        )
        await task.push()

        found_task = await TaskWithDirectDependency.pop()
        assert found_task is None

        simple_task_1.state = State.FINISHED
        await simple_task_1.save()

        found_task = await TaskWithDirectDependency.pop()
        assert found_task is not None
        assert found_task.s == "test"
        assert found_task.state == State.RUNNING

    async def test_optional_direct_dependency(self):
        task = TaskWithOptionalDependency(s="test")
        await task.push()

        found_task = await TaskWithOptionalDependency.pop()
        assert found_task is not None
        assert found_task.s == "test"
        assert found_task.state == State.RUNNING

        simple_task_1 = SimpleTask(s="test1")
        await simple_task_1.push()

        task_2 = TaskWithOptionalDependency(
            s="test2", optional_dependency=simple_task_1
        )
        await task_2.push()

        found_task = await TaskWithOptionalDependency.pop()
        assert found_task is None

        simple_task_1.state = State.FINISHED
        await simple_task_1.save()

        found_task = await TaskWithOptionalDependency.pop()
        assert found_task is not None
        assert found_task.s == "test2"
        assert found_task.state == State.RUNNING

    async def test_all_of_dependency(self):
        simple_task_1 = SimpleTask(s="test1")
        await simple_task_1.push()
        simple_task_2 = SimpleTask(s="test2")
        await simple_task_2.push()

        task = TaskWithAllOfDependency(
            s="test", all_of_dependency=[simple_task_1, simple_task_2]
        )
        await task.push()

        found_task = await TaskWithAllOfDependency.pop()
        assert found_task is None

        simple_task_1.state = State.FINISHED
        await simple_task_1.save()

        found_task = await TaskWithAllOfDependency.pop()
        assert found_task is None

        simple_task_2.state = State.FINISHED
        await simple_task_2.save()

        found_task = await TaskWithAllOfDependency.pop()
        assert found_task is not None
        assert found_task.s == "test"
        assert found_task.state == State.RUNNING

    async def test_optional_all_of_dependency(self):
        task = TaskWithOptionalAllOfDependency(s="test")
        await task.push()

        found_task = await TaskWithOptionalAllOfDependency.pop()
        assert found_task is not None
        assert found_task.s == "test"
        assert found_task.state == State.RUNNING

        simple_task_1 = SimpleTask(s="test1")
        await simple_task_1.push()

        simple_task_2 = SimpleTask(s="test2")
        await simple_task_2.push()

        task_2 = TaskWithOptionalAllOfDependency(
            s="test2",
            optional_all_of_dependency=[simple_task_1, simple_task_2],
        )
        await task_2.push()

        found_task = await TaskWithOptionalAllOfDependency.pop()
        assert found_task is None

        simple_task_1.state = State.FINISHED
        await simple_task_1.save()

        found_task = await TaskWithOptionalAllOfDependency.pop()
        assert found_task is None

        simple_task_2.state = State.FINISHED
        await simple_task_2.save()

        found_task = await TaskWithOptionalAllOfDependency.pop()
        assert found_task is not None
        assert found_task.s == "test2"
        assert found_task.state == State.RUNNING

    async def test_any_of_dependency(self):
        simple_task_1 = SimpleTask(s="test1")
        await simple_task_1.push()
        simple_task_2 = SimpleTask(s="test2")
        await simple_task_2.push()

        task = TaskWithAnyOfDependency(
            s="test", any_of_dependency=[simple_task_1, simple_task_2]
        )
        await task.push()

        found_task = await TaskWithAnyOfDependency.pop()
        assert found_task is None

        simple_task_1.state = State.FINISHED
        await simple_task_1.save()

        found_task = await TaskWithAnyOfDependency.pop()
        assert found_task is not None
        assert found_task.s == "test"
        assert found_task.state == State.RUNNING

    async def test_optional_any_of_dependency(self):
        task = TaskWithOptionalAnyOfDependency(s="test")
        await task.push()

        found_task = await TaskWithOptionalAnyOfDependency.pop()
        assert found_task is not None
        assert found_task.s == "test"
        assert found_task.state == State.RUNNING

        simple_task_1 = SimpleTask(s="test1")
        await simple_task_1.push()

        simple_task_2 = SimpleTask(s="test2")
        await simple_task_2.push()

        task_2 = TaskWithOptionalAnyOfDependency(
            s="test2",
            optional_any_of_dependency=[simple_task_1, simple_task_2],
        )
        await task_2.push()

        found_task = await TaskWithOptionalAnyOfDependency.pop()
        assert found_task is None

        simple_task_1.state = State.FINISHED
        await simple_task_1.save()

        found_task = await TaskWithOptionalAnyOfDependency.pop()
        assert found_task is not None
        assert found_task.s == "test2"
        assert found_task.state == State.RUNNING

    async def test_simple_category_pipeline(self):
        task_type2 = SimpleTask(s="test2", category="type2")
        await task_type2.push()

        task_type1 = SimpleTask(s="test1", category="type1")
        await task_type1.push()

        assert len(await SimpleTask.find({}).to_list()) == 2

        async for task in SimpleTask.queue(category="type1"):
            assert task.s == "test1"
            await task.finish()
            assert await SimpleTask.is_empty(category="type1")
            assert not await SimpleTask.is_empty(category="type2")
            break

        task_type1 = await SimpleTask.find_one({"s": "test1"})
        assert task_type1.state == State.FINISHED

        task_type2 = await SimpleTask.find_one({"s": "test2"})
        assert task_type2.state == State.CREATED

    async def test_simple_category_pipeline_failed(self):
        task_type1 = SimpleTask(s="test1", category="type1")
        await task_type1.push()

        task_type2 = SimpleTask(s="test2", category="type2")
        await task_type2.push()

        assert len(await SimpleTask.find({}).to_list()) == 2

        async for task in SimpleTask.queue(category="type1"):
            assert task.s == "test1"
            await task.fail()
            assert await SimpleTask.is_empty(category="type1")
            assert not await SimpleTask.is_empty(category="type2")
            break

        task_type1 = await SimpleTask.find_one({"s": "test1"})
        assert task_type1.state == State.FAILED

        task_type2 = await SimpleTask.find_one({"s": "test2"})
        assert task_type2.state == State.CREATED

    async def test_category_normalization(self):
        task_type_empty = SimpleTask(s="test_empty")
        await task_type_empty.push()
        assert await SimpleTask.pop()
        await task_type_empty.finish()

        task_type_string = SimpleTask(s="test_string", category="string")
        await task_type_string.push()
        assert await SimpleTask.pop(category="string")
        await task_type_string.finish()

        task_type_set = SimpleTask(s="test_set", category="set")
        await task_type_set.push()
        assert await SimpleTask.pop(category={"set"})
        await task_type_set.finish()

        task_type_list = SimpleTask(s="test_list", category="list")
        await task_type_list.push()
        assert await SimpleTask.pop(category=["list"])
        await task_type_list.finish()

    async def test_multi_category_queue(self):
        task_empty = SimpleTask(s="test_empty")
        await task_empty.push()

        task_cat_1 = SimpleTask(s="test_cat_1", category="cat_1")
        await task_cat_1.push()

        task_cat_2 = SimpleTask(s="test_cat_2", category="cat_2")
        await task_cat_2.push()

        assert len(await SimpleTask.find({}).to_list()) == 3

        cntr = 0
        async for task in SimpleTask.queue(category=["cat_1", "cat_2"]):
            assert task.s != "test_empty"
            await task.finish()
            cntr += 1
            assert await SimpleTask.is_empty(category=task.category)
            if cntr == 2:
                break
