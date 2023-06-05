import pytest

from beanie_batteries_queue.queue import State, Priority
from tests.tasks import SimpleTask


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
