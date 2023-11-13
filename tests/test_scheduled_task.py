import asyncio
from datetime import datetime, timedelta

from beanie_batteries_queue import State
from tests.tasks import SimpleScheduledTask, ScheduledTaskWithInterval


class TestScheduledTask:
    async def test_simple_task(self):
        task = SimpleScheduledTask(
            s="test", run_at=datetime.utcnow() + timedelta(seconds=2)
        )
        assert task.run_at > datetime.utcnow()
        await task.push()

        found_task = await SimpleScheduledTask.pop()
        assert found_task is None

        await asyncio.sleep(2)

        found_task = await SimpleScheduledTask.pop()
        assert found_task.s == "test"
        assert found_task.state == State.RUNNING

    async def test_task_with_interval(self):
        task = ScheduledTaskWithInterval(
            s="test", run_at=datetime.utcnow() + timedelta(seconds=2)
        )
        assert task.run_at > datetime.utcnow()
        await task.push()

        found_task = await ScheduledTaskWithInterval.pop()
        assert found_task is None

        await asyncio.sleep(2)

        found_task = await ScheduledTaskWithInterval.pop()
        assert found_task.s == "test"
        assert found_task.state == State.RUNNING
        await found_task.finish()

        found_task = await ScheduledTaskWithInterval.pop()
        assert found_task is None

        await asyncio.sleep(5)

        found_task = await ScheduledTaskWithInterval.pop()
        assert found_task.s == "test"
        assert found_task.state == State.RUNNING
