import asyncio
from datetime import datetime, timedelta

from beanie_batteries_queue import State
from tests.tasks import SimpleScheduledTask


class TestScheduledTask:
    async def test_simple_task(self):
        task = SimpleScheduledTask(s="test", run_at=datetime.utcnow() + timedelta(seconds=2))
        assert task.run_at > datetime.utcnow()
        await task.push()

        found_task = await SimpleScheduledTask.pop()
        assert found_task is None

        await asyncio.sleep(2)

        found_task = await SimpleScheduledTask.pop()
        assert found_task.s == "test"
        assert found_task.state == State.RUNNING
