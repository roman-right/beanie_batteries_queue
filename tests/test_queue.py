import asyncio

from beanie_batteries_queue import State
from tests.tasks import SimpleTask, FailingTask


class TestQueue:
    async def test_queue_retrieves_task(self):
        task = SimpleTask(s="test")
        await task.push()

        queue = SimpleTask.queue()
        retrieved_task = await queue.__anext__()
        assert retrieved_task is not None
        assert retrieved_task.s == "test"

    async def test_queue_process_tasks(self):
        task1 = SimpleTask(s="task1")
        await task1.push()
        task2 = SimpleTask(s="task2")
        await task2.push()

        queue = SimpleTask.queue()

        # Start the queue in a separate coroutine
        task = asyncio.create_task(queue.start())
        await asyncio.sleep(1)
        queue.stop()
        await task

        assert (
            await SimpleTask.find_one(SimpleTask.s == "task1".upper())
        ).state == State.FINISHED
        assert (
            await SimpleTask.find_one(SimpleTask.s == "task2".upper())
        ).state == State.FINISHED

    async def test_queue_handle_task_failures(self):
        task = FailingTask(s="fail")
        await task.push()

        queue = FailingTask.queue()

        task = asyncio.create_task(queue.start())
        await asyncio.sleep(1)
        queue.stop()
        await task

        assert (
            await FailingTask.find_one({"s": "fail"})
        ).state == State.FAILED

    async def test_queue_start_stop(self):
        queue = SimpleTask.queue()
        task = asyncio.create_task(queue.start())
        await asyncio.sleep(1)  # Let the queue start
        assert queue.running is True
        queue.stop()
        await task
        assert queue.running is False
