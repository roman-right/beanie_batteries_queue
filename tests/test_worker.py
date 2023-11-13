import asyncio

import pytest

from beanie_batteries_queue import State
from beanie_batteries_queue.worker import Worker
from tests.tasks import SimpleTask, AnotherSimpleTask


@pytest.mark.asyncio
class TestWorker:
    async def test_worker_starts_and_stops_queues(self):
        # Set up two different task types
        task1 = SimpleTask(s="task1")
        await task1.push()
        task2 = AnotherSimpleTask(s="task2")
        await task2.push()

        # Initialize the worker with both task classes
        worker = Worker([SimpleTask, AnotherSimpleTask])

        # Start the worker
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(1)  # Allow some time for queues to start

        # Stop the worker
        worker.stop()
        await task

        # Verify that tasks have been processed
        assert (
            await SimpleTask.find_one({"s": "task1".upper()})
        ).state == State.FINISHED
        assert (
            await AnotherSimpleTask.find_one({"s": "task2".upper()})
        ).state == State.FINISHED
