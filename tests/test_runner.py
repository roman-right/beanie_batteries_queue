import pytest
import asyncio

from beanie_batteries_queue.runner import Runner

from beanie_batteries_queue import State
from tests.tasks import (
    SimpleTask,
    AnotherSimpleTask,
    SimpleTaskWithLongProcessingTime,
)


@pytest.mark.asyncio
class TestRunner:
    async def test_runner_starts_and_stops_workers(self):
        # Set up tasks for testing
        task1 = SimpleTask(s="task1")
        await task1.push()
        task2 = AnotherSimpleTask(s="task2")
        await task2.push()

        # Initialize the runner with two worker processes
        runner = Runner([SimpleTask, AnotherSimpleTask], worker_count=2)

        # Start the runner
        runner.start(run_indefinitely=False)
        await asyncio.sleep(
            2
        )  # Allow some time for workers to start and process tasks

        # Stop the runner
        runner.stop()

        # Verify that tasks have been processed
        assert (
            await SimpleTask.find_one({"s": "task1".upper()})
        ).state == State.FINISHED
        assert (
            await AnotherSimpleTask.find_one({"s": "task2".upper()})
        ).state == State.FINISHED

    async def test_runner_processes_tasks_in_parallel(self):
        # Set up multiple tasks
        tasks = [
            SimpleTaskWithLongProcessingTime(s=f"task{i}") for i in range(5)
        ]
        for task in tasks:
            await task.push()

        # Initialize and start the runner with multiple workers
        runner = Runner([SimpleTaskWithLongProcessingTime], worker_count=5)
        runner.start(run_indefinitely=False)
        await asyncio.sleep(6)  # Allow time for parallel processing

        # Stop the runner
        runner.stop()

        # Verify that all tasks have been processed
        for i in range(5):
            assert (
                await SimpleTaskWithLongProcessingTime.find_one(
                    {"s": f"task{i}".upper()}
                )
            ).state == State.FINISHED

    async def test_status_check(self):
        # Set up tasks for testing
        task1 = SimpleTask(s="task1")
        await task1.push()
        task2 = AnotherSimpleTask(s="task2")
        await task2.push()

        # Initialize the runner with two worker processes
        runner = Runner([SimpleTask, AnotherSimpleTask], worker_count=2)

        # Start the runner
        runner.start(run_indefinitely=False)
        await asyncio.sleep(2)
        status = runner.check_status()
        assert status is True

        # Stop the runner
        runner.stop()

        status = runner.check_status()
        assert status is False
