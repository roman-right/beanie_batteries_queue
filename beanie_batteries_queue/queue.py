import asyncio
from multiprocessing.synchronize import Event
from typing import TYPE_CHECKING, Optional
from typing import Type

if TYPE_CHECKING:
    from beanie_batteries_queue.task import Task


class Queue:
    def __init__(
        self,
        task_model: Type["Task"],
        sleep_time: int = 1,
        stop_event: Optional[Event] = None,
    ):
        """
        Initialize the Queue.

        :param task_model: Task model class
        :param sleep_time: Sleep time between iterations
        :param stop_event: Event to stop the queue
        """
        self.task_model = task_model
        self.sleep_time = sleep_time
        self.started = False
        self.running = False
        self.stop_event = stop_event

    def __aiter__(self):
        return self

    async def __anext__(self):
        def check_exit():
            if self.started and not self.running:
                self.started = False
                raise StopAsyncIteration
            if self.stop_event and self.stop_event.is_set():
                self.running = False
                self.started = False
                raise StopAsyncIteration

        check_exit()
        task = await self.task_model.pop()
        while task is None:
            check_exit()
            await asyncio.sleep(self.sleep_time)
            task = await self.task_model.pop()
        return task

    async def start(self):
        """
        Run task
        """
        if self.started:
            raise RuntimeError("Queue is already started")
        self.started = True
        self.running = True
        async for task in self:
            try:
                await task.run()
                await task.finish()
            except Exception:
                await task.fail()

    def stop(self):
        """
        Stop the task runner.
        """
        self.running = False
