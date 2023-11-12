import asyncio
import logging
from multiprocessing.synchronize import Event
from typing import List, Type, Optional

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from beanie_batteries_queue import Task

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        task_classes: List[Type["Task"]],
        sleep_time: int = 1,
        stop_event: Optional[Event] = None,
    ):
        """
        Initialize the Worker.

        :param task_classes: List of Task classes to run tasks from.
        :param sleep_time: Time to sleep between iterations.
        :param stop_event: Event to stop the worker.
        """
        self.task_classes = task_classes
        self.queues = [
            task.queue(sleep_time=sleep_time, stop_event=stop_event)
            for task in self.task_classes
        ]
        self.stop_event = stop_event

    async def start(self):
        """
        Run the worker.
        """
        coros = [queue.start() for queue in self.queues]
        await asyncio.gather(*coros)

    def stop(self):
        """
        Stop the worker.
        """
        for queue in self.queues:
            queue.stop()
