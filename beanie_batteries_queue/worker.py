import asyncio
import logging
import multiprocessing
from typing import List, Type, Optional

from beanie_batteries_queue import Task

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, task_classes: List[Type[Task]], stop_event: Optional[multiprocessing.Event] = None):
        """
        Initialize the Worker.

        :param task_classes: List of Task classes to run tasks from.
        """
        self.task_classes = task_classes
        self.queues = [task.queue() for task in self.task_classes]
        self.stop_event = stop_event

    async def start(self):
        """
        Run the worker.
        """
        print("STart worker")
        for queue in self.queues:
            asyncio.create_task(queue.start())

    def stop(self):
        """
        Stop the worker.
        """
        for queue in self.queues:
            queue.stop()
