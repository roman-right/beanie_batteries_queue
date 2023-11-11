import asyncio
import multiprocessing
from typing import List, Type

from beanie_batteries_queue.task import Task
from beanie_batteries_queue.worker import Worker


class Runner:
    def __init__(self, task_classes: List[Type[Task]], worker_count: int):
        """
        Initialize the Runner.

        :param task_classes: List of Task classes to run tasks from.
        :param worker_count: Number of concurrent workers.
        """
        self.task_classes = task_classes
        self.worker_count = worker_count
        self.processes = []
        self.stop_events = []

    def start(self):
        """
        Start the task runner.
        """
        for _ in range(self.worker_count):
            stop_event = multiprocessing.Event()
            process = multiprocessing.Process(target=self.run_worker, args=(stop_event,))
            process.start()
            self.processes.append(process)
            self.stop_events.append(stop_event)

    def run_worker(self, stop_event):
        """
        Set up an asyncio event loop and run the worker.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        worker = Worker(self.task_classes, stop_event)
        loop.run_until_complete(worker.start())
        loop.close()

    def stop(self):
        """
        Stop the task runner.
        """
        # Signal each worker to stop
        for stop_event in self.stop_events:
            stop_event.set()

        # Wait for all processes to finish
        for process in self.processes:
            process.join()