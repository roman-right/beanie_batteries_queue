import asyncio
import multiprocessing
from multiprocessing import Process
from multiprocessing.synchronize import Event
from typing import List, Type

from beanie_batteries_queue.task import Task
from beanie_batteries_queue.worker import Worker


class Runner:
    def __init__(
        self,
        task_classes: List[Type[Task]],
        worker_count: int,
        sleep_time: int = 1,
    ):
        """
        Initialize the Runner.

        :param task_classes: List of Task classes to run tasks from.
        :param worker_count: Number of concurrent workers.
        :param sleep_time: Time to sleep between iterations.
        """
        self.task_classes = task_classes
        self.worker_count = worker_count
        self.sleep_time = sleep_time
        self.processes: List[Process] = []
        self.stop_events: List[Event] = []

    def start(self):
        """
        Start the task runner.
        """
        for _ in range(self.worker_count):
            stop_event = multiprocessing.Event()
            process = multiprocessing.Process(
                target=self.run_worker, args=(stop_event,)
            )
            process.start()
            print(f"Started worker process {process.pid}")
            self.processes.append(process)
            self.stop_events.append(stop_event)

    def run_worker(self, stop_event):
        """
        Set up an asyncio event loop and run the worker.
        """
        loop = asyncio.new_event_loop()
        loop.custom_id = multiprocessing.current_process().pid
        asyncio.set_event_loop(loop)

        worker = Worker(
            self.task_classes,
            sleep_time=self.sleep_time,
            stop_event=stop_event,
        )
        loop.run_until_complete(worker.start())
        loop.close()

    def stop(self):
        """
        Stop the task runner.
        """
        print("Stopping workers...")
        # Signal each worker to stop
        for stop_event in self.stop_events:
            stop_event.set()

        # Wait for all processes to finish
        for process in self.processes:
            process.join()
