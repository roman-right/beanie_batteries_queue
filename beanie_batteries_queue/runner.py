import asyncio
import logging
import multiprocessing
from multiprocessing import Process
from multiprocessing.synchronize import Event
from time import sleep
from typing import List, Type

from beanie_batteries_queue.task import Task
from beanie_batteries_queue.worker import Worker

logger = logging.getLogger(__name__)


class Runner:
    def __init__(
        self,
        task_classes: List[Type[Task]],
        worker_count: int = 1,
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

    def start(self, run_indefinitely: bool = True):
        """
        Start the task runner.

        :param run_indefinitely: Run the runner while all tasks are alive.
        """
        for _ in range(self.worker_count):
            stop_event = multiprocessing.Event()
            process = multiprocessing.Process(
                target=self.run_worker, args=(stop_event,)
            )
            process.start()
            logger.info(f"Started worker process {process.pid}")
            self.processes.append(process)
            self.stop_events.append(stop_event)
        if run_indefinitely:
            self.infinite_status_check()

    def check_status(self):
        """
        Check the status of the task runner.
        """
        return any([process.is_alive() for process in self.processes])

    def infinite_status_check(self):
        """
        Check the status of the task runner.
        """
        while True:
            try:
                status = self.check_status()
                if not status:
                    break
                sleep(1)
            except KeyboardInterrupt:
                logger.info("Keyboard interrupt detected")
                self.stop()
                break

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
        logger.info("Stopping workers...")
        # Signal each worker to stop
        for stop_event in self.stop_events:
            stop_event.set()

        # Wait for all processes to finish
        for process in self.processes:
            process.join()
