# Task Queue

Task Queue is an advanced queue system for Beanie (MongoDB), designed to efficiently manage and process tasks. It features task priorities, states, dependencies, and automatic expiration. Different task queues can be processed together using the Worker class. Multiple workers can be run in separate processes using the Runner class.

## Installation

```shell
pip install beanie[queue]
```

## Example

```python
from beanie_batteries_queue import Task, Runner

class ExampleTask(Task):
    data: str

    async def run(self):
        self.data = self.data.upper()
        await self.save()
        
runner = Runner(task_classes=[ExampleTask])
runner.start()
```

## Task

### Declare a task class

```python
from beanie_batteries_queue import Task


class SimpleTask(Task):
    s: str
```

### Process a task

```python
from beanie_batteries_queue import State

# Producer
task = SimpleTask(s="test")
await task.push()

# Consumer
async for task in SimpleTask.queue():
    assert task.s == "test"
    # Do some work
    await task.finish()
    break

# Check that the task is finished
task = await SimpleTask.find_one({"s": "test"})
assert task.state == State.FINISHED
```

Async generator `SimpleTask.queue()` will return all unfinished tasks in the order they were created or based on the
priority if it was specified. It is an infinite loop, so you can use `break` to stop it.

You can also use `SimpleTask.pop()` to get the next task from the queue.

```python
from beanie_batteries_queue import State

# Producer
task = SimpleTask(s="test")
await task.push()

# Consumer
task = await SimpleTask.pop()
assert task.s == "test"
# Do some work
await task.finish()
```

### Task priority

There are three priority levels: `LOW`, `MEDIUM`, and `HIGH`. The default priority is `MEDIUM`.
Tasks are popped from the queue in the following order: `HIGH`, `MEDIUM`, `LOW`.

```python
from beanie_batteries_queue import Priority

task1 = SimpleTask(s="test1", priority=Priority.LOW)
await task1.push()
task2 = SimpleTask(s="test2", priority=Priority.HIGH)
await task2.push()

async for task in SimpleTask.queue():
    assert task.s == "test2"
    await task.finish()
    break
```

### Task state

There are four states: `CREATED`, `RUNNING`, `FINISHED`, and `FAILED`. The default state is `PENDING`.
When a task is pushed, it is in the `CREATED` state. When it gets popped from the queue, it is in the `RUNNING`
state. `FINISHED` and `FAILED` states should be set manually.

Finished:

```python
from beanie_batteries_queue import State

task = SimpleTask(s="test")
await task.push()

async for task in SimpleTask.queue():
    assert task.state == State.RUNNING
    await task.finish()
    break

task = await SimpleTask.find_one({"s": "test"})
assert task.state == State.FINISHED
```

Failed:

```python
from beanie_batteries_queue import State

task = SimpleTask(s="test")
await task.push()

async for task in SimpleTask.queue():
    assert task.state == State.RUNNING
    await task.fail()
    break

task = await SimpleTask.find_one({"s": "test"})
assert task.state == State.FAILED
```

### Task dependencies

You can specify that a task depends on another task. In this case, the task will be popped from the queue only when all
its dependencies have finished.

```python
from beanie_batteries_queue import Task, DependencyType
from beanie_batteries_queue import Link
from pydantic import Field


class SimpleTask(Task):
    s: str


class TaskWithDirectDependency(Task):
    s: str
    direct_dependency: Link[SimpleTask] = Field(
        dependency_type=DependencyType.DIRECT
    )
```

```python
from beanie_batteries_queue import State

task1 = SimpleTask(s="test1")
await task1.push()

task2 = TaskWithDirectDependency(s="test2", direct_dependency=task1)
await task2.push()

task_from_queue = await TaskWithDirectDependency.pop()
assert task_from_queue is None
# task2 is not popped from the queue because task1 is not finished yet

await task1.finish()

task_from_queue = await TaskWithDirectDependency.pop()
assert task_from_queue is not None
# task2 is popped from the queue because task1 is finished
```

### Task dependencies with multiple links

You can specify that a task depends on multiple tasks. In this case, the task will be popped from the queue when all or
any its dependencies are finished. It is controlled by the `dependency_type` parameter.

All

```python
class TaskWithMultipleDependencies(Task):
    s: str
    list_of_dependencies: Link[SimpleTask] = Field(
        dependency_type=DependencyType.ALL_OF
    )
```

Any

```python
class TaskWithMultipleDependencies(Task):
    s: str
    list_of_dependencies: Link[SimpleTask] = Field(
        dependency_type=DependencyType.ANY_OF
    )
```

Tasks can have multiple links with different dependency types.

```python
class TaskWithMultipleDependencies(Task):
    s: str
    list_of_dependencies_all: Link[SimpleTask] = Field(
        dependency_type=DependencyType.ALL_OF
    )
    list_of_dependencies_any: Link[SimpleTask] = Field(
        dependency_type=DependencyType.ANY_OF
    )
    direct_dependency: Link[SimpleTask] = Field(
        dependency_type=DependencyType.DIRECT
    )
```

### Expire time

You can specify the time after which the task will be removed from the queue, even if it is not finished or has failed.
This is controlled by the `expireAfterSeconds` index, which is set to 24 hours by default.

```python
from pymongo import ASCENDING
from beanie_batteries_queue import Task


class TaskWithExpireTime(Task):
    s: str

    class Settings:
        indexes = [
            # Other indexes,

            # Expire after 5 minutes
            [("created_at", ASCENDING), ("expireAfterSeconds", 300)],
        ]
```

Finished or failed tasks are not immediately removed from the queue. They are removed after the expiration time. You can
manually delete them using the `delete()` method.

## Queue

Queues are designed to manage tasks. It will handle all the logic of creating, updating, and deleting tasks. Task logic
should be defined in the `run` method of the task

```python
from beanie_batteries_queue import Task


class ProcessTask(Task):
    data: str

    async def run(self):
        # Implement the logic for processing the task
        print(f"Processing task with data: {self.data}")
        self.data = self.data.upper()
        await self.save()
```

Now we can start the queue and it will process all the tasks. Be aware - it will run infinite loop. If you want to have
another logic after starting the queue, you should run it with `asyncio.create_task()`.

```python
queue = ProcessTask.queue()
await queue.start()
```

### Stop the queue

You can stop the queue by calling the `stop()` method.

```python
await queue.stop()
```

### Queue settings

You can specify how frequently the queue will check for new tasks. The default value is 1 second.

```python
queue = ProcessTask.queue(sleep_time=60)  # 60 seconds
await queue.start()
```
## Worker

Queue can handle only one task model. To process multiple task models, you should use Worker. It will run multiple queues

```python
from beanie_batteries_queue import Task, Worker

class ProcessTask(Task):
    data: str

    async def run(self):
        self.data = self.data.upper()
        await self.save()

class AnotherTask(Task):
    data: str

    async def run(self):
        self.data = self.data.upper()
        await self.save()
    

worker = Worker(task_classes=[ProcessTask, AnotherTask])
await worker.start()
```

Be aware - it will run infinite loop. If you want to have another logic after starting the worker, you should run it with `asyncio.create_task()`.

### Stop the worker

You can stop the worker by calling the `stop()` method.

```python
await worker.stop()
```

### Worker settings

You can specify how frequently the worker will check for new tasks. The default value is 1 second.

```python
worker = Worker(task_classes=[ProcessTask, AnotherTask], sleep_time=60)  # 60 seconds
await worker.start()
```

## Runner

Runner is a class that allows you to run multiple workers in separate processes. It is useful when your tasks are CPU intensive and you want to use all the cores of your CPU.

```python
from beanie_batteries_queue import Task, Runner

class ProcessTask(Task):
    data: str

    async def run(self):
        self.data = self.data.upper()
        await self.save()

class AnotherTask(Task):
    data: str

    async def run(self):
        self.data = self.data.upper()
        await self.save()

runner = Runner(task_classes=[ProcessTask, AnotherTask])
runner.start()
```

### Stop the runner

You can stop the runner by calling the `stop()` method.

```python
runner.stop()
```

### Runner settings

You can specify how many workers will be run. The default value is 1.

```python
runner = Runner(task_classes=[ProcessTask, AnotherTask], workers_count=4)
runner.start()
```

You can specify how frequently the worker will check for new tasks. The default value is 1 second.

```python
runner = Runner(task_classes=[ProcessTask, AnotherTask], sleep_time=60)  # 60 seconds
runner.start()
```

You can specify if the start method should run while the workers are alive or if it should return immediately. The default value is True.

```python
runner = Runner(task_classes=[ProcessTask, AnotherTask], run_indefinitely=False)
runner.start()
```
