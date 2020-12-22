# MIT License

# Copyright (c) 2020 Filip Kofron

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import threading, queue
from typing import Callable, List
from functools import partial

# TODO: Offer tasks without continuations, continuations via the executor and allow functions without executor in argument.
class Task:
    """
    Represents a work schedulable in an executor.
     - A task can only be executed once, but it can create continuations as well as define dependencies.
     - A continuation as a task becomes a dependency of other tasks depending on the original task.
     - Any task created needs to be manually executed in an executor. Task execution provides the current executor as its parameter.
     """
    def __init__(self, work: Callable[["Executor"], List["Task"]], dependencies: List["Task"] = []):
        self.lock = threading.Lock()
        self.work = work
        self.being_executed = False
        self.done = False
        self.scheduled_executor_callback = None
        self.continuations = []
        self.dependencies = []
        self.dependants : List[Task] = []
        self.dependencies_done = 0

        self.__add_dependencies(dependencies)

    def execute(self, executor: "Executor") -> None:
        """ Executes this task. Can only be done once and after all dependencies are satisfied. """
        if self.done:
            raise Exception("Task already executed")
        if self.being_executed:
            raise Exception("Task is already being executed")
        self.being_executed = True
        if len(self.dependencies) != self.dependencies_done:
            raise Exception("Dependencies not satisfied")

        # Actual execution
        continuations = self.work(executor)

        # All dependants need to add continuations as their dependencies
        if len(continuations) > 0:
            with self.lock:
                self.continuations = continuations
                for dependant in self.dependants:
                    dependant.__add_dependencies(self.continuations)

        # All is clear, we can signal that we are finished to all dependants
        with self.lock:
            for dependant in self.dependants:
                dependant.__notify_done(self)
            self.done = True
            # Task is officially done

    def on_schedule(self, callback: Callable[["Task"], None]) -> bool:
        """ Returns whether this task has satisfied dependencies to be executed. If not, callback to schedule it again during dependency satisfaction is stored. """
        with self.lock:
            if self.scheduled_executor_callback != None:
                raise Exception("Task has already been scheduled once!")
            if self.done:
                raise Exception("Task already executed!")
            if len(self.dependencies) > self.dependencies_done:
                self.scheduled_executor_callback = callback
                return False
        return True

    def __add_dependencies(self, dependencies: List["Task"]) -> None:
        with self.lock:
            for dependency in dependencies:
                if dependency.__try_add_dependants([self]):
                    self.dependencies.append(dependency)

    def __try_add_dependants(self, dependants: List["Task"]) -> bool:
        with self.lock:
            if self.done:
                return False
            self.dependants.extend(dependants)
        return True

    def __notify_done(self, dependency: "Task") -> None:
        with self.lock:
            self.dependencies_done += 1
            if self.dependencies_done == len(self.dependencies):
                self.scheduled_executor_callback(self)
                self.scheduled_executor_callback = lambda task: None

class Executor:
    """ A task queue and a thread pool for processing the queue. Offers a manual mode.  """
    def __init__(self, n_threads = -1, manual_execution = False):
        self.queue = queue.Queue()
        self.canceled = False
        self.joining = False
        self.manual_execution = manual_execution
        if n_threads == -1 and not self.manual_execution:
            n_threads = threading.active_count()
        elif n_threads <= 0 and not self.manual_execution:
            raise Exception(f"Incorrect number of threads: {n_threads}")

        if self.manual_execution and n_threads > 0:
            raise Exception("There should be no threads during manual execution")

        self.threads = []
        for i in range(n_threads):
            thread = threading.Thread(target=self.__threadFunc)
            thread.start()
            self.threads.append(thread)
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.wait_until_tasks_done()
        self.join()

    def schedule_task(self, task: Task) -> None:
        """ Schedule task for execution now or later when its dependencies are satisfied. Can only be done once for the given task. """
        if self.joining:
            raise Exception("No task can be scheduled during joining.")
        if task.on_schedule(self.__wake_callback):
            self.queue.put(task)

    def schedule_func(self, function, *args) -> Task:
        """ Schedules function as a task with no depencencies and returns the task. """
        return self.schedule_func_with_deps(function, [], *args)

    def schedule_func_with_deps(self, function, dependencies: List[Task], *args) -> Task:
        """ Schedule task with given dependencies. Task will only be executed after all dependencies have been satisfied. Can only be done once for the given task. """
        task = Task(partial(function, *args), dependencies)
        self.schedule_task(task)
        return task

    def make_task(self, function, *args):
        """ Returns a new unscheduled task from the function and given arguments. """
        return self.make_task_with_deps(function, [], *args)

    def make_task_with_deps(self, function, dependencies: List[Task], *args):
        """ Returns a new unscheduled task from the function, dependencies and given arguments. """
        return Task(partial(function, *args), dependencies)

    def __wake_callback(self, task: Task):
        self.queue.put(task)

    def __execute_single(self) -> bool:
        task_exception = None
        try:
            # TODO: configurable timeout
            timeout = 0.2
            if self.manual_execution:
                timeout = 0
            task = self.queue.get(timeout=timeout)
            try:
                task.execute(self)
            except Exception as e:
                task_exception = e
            self.queue.task_done()
        except:
            pass
        if task_exception:
            raise task_exception

        return self.queue.qsize() > 0

    def __threadFunc(self):
        while not self.canceled:
            self.__execute_single()
            if self.joining and self.queue.qsize() == 0:
                break

    def manual_execute(self) -> bool:
        """ For executor in manual mode, execute a single task. Returns whether there are any remaining tasks in queue. Thread-safe. """
        if not self.manual_execution:
            raise Exception("Executor not in manual mode!")
        return self.__execute_single()

    def join(self):
        """ Wait for all tasks to be done and causes all threads in the executor to finish and join. No task can be scheduled at this point."""
        self.wait_until_tasks_done()
        self.joining = True
        for thread in self.threads:
            wake_task = Task(lambda executor: [])
            self.queue.put(wake_task)
        for thread in self.threads:
            thread.join()

    def wait_until_tasks_done(self):
        """ Blocks a thread until the queue is  """
        self.queue.join()

    def cancel(self):
        """ Stop processing new tasks (possibly leaving unfinished ones on the queue) and join all threads. """
        self.canceled = True
        while self.queue.qsize() > 0:
            try:
                # TODO: configurable timeout
                timeout = 0.2
                self.queue.get(timeout=timeout)
                self.queue.task_done()
            except:
                pass
        self.join()

class Result:
    """ Represents a value, which can either have a set value or an exception to be raised upon retrieval of the value. """
    def __init__(self):
        self.__value = None
        self.__result_set = False
        self.__exception = None
    
    def set_value(self, value):
        """ Set the result value. Can only be called once. """
        if self.__result_set:
            raise Exception("Result already set")
        self.__value = value
        self.__result_set = True

    def set_exception(self, exception):
        if self.__exception != None:
            raise Exception("Exception already set")
        self.__exception = exception
        self.__result_set = True

    def retrieve_result(self):
        if not self.__result_set:
            raise Exception("Result wasn't set")
        if self.__exception:
            raise self.__exception
        return self.__value

class AsyncResult:
    """ Binds result and its task creating an asynchronous result. Use a new task on any executor with dependency to this result to retrieve the value. """
    def __init__(self, result: Result, task: Task):
        self.result = result
        self.task = task

    # TODO: Offer retrieval using blocking operation (temporary executor) (using dependency notificaiton?)
    def retrieve_result(self):
        if not self.task.done:
            raise Exception("Task wasn't yet executed")
        return self.result.retrieve_result()

# TODO: Delete, this functionality should be provided natively by executor.
def wrap_async_task(funcTask, *args) -> AsyncResult:
    """ Helper (to be deprecated) for wrapping a function taking result and executor and creating AsyncResult out of it. The function is responsible to set the result. """
    result = Result()
    task = Task(partial(funcTask, *args, result))
    return AsyncResult(result, task)

# TODO: Accept async results natively in the executor
def async_deps(list: List[AsyncResult]) -> List[Task]:
    """ Helper (to be deprecated) to extract task list from multiple async results. """
    return [result.task for result in list]
