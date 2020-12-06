import logging, threading, queue
from typing import Callable, List, Optional, Tuple
from functools import partial
import psutil

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
        with self.lock:
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
    def __init__(self, n_threads = -1, manual_execute = False):
        self.queue = queue.Queue()
        self.canceled = False
        self.joining = False
        if n_threads == -1 and not manual_execute:
            n_threads = len(psutil.Process().cpu_affinity())

        elif n_threads <= 0 and not manual_execute:
            raise Exception(f"Incorrect number of threads: {n_threads}")

        if manual_execute and n_threads > 0:
            raise Exception("There should be no threads during manual execution")

        self.threads = []
        for i in range(n_threads):
            thread = threading.Thread(target=self.__threadFunc)
            thread.start()
            self.threads.append(thread)
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.waitUntilTasksDone()
        self.join()

    def schedule(self, task: Task) -> None:
        if task.on_schedule(self.__wakeCallback):
            self.queue.put(task)

    def __wakeCallback(self, task: Task):
        self.queue.put(task)

    def __executeSingle(self):
        task_exception = None
        try:
            task = self.queue.get(timeout=0.2)
            try:
                task.execute(self)
            except Exception as e:
                task_exception = e
            self.queue.task_done()
        except:
            pass

        if task_exception:
            raise task_exception

    def __threadFunc(self):
        while not self.canceled:
            self.__executeSingle()
            if self.joining and self.queue.qsize() == 0:
                break

    def manualExecute(self):
        self.__executeSingle()

    def join(self):
        self.joining = True
        for thread in self.threads:
            wake_task = Task(lambda executor: [])
            self.queue.put(wake_task)
        for thread in self.threads:
            thread.join()

    def waitUntilTasksDone(self):
        self.queue.join()

    def cancel(self):
        self.canceled = True
        self.join()

class Result:
    def __init__(self):
        self.__value = None
        self.__result_set = False
        self.__exception = None
    
    def setValue(self, value):
        if self.__value != None:
            raise Exception("Result already set")
        if self.__exception != None:
            raise Exception("Exception already set")
        self.__value = value
        self.__result_set = True

    def setException(self, exception):
        if self.__exception != None:
            raise Exception("Exception already set")
        if self.__value != None:
            raise Exception("Result already set")
        self.__exception = exception
        self.__result_set = True

    def retrieveResult(self):
        if not self.__result_set:
            raise Exception("Result wasn't set")
        if self.__exception:
            raise self.__exception
        return self.__value

class AsyncResult:
    def __init__(self, result: Result, task: Task):
        self.result = result
        self.task = task

    def retrieveResult(self):
        if not self.task.done:
            raise Exception("Task wasn't yet executed")
        return self.result.retrieveResult()

def wrapAsyncTask(funcTask, *args) -> AsyncResult:
    result = Result()
    task = Task(partial(funcTask, *args, result))
    return AsyncResult(result, task)

def asyncDeps(list: List[AsyncResult]) -> List[Task]:
    return [result.task for result in list]
