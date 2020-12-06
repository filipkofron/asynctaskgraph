from typing import Callable, List
from tasks import AsyncResult, Result, Task, Executor
from functools import partial, reduce
import logging
import requests
import time

logging.root.setLevel(logging.DEBUG)
logging.basicConfig(format="%(threadName)s:\t%(message)s")
start = time.time()

def downloadUrl(url: str, executor: Executor):
    with requests.get(url) as r:
        return r.text

def downloadTask(download_func: Callable[[],str], resultt: List[str], executor: Executor) -> List[Task]:
    resultt.append(download_func())
    return []

def test2():
    print(f"> Test 2")
    urls = []
    results_lists = []
    n_tasks = 10
    for i in range(n_tasks):
        urls.append("http://192.168.2.184/")
        results_lists.append([])
    
    executor = Executor(1)

    tasks = []
    for i in range(n_tasks):
        task = Task(lambda executor: downloadTask(partial(downloadUrl, urls[i], executor), results_lists[i], executor))
        tasks.append(task)

    for i in range(n_tasks):
        executor.schedule(tasks[i])

    #finish_task = Task(lambda: print(f"{sum(len(result) for result in results_lists)}"), tasks)
    #finish_task = Task(lambda: print(f"{results_lists[0]}"), tasks)
    #executor.schedule(finish_task)
    executor.waitUntilTasksDone()

    for i in range(n_tasks):
        print(f"[{i}]: {len(results_lists[i])}")

    executor.join()

    pass

def test3():
    print(f"> Test 3")
    def getDeferredResult(url: str) -> AsyncResult:
        result = Result()
        def taskWork(executor: Executor):
            result.setValue(url + " done")
            return []
        return AsyncResult(result, Task(taskWork))
    defResult = getDeferredResult("Blah")
    executor = Executor()
    executor.schedule(defResult.task)
    
    def whenDone(executor: Executor):
        print(f"Result: {defResult.result.retrieveResult()}")
        return []
    
    executor.schedule(Task(whenDone, [defResult.task]))
    executor.waitUntilTasksDone()
    executor.join()

def nopPrint(numa, deps, executor: Executor):
    logging.debug(f"work {numa}")
    return deps

def test4():
    print(f"> Test 4")

    with Executor(n_threads=0, manual_execute=True) as executor:
        task = Task(partial(nopPrint, 0, []))
        setattr(task, "DEBUG_NAME", "task")
        executor.schedule(task)

        task2 = Task(partial(nopPrint, 1, [task]), [])
        setattr(task2, "DEBUG_NAME", "task2")
        executor.schedule(task2)
        executor.manualExecute()

        task3 = Task(partial(nopPrint, 2, []), [task2])
        setattr(task3, "DEBUG_NAME", "task3")
        executor.schedule(task3)
        executor.manualExecute()
        executor.manualExecute()
        
        # Nothing should be done
        executor.manualExecute()

        if not task.done:
            raise Exception("Task wasn't done")
        if not task2.done:
            raise Exception("Task wasn't done")
        if not task3.done:
            raise Exception("Task wasn't done")       

def test5():
    print(f"> Test 5")

    with Executor(n_threads=2) as executor:
        task = Task(partial(nopPrint, 0, []))
        setattr(task, "DEBUG_NAME", "task")
        executor.schedule(task)

        task2 = Task(partial(nopPrint, 1, [task]), [])
        setattr(task2, "DEBUG_NAME", "task2")
        executor.schedule(task2)

        task3 = Task(partial(nopPrint, 2, []), [task2])
        setattr(task3, "DEBUG_NAME", "task3")
        executor.schedule(task3)

        executor.waitUntilTasksDone()

        if not task.done:
            raise Exception("Task wasn't done")
        if not task2.done:
            raise Exception("Task wasn't done")
        if not task3.done:
            raise Exception("Task wasn't done")

        executor.join()

def checkDepDone(numa, deps, executor: Executor):
    logging.debug(f"work {numa}")
    for dep in deps:
        if not dep.done:
            raise Exception("Dependency not done.")
    return []

def test6():
    print(f"> Test 6")
    with Executor(n_threads=2) as executor:
        task = Task(partial(checkDepDone, 0, []))
        task2 = Task(partial(checkDepDone, 1, [task]), [task])
        task3 = Task(partial(checkDepDone, 1, [task, task2]), [task, task2])
        setattr(task3, "DEBUG_NAME", "task3")
        setattr(task2, "DEBUG_NAME", "task2")
        setattr(task, "DEBUG_NAME", "task")
        executor.schedule(task3)
        executor.schedule(task2)
        executor.schedule(task)

        executor.waitUntilTasksDone()

        if not task.done:
            raise Exception("Task wasn't done")
        if not task2.done:
            raise Exception("Task wasn't done")
        if not task3.done:
            raise Exception("Task wasn't done")

        executor.join()

test2()
test3()
test4()
test5()
test6()

print(f"> Finished in {time.time() - start} seconds")
