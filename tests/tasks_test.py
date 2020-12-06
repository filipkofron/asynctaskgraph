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

from typing import Callable, List
from functools import partial
import logging
import os
import requests
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from asynctaskgraph import AsyncResult, Result, Task, Executor

logging.root.setLevel(logging.DEBUG)
logging.basicConfig(format="%(threadName)s:\t%(message)s")
start = time.time()

def download_url(url: str, executor: Executor):
    with requests.get(url) as r:
        return r.text

def download_task(download_func: Callable[[],str], resultt: List[str], executor: Executor) -> List[Task]:
    resultt.append(download_func())
    return []

def test2():
    print(f"> Test 2")
    urls = []
    results_lists = []
    n_tasks = 10
    for i in range(n_tasks):
        urls.append("http://example.org/")
        results_lists.append([])
    
    executor = Executor(1)

    tasks = []
    for i in range(n_tasks):
        task = Task(lambda executor: download_task(partial(download_url, urls[i], executor), results_lists[i], executor))
        tasks.append(task)

    for i in range(n_tasks):
        executor.schedule(tasks[i])

    #finish_task = Task(lambda: print(f"{sum(len(result) for result in results_lists)}"), tasks)
    #finish_task = Task(lambda: print(f"{results_lists[0]}"), tasks)
    #executor.schedule(finish_task)
    executor.wait_until_tasks_done()

    for i in range(n_tasks):
        print(f"[{i}]: {len(results_lists[i])}")

    executor.join()

    pass

def test3():
    print(f"> Test 3")
    def getDeferredResult(url: str) -> AsyncResult:
        result = Result()
        def taskWork(executor: Executor):
            result.set_value(url + " done")
            return []
        return AsyncResult(result, Task(taskWork))
    defResult = getDeferredResult("Blah")
    executor = Executor()
    executor.schedule(defResult.task)
    
    def whenDone(executor: Executor):
        print(f"Result: {defResult.result.retrieve_result()}")
        return []
    
    executor.schedule(Task(whenDone, [defResult.task]))
    executor.wait_until_tasks_done()
    executor.join()

def nop_print(numa, deps, executor: Executor):
    logging.debug(f"work {numa}")
    return deps

def test4():
    print(f"> Test 4")

    with Executor(n_threads=0, manual_execution=True) as executor:
        task = Task(partial(nop_print, 0, []))
        setattr(task, "DEBUG_NAME", "task")
        executor.schedule(task)

        task2 = Task(partial(nop_print, 1, [task]), [])
        setattr(task2, "DEBUG_NAME", "task2")
        executor.schedule(task2)
        executor.manual_execute()

        task3 = Task(partial(nop_print, 2, []), [task2])
        setattr(task3, "DEBUG_NAME", "task3")
        executor.schedule(task3)
        executor.manual_execute()
        executor.manual_execute()
        
        # Nothing should be done
        executor.manual_execute()

        if not task.done:
            raise Exception("Task wasn't done")
        if not task2.done:
            raise Exception("Task wasn't done")
        if not task3.done:
            raise Exception("Task wasn't done")       

def test5():
    print(f"> Test 5")

    with Executor(n_threads=2) as executor:
        task = Task(partial(nop_print, 0, []))
        setattr(task, "DEBUG_NAME", "task")
        executor.schedule(task)

        task2 = Task(partial(nop_print, 1, [task]), [])
        setattr(task2, "DEBUG_NAME", "task2")
        executor.schedule(task2)

        task3 = Task(partial(nop_print, 2, []), [task2])
        setattr(task3, "DEBUG_NAME", "task3")
        executor.schedule(task3)

        executor.wait_until_tasks_done()

        if not task.done:
            raise Exception("Task wasn't done")
        if not task2.done:
            raise Exception("Task wasn't done")
        if not task3.done:
            raise Exception("Task wasn't done")

        executor.join()

def check_dep_done(numa, deps, executor: Executor):
    logging.debug(f"work {numa}")
    for dep in deps:
        if not dep.done:
            raise Exception("Dependency not done.")
    return []

def test6():
    print(f"> Test 6")
    with Executor(n_threads=2) as executor:
        task = Task(partial(check_dep_done, 0, []))
        task2 = Task(partial(check_dep_done, 1, [task]), [task])
        task3 = Task(partial(check_dep_done, 1, [task, task2]), [task, task2])
        setattr(task3, "DEBUG_NAME", "task3")
        setattr(task2, "DEBUG_NAME", "task2")
        setattr(task, "DEBUG_NAME", "task")
        executor.schedule(task3)
        executor.schedule(task2)
        executor.schedule(task)

        executor.wait_until_tasks_done()

        if not task.done:
            raise Exception("Task wasn't done")
        if not task2.done:
            raise Exception("Task wasn't done")
        if not task3.done:
            raise Exception("Task wasn't done")

        executor.join()

# TODO: Write proper tests without requiring web-server

test2()
test3()
test4()
test5()
test6()

print(f"> Finished in {time.time() - start} seconds")
