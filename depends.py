#!/bin/env python

from multiprocessing import Pool
from time import sleep

import click
from toposort import toposort, toposort_flatten


class TaskGraph:
    '''directed acyclic graph representing dependency tree. tasks
       may be run serially or in parallel.
    '''
    def __init__(self, pool_size=4):
        self._graph = {}
        self._tasks = {}
        self.pool_size = pool_size

    def requires(self, *deps):
        def wrapper(fn):
            self._graph[fn.__name__] = set(f.__name__ for f in deps or [])
            self._tasks[fn.__name__] = fn
            return fn
        return wrapper

    def sort(self):
        return toposort_flatten(self._graph)

    def run(self, *args):
        for task in self.sort():
            yield self._tasks[task](*args)

    def run_parallel(self, *args):
        for tasks in toposort(self._graph):
            with Pool(processes=self.pool_size) as pool:
                results = []
                for task in tasks:
                    results.append(pool.apply_async(self._tasks[task], args))
                for result in results:
                    yield result.get()


task = TaskGraph()

class Job:
    @task.requires()
    def foo(self):
        sleep(2)
        return "foo"

    @task.requires()
    def bar(self):
        sleep(2)
        return "bar"

    @task.requires(bar)
    def baz(self):
        sleep(2)
        return "baz"

    @task.requires(foo, bar, baz)
    def qux(self):
        sleep(2)
        return "qux"

    @task.requires(qux)
    def quz(self):
        sleep(2)
        return "quz"

    @task.requires(baz)
    def xyzzy(self):
        sleep(2)
        return "xyzzy"


@click.command()
@click.option('--parallel', '-p', is_flag=True, help="Run tasks in parallel.")
@click.option('--pool-size', '-z', type=click.IntRange(1, 8), help="Size of pool for parallel processing.")
def main(parallel, pool_size):
    job = Job()
    task.pool_size = pool_size
    run = task.run_parallel if parallel else task.run

    for result in run(job):
        print(f"finished {result}")


if __name__ == '__main__':
    main()