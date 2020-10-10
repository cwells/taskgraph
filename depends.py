#!/bin/env python

from multiprocessing import Pool
from time import sleep

import click
from toposort import toposort, toposort_flatten


class TaskGraph:
    '''Task dependency tree.
       Tasks may be run serially or in parallel.
       Declare tasks using @TaskGraph.requires().
    '''
    def __init__(self, pool_size=4):
        self._graph = {}
        self._tasks = {}
        self.pool_size = pool_size

    def requires(self, *deps):
        '''decorator for declaring a function as a task as well as
           listing other tasks as dependencies.
        '''
        def wrapper(fn):
            self._graph[fn.__name__] = set(f.__name__ for f in deps or [])
            self._tasks[fn.__name__] = fn
            return fn
        return wrapper

    def run(self, *args, **kwargs):
        '''Run tasks serially. Useful when:
            - tasks utilize shared mutable state
            - tasks are too small to be worth running in parallel
        '''
        for task in toposort_flatten(self._graph):
            yield self._tasks[task](*args, **kwargs)

    def run_parallel(self, *args, **kwargs):
        '''Run independent tasks in parallel.
           For example, given dependencies a -> b and c -> d,
           (a, c) would be run in parallel, followed by (b, d)
           being run in parallel.
           Tasks should not share state unless appropriate guardrails
           are in place.
        '''
        for tasks in toposort(self._graph):
            with Pool(processes=self.pool_size) as pool:
                results = []
                for task in tasks:
                    results.append(pool.apply_async(self._tasks[task], args, kwargs))
                for result in results:
                    yield result.get()


#
# This has to be top-level due to the way Processing sends jobs to
# worker processes (it can only pickle top-level objects). If this
# is an issue, replace Processing with some other parallel-processing
# library such as Threading.
#
task = TaskGraph()

class Job:
    @task.requires()
    def foo(self, delay):
        sleep(delay)
        return "foo"

    @task.requires()
    def bar(self, delay):
        sleep(delay)
        return "bar"

    @task.requires(bar)
    def baz(self, delay):
        sleep(delay)
        return "baz"

    @task.requires(foo, bar, baz)
    def qux(self, delay):
        sleep(delay)
        return "qux"

    @task.requires(qux)
    def quz(self, delay):
        sleep(delay)
        return "quz"

    @task.requires(baz)
    def xyzzy(self, delay):
        sleep(delay)
        return "xyzzy"


@click.command()
@click.option('--parallel', '-p', is_flag=True, help="Run tasks in parallel.")
@click.option('--pool-size', '-z', type=click.IntRange(1, 8), default=4, help="Size of pool for parallel processing.")
@click.option('--delay', '-d', type=click.IntRange(1, 10), default=0, help="Seconds to sleep in functions.")
def main(parallel, pool_size, delay):
    job = Job()
    task.pool_size = pool_size
    run = task.run_parallel if parallel else task.run

    for result in run(job, delay=delay):
        print(f"finished {result}")


if __name__ == '__main__':
    main()