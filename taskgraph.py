#!/bin/env python

import json
import time
from functools import partial
from multiprocessing import Pool

import click
from toposort import toposort, toposort_flatten


class TaskGraph:
    '''Task dependency tree.
    Tasks may be run serially or in parallel.
    Declare tasks using @TaskGraph.requires().
    '''
    def __init__(self):
        self._graph = {}
        self._tasks = {}

    def requires(self, *deps):
        '''Decorator for declaring a function as a task as well as listing
        other tasks as dependencies.
        '''
        def wrapper(fn):
            self._graph[fn.__name__] = set(f.__name__ for f in deps or [])
            self._tasks[fn.__name__] = fn
            return fn
        return wrapper

    def graph(self, parallel=False):
        '''Return formatted graph representation as string.
        '''
        if parallel:
            return "(" + ") -> (".join(', '.join(s) for s in toposort(self._graph)) + ")"

        return ' -> '.join([ s for s in toposort_flatten(self._graph) ])

    def run(self, *args, **kwargs):
        '''Run tasks serially.
        Useful when:
            - tasks share mutable state
            - tasks are too small to be worth running in parallel
        '''
        for task in toposort_flatten(self._graph):
            yield self._tasks[task](*args, **kwargs)

    def run_parallel(self, *args, pool_size=4, **kwargs):
        '''Run independent tasks in parallel.
        For example, given dependencies a -> b and c -> d, (a, c) would be run
        in parallel, followed by (b, d) being run in parallel.
        '''
        for tasks in toposort(self._graph):
            with Pool(processes=pool_size) as pool:
                results = []
                for task in tasks:
                    results.append(pool.apply_async(self._tasks[task], args, kwargs))
                for result in results:
                    yield result.get()


# ====
# DEMO
# ====

#
# This has to be top-level due to the way Processing sends jobs to worker
# processes (it can only pickle top-level objects). If this is an issue,
# replace Processing with some other parallel-processing library such as
# Threading.
#
task = TaskGraph()

class Job:
    def __init__(self):
        self.start_time = time.time()

    def do_work(self, delay):
        time.sleep(delay)
        return round(time.time() - self.start_time, 1)

    @task.requires()
    def foo(self, delay):
        return { "foo": self.do_work(delay) }

    @task.requires()
    def bar(self, delay):
        return { "bar": self.do_work(delay) }

    @task.requires(bar)
    def baz(self, delay):
        return { "baz": self.do_work(delay) }

    @task.requires(foo, bar)
    def qux(self, delay):
        return { "qux": self.do_work(delay) }

    @task.requires(qux, baz)
    def quz(self, delay):
        return { "quz": self.do_work(delay) }

    @task.requires(foo, bar)
    def xyzzy(self, delay):
        return { "xyzzy": self.do_work(delay) }


@click.command()
@click.option('--parallel', '-p', is_flag=True, help="Run tasks in parallel.")
@click.option('--pool-size', '-z', type=click.IntRange(1, 8), default=4, help="Size of pool for parallel processing.")
@click.option('--delay', '-d', type=click.IntRange(0, 10), default=0, help="Seconds to time.sleep in functions.")
@click.option('--graph', '-g', is_flag=True, help="Dump graph to stdout and exit (affected by -p flag).")
def main(parallel, pool_size, delay, graph):
    job = Job()

    if graph:
        print(task.graph(parallel))
        raise SystemExit

    run = partial(task.run_parallel, pool_size=pool_size) if parallel else task.run
    record = {}

    for result in run(job, delay=delay):
        record.update(result)

    print(json.dumps(record, indent=2))

if __name__ == '__main__':
    main()
