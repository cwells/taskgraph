#!/bin/env python

from multiprocessing import Pool

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

    def pipe(self, *args, **kwargs):
        '''Run tasks serially, and pass the merged return value from each task
        as the argument to the next task.
        '''
        data = {}
        for task in toposort_flatten(self._graph):
            data.update(self._tasks[task](*args, data=data, **kwargs))
        return data

    def pipe_parallel(self, *args, pool_size=4, **kwargs):
        '''Run independent tasks in parallel, and pass the merged return values
        from each task group to the next task group.
        '''
        data = {}
        kwargs['data'] = data

        for tasks in toposort(self._graph):
            with Pool(processes=pool_size) as pool:
                results = []
                for task in tasks:
                    results.append(pool.apply_async(self._tasks[task], args, kwargs))
                for result in results:
                    data.update(result.get())
        return data
