#!/bin/env python

from copy import deepcopy
from multiprocessing import Pool

from toposort import toposort, toposort_flatten


class TaskGraph:
    '''Task dependency tree.
    Tasks may be run serially or in parallel.
    Declare tasks using @TaskGraph.requires().
    '''
    def __init__(self):
        self._graph = {}

    def __str__(self):
        '''Return formatted graph representation as string.
        '''
        sep = "\n- "
        return sep + sep.join(', '.join(s) for s in toposort(self._graph))

    def __call__(self, require=None):
        '''Shortcut decorator for tasks with no dependencies.
        '''
        def wrapper(fn):
            self._graph[fn.__name__] = set()
            return fn
        return wrapper

    def requires(self, *deps):
        '''Decorator for declaring a function as a task as well as listing
        other tasks as dependencies.
        '''
        def wrapper(fn):
            self._graph[fn.__name__] = set(f.__name__ for f in deps)
            return fn
        return wrapper

    def run(self, obj, data, *args, **kwargs):
        '''Run tasks serially, and pass the return value from each task
        as the argument to the next task.
        '''
        for task in toposort_flatten(self._graph):
            try:
                data = getattr(obj, task)(deepcopy(data), *args, **kwargs)
            except TaskAborted:
                return None

        return data

    def run_parallel(self, obj, data, *args, pool_size=4, **kwargs):
        '''Run independent tasks in parallel, and pass the merged return values
        from each task group to the next task group.
        '''
        for tasks in toposort(self._graph):
            with Pool(processes=pool_size) as pool:
                results = []
                for task in tasks:
                    results.append(pool.apply_async(getattr(obj, task), deepcopy(data), args, kwargs))
                for result in results:
                    data.update(result.get())
        return data

class TaskAborted(Exception):
    pass
