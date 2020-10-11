#!/bin/env python

import json
import time
from collections import ChainMap
from functools import partial

import click

from taskgraph import TaskGraph


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
@click.option('--delay', '-d', type=click.IntRange(0, 10), default=0, help="Seconds to sleep in functions.")
@click.option('--graph', '-g', is_flag=True, help="Dump graph to stdout and exit (affected by -p flag).")
def main(parallel, pool_size, delay, graph):
    if graph:
        print(task.graph(parallel))
        raise SystemExit

    job = Job()
    run = partial(task.run_parallel, pool_size=pool_size) if parallel else task.run
    record = dict(ChainMap(*run(job, delay=delay)))

    print(json.dumps(record, indent=2))

if __name__ == '__main__':
    main()
