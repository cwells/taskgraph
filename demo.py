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


pipetask = TaskGraph()

class JobPipeline:
    def __init__(self):
        self.start_time = time.time()

    def do_work(self, data, delay):
        time.sleep(delay)
        return {
            'elapsed': round(time.time() - self.start_time, 1),
            'finished': list(data.keys())
        }

    @pipetask.requires()
    def foo(self, data, delay):
        return { "foo": self.do_work(data, delay) }

    @pipetask.requires()
    def bar(self, data, delay):
        return { "bar": self.do_work(data, delay) }

    @pipetask.requires(bar)
    def baz(self, data, delay):
        return { "baz": self.do_work(data, delay) }

    @pipetask.requires(foo, bar)
    def qux(self, data, delay):
        return { "qux": self.do_work(data, delay) }

    @pipetask.requires(qux, baz)
    def quz(self, data, delay):
        return { "quz": self.do_work(data, delay) }

    @pipetask.requires(foo, bar)
    def xyzzy(self, data, delay):
        return { "xyzzy": self.do_work(data, delay) }


@click.group()
def cli():
    pass


@cli.command()
@click.option('--parallel', '-p', is_flag=True, help="Run tasks in parallel.")
@click.option('--pool-size', '-z', type=click.IntRange(1, 8), default=4, help="Size of pool for parallel processing.")
@click.option('--delay', '-d', type=click.IntRange(0, 10), default=0, help="Seconds to sleep in functions.")
@click.option('--graph', '-g', is_flag=True, help="Dump graph to stdout and exit (affected by -p flag).")
def runjob(parallel, pool_size, delay, graph):
    if graph:
        print(task.graph(parallel))
        raise SystemExit

    job = Job()
    run = partial(task.run_parallel, pool_size=pool_size) if parallel else task.run
    record = dict(ChainMap(*run(job, delay=delay)))

    print(json.dumps(record, indent=2))


@cli.command()
@click.option('--parallel', '-p', is_flag=True, help="Run tasks in parallel.")
@click.option('--pool-size', '-z', type=click.IntRange(1, 8), default=4, help="Size of pool for parallel processing.")
@click.option('--delay', '-d', type=click.IntRange(0, 10), default=0, help="Seconds to sleep in functions.")
@click.option('--graph', '-g', is_flag=True, help="Dump graph to stdout and exit (affected by -p flag).")
def runpipeline(parallel, pool_size, delay, graph):
    if graph:
        print(pipetask.graph(parallel))
        raise SystemExit

    pipeline = JobPipeline()
    run = partial(pipetask.pipe_parallel, pool_size=pool_size) if parallel else pipetask.pipe
    record = run(pipeline, delay=delay)

    print(json.dumps(record, indent=2))


if __name__ == '__main__':
    cli()
