#!/bin/env python

import json
import time
from collections import ChainMap
from functools import partial

import click

from taskgraph import TaskGraph

class Job:
    task = TaskGraph()

    def __init__(self):
        self.start_time = time.time()

    def do_work(self, data, delay):
        time.sleep(delay)
        return {
            'elapsed': round(time.time() - self.start_time, 1),
            'finished': list(data.keys())
        }

    @task.requires()
    def foo(self, data, delay):
        data.update({ "foo": self.do_work(data, delay) })
        return data

    @task.requires()
    def bar(self, data, delay):
        data.update({ "bar": self.do_work(data, delay) })
        return data

    @task.requires(bar)
    def baz(self, data, delay):
        data.update({ "baz": self.do_work(data, delay) })
        return data

    @task.requires(foo, bar)
    def qux(self, data, delay):
        data.update({ "qux": self.do_work(data, delay) })
        return data

    @task.requires(qux, baz)
    def quz(self, data, delay):
        data.update({ "quz": self.do_work(data, delay) })
        return data

    @task.requires(foo, bar)
    def xyzzy(self, data, delay):
        data.update({ "xyzzy": self.do_work(data, delay) })
        return data

    def run(self, data, delay):
        return self.task.run(self, data, delay=delay)


@click.group()
def cli():
    pass

@cli.command()
@click.option('--parallel', '-p', is_flag=True, help="Run tasks in parallel.")
@click.option('--pool-size', '-z', type=click.IntRange(1, 8), default=4, help="Size of pool for parallel processing.")
@click.option('--delay', '-d', type=click.IntRange(0, 10), default=0, help="Seconds to sleep in functions.")
@click.option('--graph', '-g', is_flag=True, help="Dump graph to stdout and exit (affected by -p flag).")
def run(parallel, pool_size, delay, graph):
    if graph:
        print(Job.task)
        raise SystemExit

    job = Job()
    record = job.run({}, delay=delay)
    print(json.dumps(record, indent=2))

if __name__ == '__main__':
    cli()
