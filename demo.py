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
    def foo(self, data, delay, *args, **kwargs):
        data.update({ "foo": self.do_work(data, delay) })
        return data

    @task.requires(foo)
    def bar(self, data, delay, *args, **kwargs):
        data.update({ "bar": self.do_work(data, delay) })
        return data

    @task.requires(bar)
    def baz(self, data, delay, *args, **kwargs):
        data.update({ "baz": self.do_work(data, delay) })
        return data

    @task.requires(foo, bar)
    def qux(self, data, delay, *args, **kwargs):
        data.update({ "qux": self.do_work(data, delay) })
        return data

    @task.requires(qux, baz)
    def quz(self, data, delay, *args, **kwargs):
        data.update({ "quz": self.do_work(data, delay) })
        return data

    @task.requires(foo, bar)
    def xyzzy(self, data, delay, *args, **kwargs):
        data.update({ "xyzzy": self.do_work(data, delay) })
        return data

    def run(self, data, delay):
        return self.task.run(self, data, delay=delay)

    def run_parallel(self, data, delay):
        return self.task.run_parallel(self, data, delay=delay)


@click.group()
def cli():
    pass

@cli.command()
@click.option('--parallel', '-p', is_flag=True, help="Run tasks in parallel.")
@click.option('--delay', '-d', type=click.IntRange(0, 10), default=0, help="Seconds to sleep in functions.")
@click.option('--graph', '-g', is_flag=True, help="Dump graph to stdout and exit (affected by -p flag).")
def run(parallel, delay, graph):
    if graph:
        print(Job.task)
        raise SystemExit

    job = Job()
    run = job.run_parallel if parallel else job.run
    record = run({}, delay=delay)
    print(json.dumps(record, indent=2))

if __name__ == '__main__':
    cli()
