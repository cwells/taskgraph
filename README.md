Running the code
----------------
Print the graph:
```
$ python demo.py run -g

	(bar, foo) -> (xyzzy, baz, qux) -> (quz)

```

Serial execution:
```json
$ python demo.py -d 1
{
  "bar": {
    "elapsed": 1.0,
    "finished": []
  },
  "foo": {
    "elapsed": 2.0,
    "finished": [
      "bar"
    ]
  },
  "baz": {
    "elapsed": 3.0,
    "finished": [
      "bar",
      "foo"
    ]
  },
  "qux": {
    "elapsed": 4.0,
    "finished": [
      "bar",
      "foo",
      "baz"
    ]
  },
  "xyzzy": {
    "elapsed": 5.0,
    "finished": [
      "bar",
      "foo",
      "baz",
      "qux"
    ]
  },
  "quz": {
    "elapsed": 6.0,
    "finished": [
      "bar",
      "foo",
      "baz",
      "qux",
      "xyzzy"
    ]
  }
}

```

Parallel execution:
```json
$ python demo.py -d 1 -p
{
  "bar": {
    "elapsed": 1.0,
    "finished": []
  },
  "foo": {
    "elapsed": 2.0,
    "finished": [
      "bar"
    ]
  },
  "baz": {
    "elapsed": 3.0,
    "finished": [
      "bar",
      "foo"
    ]
  },
  "qux": {
    "elapsed": 4.0,
    "finished": [
      "bar",
      "foo",
      "baz"
    ]
  },
  "xyzzy": {
    "elapsed": 5.0,
    "finished": [
      "bar",
      "foo",
      "baz",
      "qux"
    ]
  },
  "quz": {
    "elapsed": 6.0,
    "finished": [
      "bar",
      "foo",
      "baz",
      "qux",
      "xyzzy"
    ]
  }
}
```
