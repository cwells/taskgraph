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
  "foo": {
    "elapsed": 1.0,
    "finished": []
  },
  "bar": {
    "elapsed": 2.0,
    "finished": [
      "foo"
    ]
  },
  "baz": {
    "elapsed": 3.0,
    "finished": [
      "foo",
      "bar"
    ]
  },
  "qux": {
    "elapsed": 4.0,
    "finished": [
      "foo",
      "bar",
      "baz"
    ]
  },
  "xyzzy": {
    "elapsed": 5.0,
    "finished": [
      "foo",
      "bar",
      "baz",
      "qux"
    ]
  },
  "quz": {
    "elapsed": 6.0,
    "finished": [
      "foo",
      "bar",
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
  "foo": {
    "elapsed": 1.0,
    "finished": []
  },
  "bar": {
    "elapsed": 2.1,
    "finished": [
      "foo"
    ]
  },
  "baz": {
    "elapsed": 3.1,
    "finished": [
      "foo",
      "bar"
    ]
  },
  "xyzzy": {
    "elapsed": 3.1,
    "finished": [
      "foo",
      "bar"
    ]
  },
  "qux": {
    "elapsed": 3.1,
    "finished": [
      "foo",
      "bar"
    ]
  },
  "quz": {
    "elapsed": 4.1,
    "finished": [
      "foo",
      "bar",
      "baz",
      "xyzzy",
      "qux"
    ]
  }
}
```
