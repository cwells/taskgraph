Running the code
----------------
Serial execution:
```
$ python demo.py -g
bar -> foo -> baz -> qux -> xyzzy -> quz
```

```json
$ python demo.py -d 2
{
  "bar": 2.0,
  "foo": 4.0,
  "baz": 6.0,
  "qux": 8.0,
  "xyzzy": 10.0,
  "quz": 12.0
}
```
Parallel execution:
```
$ python demo.py -g -p
(bar, foo) -> (qux, xyzzy, baz) -> (quz)
```

```json
$ python demo.py -d 2 -p
{
  "bar": 2.0,
  "foo": 2.0,
  "qux": 4.0,
  "baz": 4.0,
  "xyzzy": 4.0,
  "quz": 6.0
}
```
