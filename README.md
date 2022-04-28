Reproduce:

```
nix run nixpkgs#python3 -- -m venv .venv --copies
pip install -r requirements.txt
python -m test.test_file_based_datasource
```

An error appears:

```
2022-04-28 15:46:20,830 WARNING worker.py:523 -- `ray.get_gpu_ids()` will always return the empty list when called from the driver. This is because Ray does not manage GPU allocations to the driver process.
:task_name:_prepare_read
:task_name:_prepare_read
[2022-04-28 15:46:20,887 E 1296042 1296042] core_worker.cc:1382: Pushed Error with JobID: 01000000 of type: task with message: ray::_prepare_read() (pid=1296042, ip=192.168.1.40)
TypeError: object of type 'int' has no len()

During handling of the above exception, another exception occurred:

ray::_prepare_read() (pid=1296042, ip=192.168.1.40)
  File "/tmp/ray-issue-24296/.venv/lib/python3.9/site-packages/ray/data/read_api.py", line 902, in _prepare_read
    return ds.prepare_read(parallelism, **kwargs)
  File "/tmp/ray-issue-24296/.venv/lib/python3.9/site-packages/ray/data/datasource/file_based_datasource.py", line 326, in prepare_read
    np.array_split(paths, parallelism), np.array_split(file_sizes, parallelism)
  File "<__array_function__ internals>", line 180, in array_split
  File "/tmp/ray-issue-24296/.venv/lib/python3.9/site-packages/numpy/lib/shape_base.py", line 782, in array_split
    (Nsections-extras) * [Neach_section])
MemoryError at time: 1.65115e+09
Traceback (most recent call last):
  File "/nix/store/hb1lzaisgx2m9n29hqhh6yp6hasplq1v-python3-3.9.10/lib/python3.9/runpy.py", line 197, in _run_module_as_main
    return _run_code(code, main_globals, None,
  File "/nix/store/hb1lzaisgx2m9n29hqhh6yp6hasplq1v-python3-3.9.10/lib/python3.9/runpy.py", line 87, in _run_code
    exec(code, run_globals)
  File "/tmp/ray-issue-24296/test/test_file_based_datasource.py", line 53, in <module>
    main()
  File "/tmp/ray-issue-24296/test/test_file_based_datasource.py", line 40, in main
    ds = ray.data.read_datasource(
  File "/tmp/ray-issue-24296/.venv/lib/python3.9/site-packages/ray/data/read_api.py", line 231, in read_datasource
    read_tasks = ray.get(
  File "/tmp/ray-issue-24296/.venv/lib/python3.9/site-packages/ray/_private/client_mode_hook.py", line 105, in wrapper
    return func(*args, **kwargs)
  File "/tmp/ray-issue-24296/.venv/lib/python3.9/site-packages/ray/worker.py", line 1809, in get
    raise value.as_instanceof_cause()
ray.exceptions.RayTaskError(MemoryError): ray::_prepare_read() (pid=1296042, ip=192.168.1.40)
TypeError: object of type 'int' has no len()

During handling of the above exception, another exception occurred:

ray::_prepare_read() (pid=1296042, ip=192.168.1.40)
  File "/tmp/ray-issue-24296/.venv/lib/python3.9/site-packages/ray/data/read_api.py", line 902, in _prepare_read
    return ds.prepare_read(parallelism, **kwargs)
  File "/tmp/ray-issue-24296/.venv/lib/python3.9/site-packages/ray/data/datasource/file_based_datasource.py", line 326, in prepare_read
    np.array_split(paths, parallelism), np.array_split(file_sizes, parallelism)
  File "<__array_function__ internals>", line 180, in array_split
  File "/tmp/ray-issue-24296/.venv/lib/python3.9/site-packages/numpy/lib/shape_base.py", line 782, in array_split
    (Nsections-extras) * [Neach_section])
MemoryError
```
