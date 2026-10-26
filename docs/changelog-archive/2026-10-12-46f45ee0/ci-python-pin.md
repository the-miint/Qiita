### Fixed

- **CI pins the Python interpreter to 3.14.** `requires-python` is open-ended
  (`>=3.14`), so once CPython 3.15.0 was published `uv` began downloading it in
  CI and `pydantic-core`'s `pyo3` build failed against a Python newer than pyo3
  supports. Each `astral-sh/setup-uv` step now passes `python-version: "3.14"`.
