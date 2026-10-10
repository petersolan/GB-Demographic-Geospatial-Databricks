"""Entry point for the integration-test job: run pytest on Databricks, fail the task on failure.

Serverless runs a python_file task by exec()-ing its source in a notebook kernel, so __file__
is not defined there: the job passes the tests folder as the first parameter instead.
/Workspace files can't hold __pycache__ folders, so no bytecode is written (pytest still
rewrites asserts for detailed failure messages, just in memory).
In that kernel even sys.exit(0) surfaces as an error, so the runner raises only on failure.
Further parameters of the form NAME=value (the pipeline's schemas) are passed to the tests as
environment variables.
"""
import os
import sys

import pytest

sys.dont_write_bytecode = True
here = sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(os.path.abspath(__file__))
for arg in sys.argv[2:]:
    name, _, value = arg.partition("=")
    os.environ[name] = value
code = pytest.main(["-q", "-p", "no:cacheprovider", "--rootdir", here, here])
if code != 0:
    raise RuntimeError(f"integration tests failed (pytest exit code {int(code)})")
