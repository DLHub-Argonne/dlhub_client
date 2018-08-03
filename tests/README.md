*NOTE: These tests are still in development, and should not be expected to properly cover all test cases yet.*
## DLHub Tests
This directory contains the tests for the `dlhub_client` package.
The tests cover the `DLHub` class.

### Running the tests
Python 3 must be installed. Go to https://www.python.org/downloads/ to download Python 3.
Both Pytest and globus_sdk must also be installed. To do this, run `pip install pytest` and `pip install globus_sdk`
After Pytest is installed, the tests can be executed by running `pytest` in this directory.

### About the tests
These tests cover the basic and advanced functionality of the `dlhub_client` package. They test each function to check that operations succeed with expected values, error with invalid values, and respect parameters appropriately.
However, the tests currently do not cover functionality in Globus Search or MDF Forge's Query; both are services that DLHub relies on. Search results are not verified. Additionally, errors from Search usually will, but are not guaranteed to, fail these tests.
