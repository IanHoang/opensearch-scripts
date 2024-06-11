### OSB Scripts SOP
#### Prerequisites
1. Setup a venv. Only need to do this once
```
python3 -m venv .venv
```
2. Activate venv
```
source .venv/bin/activate
```
3. Run this to install requirements
```
python3 -m pip install -r requirements.txt
```

#### Get Averages for All Runs
Using `average-test-execution-results.py` now instead of `average-run-results.py`.
1. SCP the test_execution.json files over to local host where scripts are. Create a folder and include all the test execution files. Renaming them to describe which one refers to each run would help.
2. Run `python3 average-test-execution-results.py -f <folder where test execution files from all runs are> -i <id of output file>
    - Can optionally include `-l` to include latency values instead of service time. Useful if comparing service time and latency values and determining if they are identical when target-throughput is set to None.
3. It will produce a test execution json file with average values from all runs. Provide that to the next script `python3 convert-results-to-csv.py -f <input average json file>`
4. This will produce a CSV that you can paste into quip

Note: Arithmetic means should be calcualted for the same types. Geometric means should be calculated when combining values from different types.
