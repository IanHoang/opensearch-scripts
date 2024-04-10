import os
import pandas as pd
from tabulate import tabulate
from colorama import Fore, Style
import json
import argparse
import numpy as np
import statistics

"""
1. Get folder path
2. Get files in folder path
3. Get data from each folder path
4. Go through each file and update the dictionary. Then show distribution of values for tasks.
5. Perform mean
6. Export CSV
"""

def main(folder_path:str, id: str, use_latency: bool):
    file_paths = get_test_execution_file_paths(folder_path)

    all_runs_names = []
    all_runs = []
    for file_name in file_paths:
        result_dict = get_data(file_name)
        all_runs_names.append(result_dict["test-execution-id"])
        result_dict_specific = result_dict["results"]["op_metrics"]

        all_runs.append(result_dict_specific)

    # print(all_runs_names)
    # print(all_runs)

    if all_runs_have_same_number_of_tasks(all_runs, all_runs_names):
        print(f"Averaging p50 and p90 results from these run: {all_runs_names}")
        averaged_results = compute_average_across_all_runs(all_runs, id, use_latency)

        output_json_name = f"{id}.json"
        write_to_file(averaged_results, output_json_name)


def compute_average_across_all_runs(all_test_runs: list, id: str, use_latency: bool):
    exclude_tasks = ["force-merge", "wait-until-merges-finish", "wait-until-merges-1-seg-finish"]
    averaged_results = {
        "test-execution-id": f"average-{id}",
        "results": {
            "op_metrics": []
        }
    }

    # Compile metrics for all test runs
    task_metrics_for_all_runs = get_metrics_for_all_test_runs(all_test_runs, use_latency)
    # print(task_metrics_for_all_runs)
    # Calculate averages
    for task, metrics in task_metrics_for_all_runs.items():
        if task in exclude_tasks:
            continue

        task_averages_and_stdev = compute_avg_and_stdev_for_task(task, metrics, use_latency)

        # Add final averages for a task into dictionary
        avg_task_metrics = {}
        if use_latency:
            avg_task_metrics = {
                "task": task,
                "latency": task_averages_and_stdev["latency"],
                "error_rate": task_averages_and_stdev["error_rate"],
                "rsd": task_averages_and_stdev["relative_standard_deviation"]
            }
        else:
            avg_task_metrics = {
                "task": task,
                "service_time": task_averages_and_stdev["service_time"],
                "error_rate": task_averages_and_stdev["error_rate"],
                "rsd": task_averages_and_stdev["relative_standard_deviation"]
            }

        # Append to test_execution.json file
        averaged_results["results"]["op_metrics"].append(avg_task_metrics)

    print("Finished calculating averages for all tasks")
    return averaged_results

def get_metrics_for_all_test_runs(all_test_runs: list, use_latency: bool):
    metrics_from_all_runs = {}
    all_tasks = len(all_test_runs[0])

    # Initialize dictionary containing all metrics. Only grab the task names from the first test run because all of them have the same amount of tasks.
    for i in range(all_tasks):
        task_name = all_test_runs[0][i]["task"]
        metrics_from_all_runs[task_name] = {
            "metric_50": [],
            "metric_90": [],
            "error_rates": []
        }

    # Each test run is a dictionary, and each task is a dictionary, which contains keys that are metric names and values that are lists
    for test_run in all_test_runs:
        for i in range(all_tasks):
            # A list of tasks from all three runs
            task_from_test_run = test_run[i]
            task_name = task_from_test_run["task"]

            task_latency_50, task_latency_90, task_error_rate = get_metrics_for_task(task_from_test_run, use_latency)

            # append metrics to task name latencies and error rates
            metrics_from_all_runs[task_name]["metric_50"].append(task_latency_50)
            metrics_from_all_runs[task_name]["metric_90"].append(task_latency_90)
            metrics_from_all_runs[task_name]["error_rates"].append(task_error_rate)

    return metrics_from_all_runs

def get_metrics_for_task(test_run_task: dict, use_latency: bool):
    metric_name = "service_time"
    if use_latency:
        metric_name = "latency"

    task_latency_50 = test_run_task[metric_name].get("50_0", 0)
    task_latency_90 = test_run_task[metric_name].get("90_0", 0)
    task_error_rate = test_run_task.get("error_rate", 0)

    return task_latency_50, task_latency_90, task_error_rate

def compute_avg_and_stdev_for_task(task: str, task_metrics: dict, use_latency: bool):
    cumulative_metric_50 = task_metrics["metric_50"]
    cumulative_metric_90 = task_metrics["metric_90"]
    cumulative_error_rates = task_metrics["error_rates"]

    average_metric_50, average_metric_90, average_error_rates = compute_averages(cumulative_metric_50, cumulative_metric_90, cumulative_error_rates)
    stdev_metric_50, stdev_metric_90 = compute_stdev(cumulative_metric_50, cumulative_metric_90)

    # Calculate RSD
    rsd_metric_50 = np.round((stdev_metric_50 / average_metric_50) * 100, decimals=2)
    rsd_metric_90 = np.round((stdev_metric_90 / average_metric_90) * 100, decimals=2)

    # Round before publishing data. Want to use original numbers for calculations of RSD. If do not care for this, uncomment the round numbers portion in compute_averages() and compute_stdev() funtions and comment this out.
    average_metric_50 = np.round(average_metric_50, decimals=2)
    average_metric_90 = np.round(average_metric_90, decimals=2)
    average_error_rates = np.round(average_error_rates, decimals=2)

    metric_name = "service_time"
    if use_latency:
        metric_name = "latency"

    print("Task: ", task)
    print(f"Cumulative {metric_name} p50 {cumulative_metric_50}, p90 {cumulative_metric_90}")
    print("Means: ", average_metric_50, average_metric_90)
    print("Stdev: ", stdev_metric_50, stdev_metric_90)
    print("RSD: ", rsd_metric_50, rsd_metric_90)
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------")

    average_metrics_for_task = {
        "service_time": {
            "50_0": average_metric_50,
            "90_0": average_metric_90
        },
        "relative_standard_deviation": {
            "50_0": rsd_metric_50,
            "90_0": rsd_metric_90
        },
        "error_rate": average_error_rates
    }

    if use_latency:
        average_metrics_for_task = {
            "latency": {
                "50_0": average_metric_50,
                "90_0": average_metric_90
            },
            "relative_standard_deviation": {
                "50_0": rsd_metric_50,
                "90_0": rsd_metric_90
            },
            "error_rate": average_error_rates
        }

    return average_metrics_for_task

def compute_averages(cumulative_metric_50, cumulative_metric_90, cumulative_error_rates):
    average_task_metric_50 = statistics.mean(cumulative_metric_50)
    average_task_metric_90 = statistics.mean(cumulative_metric_90)
    average_task_error_rates = statistics.mean(cumulative_error_rates)

    # Round if do not care about calculating RSD with original numbers
    # average_task_metric_50 = np.round(average_task_metric_50, decimals=2)
    # average_task_metric_90 = np.round(average_task_metric_90, decimals=2)
    # average_task_error_rates = np.round(average_task_error_rates, decimals=2)

    return average_task_metric_50, average_task_metric_90, average_task_error_rates

def compute_stdev(cumulative_metric_50, cumulative_metric_90):
    stdev_task_metric_50 = statistics.stdev(cumulative_metric_50)
    stdev_task_metric_90 = statistics.stdev(cumulative_metric_90)

    # Round if do not care about calculating RSD with original numbers
    # stdev_task_metric_50 = np.round(stdev_task_metric_50, decimals=2)
    # stdev_task_metric_90 = np.round(stdev_task_metric_90, decimals=2)

    return stdev_task_metric_50, stdev_task_metric_90

def get_test_execution_file_paths(folder_path: str):
    test_execution_files = []
    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path) and "avg" not in filename and ".json" in filename:
                test_execution_files.append(file_path)
    else:
        print("Folder path does not exist: ", folder_path)

    try:
        test_execution_files = sorted(test_execution_files, key=(lambda x: int(''.join(filter(str.isdigit, x)))))
    except:
        raise Exception("Files need to have the run number associated with it. Check folder directory to see if there are any file names without their associated run number or iteration. For example, to represent run 2 of a specific run, use test-execution-2.json instead of test-execution.json")

    return test_execution_files

def all_runs_have_same_number_of_tasks(all_runs: list, all_runs_names: list):
    # Check if all runs have the same number of operations or tasks
    if len(all_runs) == 1 and len(all_runs_names) == 1:
        raise Exception("Only 1 file is present in the directory provided. No averages need to be calculated.")
    else:
        number_of_tasks_from_all_runs = []
        for run in all_runs:
            number_of_tasks_from_all_runs.append(len(run))

        all_same = all(x == number_of_tasks_from_all_runs[0] for x in number_of_tasks_from_all_runs)
        if not all_same:
            raise Exception("Not all runs have same number of tasks. Verify that all runs have same set of tasks and number of tasks.")
        else:
            return True

def get_data(result_file_path: str) -> dict:
    with open(result_file_path) as file:
        data = json.load(file)

    return data

def write_to_file(avg_task_metrics: dict, file_name: str):
    formatted_avg_task_metrics = json.dumps(avg_task_metrics, indent=4)

    with open(file_name, "w") as output_json:
        print(formatted_avg_task_metrics, file=output_json)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Average OSB Test Execution Results", description="A tool that takes test execution results, averages metrics across each task, and outputs them to a json file.")
    parser.add_argument("--folder-path", "-f", required=True, help="Path to folder containing test execution files")
    parser.add_argument("--id", "-i", required=True, help="ID for final output. Will be used as outputted json name")
    parser.add_argument("--use-latency", "-l", action="store_true", help="Use latency instead of service time. By default, script computes averages for service time.")
    args = parser.parse_args()
    folder_path = args.folder_path
    test_execution_id = args.id
    use_latency = args.use_latency

    main(folder_path, test_execution_id, use_latency)


