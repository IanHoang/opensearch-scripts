import pandas as pd
import json
import argparse

def main(results):
    with open(results) as file:
        data = json.load(file)

    formatted_data = pd.json_normalize(data["results"]["op_metrics"])

    formatted_data.to_csv(f"{results}-output.csv", index=False)
    print("Finished converting json to csv.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Convert OSB Test Executions to CSV", description="A tool to parse and convert JSON result files from OSB to CSV")
    parser.add_argument("--file", "-f", required=True, help="File from ~/.benchmark/benchmarks/test_executions/")
    args = parser.parse_args()
    results_file = args.file
    main(results_file)

