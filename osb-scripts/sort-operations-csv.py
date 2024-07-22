import csv
import argparse
import pandas as pd

# Define the desired order of operation names
desired_order = ['default', 'scroll', 'query-string-on-message', 'query-string-on-message-filtered', 'query-string-on-message-filtered-sorted-num', 'term', 'desc_sort_timestamp', 'asc_sort_timestamp', 'desc_sort_with_after_timestamp', 'asc_sort_with_after_timestamp', 'desc_sort_timestamp_can_match_shortcut', 'desc_sort_timestamp_no_can_match_shortcut', 'asc_sort_timestamp_can_match_shortcut', 'asc_sort_timestamp_no_can_match_shortcut', 'sort_keyword_can_match_shortcut', 'sort_keyword_no_can_match_shortcut', 'sort_numeric_desc', 'sort_numeric_asc', 'sort_numeric_desc_with_match', 'sort_numeric_asc_with_match', 'multi_terms-keyword', 'keyword-terms', 'keyword-terms-low-cardinality', 'composite-terms', 'composite_terms-keyword', 'range', 'range-numeric', 'keyword-in-range', 'range_field_conjunction_big_range_big_term_query', 'range_field_disjunction_big_range_small_term_query', 'range_field_conjunction_small_range_small_term_query', 'range_field_conjunction_small_range_big_term_query', 'date_histogram_hourly_agg', 'date_histogram_minute_agg', 'composite-date_histogram-daily', 'range-auto-date-histo', 'range-auto-date-histo-with-metrics']

def sort_and_create_new_table(filename, output):
    df = pd.read_csv(filename)

    df['operation'] = pd.Categorical(df['operation'], categories=desired_order, ordered=True)
    df = df.sort_values(['operation', 'distribution_version'])

    pivoted = df.pivot_table(index='operation', observed=False, columns='distribution_version', values='p90_value', aggfunc='mean')

    pivoted = pivoted.fillna(0)

    pivoted.to_csv(f"{output}.csv")

    print("Finished sorting operations and creating new CSV")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Sort CSV OSB Test Executions from Quicksight", description="A tool to sort and convert OSB CSV results exported from Quicksight")
    parser.add_argument("--file", "-f", required=True, help="CSV file")
    parser.add_argument("--output", "-o", required=True, help="Output CSV filename")
    args = parser.parse_args()
    results_file = args.file
    output_file = args.output
    sort_and_create_new_table(results_file, output_file)