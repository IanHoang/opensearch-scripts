import csv
import argparse
import pandas as pd

# Define the desired order of operation names
# Original Desired Order
#desired_order = ['default', 'scroll', 'query-string-on-message', 'query-string-on-message-filtered', 'query-string-on-message-filtered-sorted-num', 'term', 'desc_sort_timestamp', 'asc_sort_timestamp', 'desc_sort_with_after_timestamp', 'asc_sort_with_after_timestamp', 'desc_sort_timestamp_can_match_shortcut', 'desc_sort_timestamp_no_can_match_shortcut', 'asc_sort_timestamp_can_match_shortcut', 'asc_sort_timestamp_no_can_match_shortcut', 'sort_keyword_can_match_shortcut', 'sort_keyword_no_can_match_shortcut', 'sort_numeric_desc', 'sort_numeric_asc', 'sort_numeric_desc_with_match', 'sort_numeric_asc_with_match', 'multi_terms-keyword', 'keyword-terms', 'keyword-terms-low-cardinality', 'composite-terms', 'composite_terms-keyword', 'range', 'range-numeric', 'keyword-in-range', 'range_field_conjunction_big_range_big_term_query', 'range_field_disjunction_big_range_small_term_query', 'range_field_conjunction_small_range_small_term_query', 'range_field_conjunction_small_range_big_term_query', 'date_histogram_hourly_agg', 'date_histogram_minute_agg', 'composite-date_histogram-daily', 'range-auto-date-histo', 'range-auto-date-histo-with-metrics']
# New Desired Order
text_querying = ['query-string-on-message','query-string-on-message-filtered','query-string-on-message-filtered-sorted-num','term']
term_aggs = ['cardinality-agg-high', 'cardinality-agg-low', 'composite_terms-keyword', 'composite-terms', 'keyword-terms',
          'keyword-terms-low-cardinality', 'multi_terms-keyword', 'keyword-terms-low-cardinality-min', 'keyword-terms-min',
          'keyword-terms-numeric-terms', 'numeric-terms-numeric-terms']
sorting = ['asc_sort_timestamp', 'asc_sort_timestamp_can_match_shortcut', 'asc_sort_timestamp_no_can_match_shortcut', 'asc_sort_with_after_timestamp', 'desc_sort_timestamp',
          'desc_sort_timestamp_can_match_shortcut', 'desc_sort_timestamp_no_can_match_shortcut', 'desc_sort_with_after_timestamp', 'sort_keyword_can_match_shortcut', 'sort_keyword_no_can_match_shortcut',
          'sort_numeric_asc', 'sort_numeric_asc_with_match', 'sort_numeric_desc', 'sort_numeric_desc_with_match']
range_queries = ['keyword-in-range', 'range', 'range_field_conjunction_big_range_big_term_query', 'range_field_conjunction_small_range_big_term_query',
          'range_field_conjunction_small_range_small_term_query', 'range_field_disjunction_big_range_small_term_query', 'range-agg-1', 'range-agg-2',
          'range-numeric', 'range-aggregation', 'range-date-histo', 'range-date-histo-with-metrics', 'range-numeric-significant-terms']
date_histogram = ['composite-date_histogram-daily', 'date_histogram_hourly_agg', 'date_histogram_minute_agg', 'range-auto-date-histo', 'range-auto-date-histo-with-metrics', 'date-histo-numeric-terms']
date_histogram_additional = ['date-histo-entire-range', 'date-histo-geohash-grid', 'date-histo-geotile-grid', 'date-histo-histo', 'date-histo-string-significant-terms-via-default-strategy', 'date-histo-string-significant-terms-via-global-ords', 'date-histo-string-significant-terms-via-map', 'date-histo-string-terms-via-default-strategy', 'date-histo-string-terms-via-global-ords', 'date-histo-string-terms-via-map', 'range-auto-date-histo-with-time-zone']

desired_order = text_querying + sorting + term_aggs + range_queries + date_histogram + date_histogram_additional
print(desired_order)

def sort_and_create_new_table(filename, output, show_rsd):
    df = pd.read_csv(filename)

    df['operation'] = pd.Categorical(df['operation'], categories=desired_order, ordered=True)
    df = df.sort_values(['operation', 'distribution_version'])

    pivoted = df.pivot_table(index='operation', observed=False, columns='distribution_version', values='p90_value', aggfunc=['mean', 'std'])

    # Flatten the multiindex columns
    pivoted.columns = [f'{col[1]}_{col[0]}' for col in pivoted.columns]

    if show_rsd:
        # Calculate the relative standard deviation (RSD) for each distribution version
        for col in [c for c in pivoted.columns if c.endswith('_mean')]:
            version = col.split('_')[0]
            mean_col = col
            std_col = f'{version}_std'
            pivoted[f'{version} Relative STD (%)'] = ((pivoted[std_col] / pivoted[mean_col]) * 100).round(2)

        pivoted = pivoted.drop([col for col in pivoted.columns if col.endswith('_std') and not col.endswith('Relative STD (%)')], axis=1)
    if not show_rsd:
        pivoted = pivoted.drop([col for col in pivoted.columns if col.endswith('_std')], axis=1)

    # Rename mean columns
    pivoted = pivoted.rename(columns={col: col.split('_')[0] for col in pivoted.columns if col.endswith('_mean')})

    pivoted = pivoted.fillna(0)

    pivoted.to_csv(f"{output}.csv")

    print("Finished sorting operations and creating new CSV")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Sort CSV OSB Test Executions from Quicksight", description="A tool to sort and convert OSB CSV results exported from Quicksight")
    parser.add_argument("--file", "-f", required=True, help="CSV file")
    parser.add_argument("--output", "-o", required=True, help="Output CSV filename")
    parser.add_argument("--show-rsd", "-r", action="store_true", help="Output CSV filename")
    args = parser.parse_args()
    results_file = args.file
    output_file = args.output
    show_rsd = args.show_rsd
    sort_and_create_new_table(results_file, output_file, show_rsd)