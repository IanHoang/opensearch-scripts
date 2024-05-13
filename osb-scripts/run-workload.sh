#!/bin/bash
host="\"\""
workload="big5"
# No force merge
id="os-213"
tag="test-type:os-213"
workload_params='"bulk_indexing_clients:1,number_of_replicas:0"'
client_options="\"basic_auth_user:'',basic_auth_password:'',verify_certs:false\""

err_exit() {
    echo "${0##*/}: $1" >&2
    exit 1
}

mkdir ~/$id

for i in {1..4}
do
    echo "Running Search Only Test"
    run_id="${id}-run-${i}"
    results_file="/home/ec2-user/${run_id}"

    #opensearch-benchmark execute-test --pipeline=benchmark-only --workload=$workload  --target-hosts=$host --client-options=$client_options --workload-params=$workload_params --kill-running-processes --results-file=$results_file --test-execution-id=$run_id --include-tasks="type:search-only" --test-mode

    # Ingestion and Search
    # opensearch-benchmark execute-test --pipeline=benchmark-only --workload=big5 --target-hosts="" --client-options="basic_auth_user:'',basic_auth_password:'',verify_certs:false" --workload-params=target_throughput:"",bulk_indexing_clients:1,number_of_replicas:0 --kill-running-processes --results-file=/home/ec2-user/$run_id --test-execution-id=$run_id

    # Search Only
    opensearch-benchmark execute-test --pipeline=benchmark-only --workload=big5 --target-hosts="" --client-options="basic_auth_user:'',basic_auth_password:'',verify_certs:false" --workload-params=target_throughput:"",bulk_indexing_clients:1,number_of_replicas:0 --kill-running-processes --results-file=/home/ec2-user/$run_id --test-execution-id=$run_id --include-tasks="type:search"

    echo "Finished running iteration. Moving test execution file to home directory"
    cp ~/.benchmark/benchmarks/test_executions/$run_id/test_execution.json ~/$run_id.json || err_exit "Cannot cp file $run_id/test_execution.json over to home directory"
    mv ~/$run_id.json ~/$id || err_exit "Cannot move $run_id.json"

    echo "Sleeping for 10 seconds"
    sleep 10

done

echo "Finshed running tests for all iterations"