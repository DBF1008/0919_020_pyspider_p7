#!/bin/sh
# Run all unit tests manually.
# Usage: ./test.sh            # run all test modules
#        ./test.sh scheduler  # run a single module, e.g. tests/test_scheduler.py
set -e
cd "$(dirname "$0")"

run() {
    echo "======================================================================"
    echo "RUN  tests.$1"
    echo "======================================================================"
    python -m unittest "tests.$1" -v
}

if [ -n "$1" ]; then
    run "$1"
    exit 0
fi

# unified error classification / retry policy (no third-party deps needed)
run test_error_policy

# scheduler / fetcher / processor, including retry & dead-letter integration
run test_scheduler
run test_task_queue
run test_counter
run test_processor
run test_fetcher
run test_fetcher_processor
run test_base_handler

# the rest of the suite
run test_utils
run test_response
run test_database
run test_message_queue
run test_result_worker
run test_result_dump
run test_webui
run test_webdav
run test_xmlrpc
run test_run
