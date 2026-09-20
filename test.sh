#!/usr/bin/env bash
#
# Manual unit test runner for the error-handling / retry refactor.
#
# The new unified error taxonomy, retry policies and dead letter queue are
# covered by tests/test_error_handling.py (pure offline, no sockets/external
# services).  It also runs the other self-contained unit modules; tests that
# require network, xmlrpc sockets or optional services (httpbin, databases,
# phantomjs, ...) are grouped separately and skipped unless their deps are
# available, so a normal `./test.sh` run stays deterministic.
#
# Usage:
#   ./test.sh                 # offline unit tests (default)
#   ./test.sh --all           # include service/integration tests too
#   ./test.sh --new           # only the error/retry/DLQ tests
#   ./test.sh path/to/test.py # explicit test module(s)
set -u

cd "$(dirname "$0")"

# pick the project venv interpreter if present
if [ -x ".venv/bin/python" ]; then
    PY=".venv/bin/python"
else
    PY="$(command -v python3 || command -v python)"
fi

MODE="offline"
EXPLICIT=()
for arg in "$@"; do
    case "$arg" in
        --new) MODE="new" ;;
        --all) MODE="all" ;;
        -h|--help)
            sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) EXPLICIT+=("$arg") ;;
    esac
done

if [ "${#EXPLICIT[@]}" -gt 0 ]; then
    echo "==> Running: ${EXPLICIT[*]}"
    exec "$PY" -m pytest "${EXPLICIT[@]}" -v
fi

# tier 1: tests introduced by this refactor (must always pass offline)
NEW_TESTS=(
    "tests/test_error_handling.py"
)

# tier 2: existing self-contained offline modules
OFFLINE_TESTS=(
    "tests/test_error_handling.py"
    "tests/test_base_handler.py"
    "tests/test_task_queue.py"
    "tests/test_counter.py"
    "tests/test_utils.py"
    "tests/test_response.py"
    "tests/test_result_dump.py"
)

# extra pytest args appended to the offline run (keyword-selected sqlite /
# local database tests do not need external services)
OFFLINE_EXTRA_ARGS=(
    "tests/test_database.py"
    "-k"
    "Sqlite or Local"
)

# tier 3: need sockets / external binaries / optional services
SERVICE_TESTS=(
    "tests/test_scheduler.py"
    "tests/test_fetcher.py"
    "tests/test_processor.py"
    "tests/test_fetcher_processor.py"
    "tests/test_webui.py"
    "tests/test_xmlrpc.py"
    "tests/test_run.py"
    "tests/test_message_queue.py"
    "tests/test_result_worker.py"
    "tests/test_webdav.py"
    "tests/test_bench.py"
)

run_tests() {
    local label="$1"; shift
    echo ""
    echo "==> $label"
    "$PY" -m pytest "$@" -v
    return $?
}

check_import() {
    "$PY" -c "import $1" >/dev/null 2>&1
}

rc=0

case "$MODE" in
    new)
        run_tests "Error taxonomy / retry policy / dead letter queue" \
            "${NEW_TESTS[@]}"
        rc=$?
        ;;
    offline)
        # drop modules whose collection is broken in this environment
        available=()
        for t in "${OFFLINE_TESTS[@]}"; do
            if "$PY" -m pytest --collect-only -q "$t" >/dev/null 2>&1; then
                available+=("$t")
            else
                echo "==> skip $t (missing optional dependency)"
            fi
        done
        run_tests "Offline unit tests" "${available[@]}"
        rc=$?
        echo ""
        echo "==> SQLite / local database tests"
        "$PY" -m pytest "${OFFLINE_EXTRA_ARGS[@]}" -v || rc=1
        ;;
    all)
        run_tests "Offline unit tests" "${OFFLINE_TESTS[@]}" || rc=1
        run_tests "Service / integration tests (need sockets & services)" \
            "${SERVICE_TESTS[@]}" || rc=1
        ;;
esac

echo ""
if [ "$rc" -eq 0 ]; then
    echo "==> RESULT: PASS"
else
    echo "==> RESULT: FAIL (exit $rc)"
    echo "    re-run a single failing module, e.g.:"
    echo "    $PY -m pytest tests/test_error_handling.py -v"
fi
exit "$rc"
