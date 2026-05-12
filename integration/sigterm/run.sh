#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/../common.sh"

echo "=== SIGTERM rollback integration test ==="

# Build the binary once.
pushd "${SCRIPT_DIR}/../../" > /dev/null
cargo build
PGDOG_BIN="${PWD}/target/debug/pgdog"
popd > /dev/null

# Start pgdog with our config (short shutdown_timeout).
PGDOG_BIN="${PGDOG_BIN}" run_pgdog "${SCRIPT_DIR}"
wait_for_pgdog

# Read the pgdog PID so we can send SIGTERM later.
PGDOG_PID=$(cat "${SCRIPT_DIR}/../common/pgdog.pid" 2>/dev/null || cat "${SCRIPT_DIR}/../pgdog.pid" 2>/dev/null || echo "")
if [ -z "${PGDOG_PID}" ]; then
    # Fallback: find it from the common.sh pid file location.
    COMMON_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/../" &> /dev/null && pwd )
    PGDOG_PID=$(cat "${COMMON_DIR}/common/pgdog.pid" 2>/dev/null || pgrep -f "pgdog.*sigterm/pgdog.toml" || echo "")
fi

if [ -z "${PGDOG_PID}" ] || ! kill -0 "${PGDOG_PID}" 2>/dev/null; then
    echo "FAIL: could not find pgdog process"
    exit 1
fi

echo "PgDog running with PID ${PGDOG_PID}"

# Step 1: Open a transaction through pgdog and capture the backend PID.
# Use a coproc so the connection stays alive.
coproc TXCONN { psql -h 127.0.0.1 -p 6432 -U pgdog -d pgdog -At 2>&1; }

# Send BEGIN and get the backend PID.
echo "BEGIN;" >&"${TXCONN[1]}"
read -t 5 begin_result <&"${TXCONN[0]}"
echo "BEGIN result: ${begin_result}"

echo "SELECT pg_backend_pid();" >&"${TXCONN[1]}"
read -t 5 backend_pid <&"${TXCONN[0]}"
echo "Backend PID via pgdog: ${backend_pid}"

if [ -z "${backend_pid}" ] || [ "${backend_pid}" = "" ]; then
    echo "FAIL: could not get backend PID"
    exit 1
fi

# Capture backend_start to uniquely identify the session even if PID is recycled.
backend_start=$(psql -h 127.0.0.1 -U pgdog -d pgdog -At -c \
    "SELECT backend_start FROM pg_stat_activity WHERE pid = ${backend_pid}" 2>/dev/null)
echo "Backend start: ${backend_start}"

# Step 2: Verify precondition — the backend IS in idle-in-transaction state.
sleep 0.5
state=$(psql -h 127.0.0.1 -U pgdog -d pgdog -At -c \
    "SELECT state FROM pg_stat_activity WHERE pid = ${backend_pid} AND backend_start = '${backend_start}'")
echo "Backend state before SIGTERM: ${state}"

if [ "${state}" != "idle in transaction" ]; then
    echo "FAIL: expected backend to be 'idle in transaction', got '${state}'"
    exit 1
fi

# Step 3: Send SIGTERM to pgdog. Disable the EXIT trap so stop_pgdog doesn't
# double-kill.
export PGDOG_KEEP_RUNNING=1
kill -TERM "${PGDOG_PID}"

echo "Sent SIGTERM to pgdog (PID ${PGDOG_PID}), waiting for exit..."
waited=0
while kill -0 "${PGDOG_PID}" 2>/dev/null && [ ${waited} -lt 15 ]; do
    sleep 1
    waited=$((waited + 1))
done

if kill -0 "${PGDOG_PID}" 2>/dev/null; then
    echo "FAIL: pgdog did not exit within 15 seconds"
    kill -KILL "${PGDOG_PID}" 2>/dev/null || true
    exit 1
fi
echo "PgDog exited after ${waited}s"

# Close the coproc (the connection is already dead).
exec {TXCONN[1]}>&- 2>/dev/null || true
wait "${TXCONN_PID}" 2>/dev/null || true

# Step 4: Assert — the backend should NOT still be in idle-in-transaction.
sleep 0.5
state_after=$(psql -h 127.0.0.1 -U pgdog -d pgdog -At -c \
    "SELECT state FROM pg_stat_activity WHERE pid = ${backend_pid} AND backend_start = '${backend_start}'" 2>/dev/null || echo "gone")
echo "Backend state after SIGTERM: ${state_after}"

if [ "${state_after}" = "idle in transaction" ]; then
    echo "FAIL: backend is STILL 'idle in transaction' — ROLLBACK was not issued"
    exit 1
fi

echo "PASS: backend is no longer in idle-in-transaction (state: ${state_after:-gone})"

# Clean up the pid file so stop_pgdog doesn't try to kill a dead process.
rm -f "${SCRIPT_DIR}/../common/pgdog.pid" 2>/dev/null || true

# Print pgdog log.
if [ -f "${SCRIPT_DIR}/../common/log.txt" ]; then
    echo "=== PgDog log ==="
    cat "${SCRIPT_DIR}/../common/log.txt"
    rm -f "${SCRIPT_DIR}/../common/log.txt"
fi

echo "=== SIGTERM rollback test PASSED ==="
