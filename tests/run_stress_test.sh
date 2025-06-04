#!/bin/bash

set -e

echo "Starting pg_mooncake stress test..."
echo "Make sure 'make run' is running in another terminal first!"
echo

if ! command -v python3 &> /dev/null; then
    echo "Error: python3 not found"
    exit 1
fi

if ! python3 -c "import psycopg2" 2>/dev/null; then
    echo "Installing psycopg2-binary..."
    pip3 install psycopg2-binary
fi

echo "Running stress test with default parameters..."
python3 stress_test.py --duration 30 --verbose

echo
echo "Stress test completed!"
