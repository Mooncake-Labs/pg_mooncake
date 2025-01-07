#!/bin/bash

echo "Building and installing the extension..."
make debug -j$(nproc)
if [ $? -ne 0 ]; then
    echo "Failed to build the extension."
    exit 1
fi

make install -j$(nproc)
if [ $? -ne 0 ]; then
    echo "Failed to install the extension."
    exit 1
fi
echo "Extension built and installed successfully."

PG_DATA_DIR="/usr/local/pgsql/data"

echo "Restarting PostgreSQL..."
pg_ctl -D "$PG_DATA_DIR" restart
if [ $? -ne 0 ]; then
    echo "Failed to restart PostgreSQL."
    exit 1
fi
echo "PostgreSQL restarted successfully."

echo "Reloading pg_mooncake extension..."
psql -U postgres -c "DROP EXTENSION IF EXISTS pg_mooncake CASCADE;" &&
psql -U postgres -c "CREATE EXTENSION pg_mooncake;"

if [ $? -ne 0 ]; then
    echo "Failed to reload pg_mooncake extension."
    exit 1
fi

echo "pg_mooncake extension reloaded successfully."
