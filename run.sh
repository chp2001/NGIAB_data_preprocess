#!/bin/sh

# clear the app log
# the file needs to exist and have non whitespace content
echo "Starting Application!" > app.log

# assugn current log level to var so we can reset it after the script ends
LOG_LEVEL=$DASK_LOGGING__DISTRIBUTED

export DASK_LOGGING__DISTRIBUTED="error"

dask scheduler > /dev/null &
dask worker localhost:8786 --nworkers -1 > /dev/null &

# if .dev is present run in debug mode
if [ -f .dev ]; then
    echo "Running in debug mode"
    flask -A map_app run --debug
else
    echo "Running in production mode"
    flask -A map_app run
fi

# Close the dask processes after the script ends
pkill -f "dask scheduler"
pkill -f "dask worker"

export DASK_LOGGING__DISTRIBUTED=$LOG_LEVEL