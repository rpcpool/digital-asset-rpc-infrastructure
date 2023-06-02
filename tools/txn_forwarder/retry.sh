#!/bin/bash

# Check if at least one argument was provided
if [ $# -eq 0 ]; then
    echo "No arguments provided"
    exit 1
fi

# Define the command. If a second argument is provided, add the --before flag.
if [ -n "$2" ]; then
    cmd="cargo run -- --before $2 --redis-url $REDIS_URL --rpc-url $RPC_URL address --address $1"
else
    cmd="cargo run -- --redis-url $REDIS_URL --rpc-url $RPC_URL address --address $1"
fi

while true; do

    # Create a temporary file to hold the output
    tempfile=$(mktemp)

    # Run the command, print the output to the terminal with tee, and also save it to the temporary file
    $cmd 2>&1 | tee -a $1.log | tee $tempfile

    # Load the output from the temporary file
    output=$(cat $tempfile)

    # Delete the temporary file
    rm $tempfile

    # Check if there was an error
    if echo "$output" | grep -q 'Transaction could not be decoded'; then
        # If so, get the last "Sent transaction to stream" line
        last_line=$(echo "$output" | grep 'Sent transaction to stream' | tail -n 1)

        # And extract the stream id
        stream_id=$(echo $last_line | awk '{print $NF}')

        # Update the command with the new --before flag
        cmd="cargo run -- --before $stream_id --redis-url $REDIS_URL --rpc-url $RPC_URL address --address $1"
    else
        # If there were no errors, break the loop
        break
    fi
done

echo "Done!"
