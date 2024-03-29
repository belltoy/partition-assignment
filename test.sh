#!/usr/bin/env bash

CMD=./target/release/assignment
replication_factor=3
partitions=60
init_nodes=5
nodes_str=$(jq --arg num $init_nodes -n '[range(1; ($num | tonumber) + 1)] | map("node_" + (. | tostring)) | join(",")' -r)

init=$($CMD init -r $replication_factor -p $partitions -n "$nodes_str" -o json -w)

echo "Initial assignment:"
echo "$init"
echo

result=$init

for _i in $(seq 1 500); do
    remove_node=$(echo "$result" | jq '.assignment | to_entries | map(.value) | flatten | unique .[]' -r | shuf -n 1)
    result=$(echo "$result" | jq '.assignment' | $CMD remove -n "$remove_node" -r $replication_factor -o json -w)
    moves=$(echo "$result" | jq '.moves' -r)
    printf "Removed %8s,   moves: %3s\n" "$remove_node" "$moves"

    result=$(echo "$result" | jq '.assignment' | $CMD add -n "$remove_node" -o json -w)
    moves=$(echo "$result" | jq '.moves' -r)
    printf "Added   %8s,   moves: %3s\n" "$remove_node" "$moves"
done

echo
echo "Final assignment:"
echo "$result" | jq '.assignment' | $CMD validate -p $partitions -r $replication_factor
