#!/usr/bin/env bash

# how frequently to stop the chain and snapshot
REPORT_FREQUENCY=${REPORT_FREQUENCY:-"1 days"}

# the binary to run
NODE_BINARY=${NODE_BINARY:-gaiad}

# how to access the RPC
RPC_URL=${RPC_URL:-"localhost:26657"}

# FINAL_BLOCK is needed so we know when the chain stopped
if [ -z $FINAL_BLOCK ]; then
  echo "Specify FINAL_BLOCK height of the target chain."
  exit 1
fi

# DATA_DIR is the --home option when running gaiad
if [ -z $DATA_DIR ]; then
  echo "No DATA_DIR specified. This script must be run with a --home option which resides in a ZFS pool/filesystem."
  exit 1
fi

# determine ZFS filesystem in use
ZFS_FS=$(zfs list | grep `eval echo $DATA_DIR` | awk '{print $1;}')

if [ ! -z $RESET_DATA ]; then
  echo -n "Resetting data... "

  # clean existing ZFS snapshots
  zfs list -Hp -t snapshot -o name | grep $ZFS_FS | while read snapshot; do
    sudo zfs destroy -R $snapshot
  done

  # clear chain data
  $NODE_BINARY unsafe-reset-all --home $DATA_DIR > /dev/null 2>&1

  sleep 3
  echo "DONE"
fi

echo -n "Determine genesis time... "
GENESIS_TIME=$(date -d `cat $DATA_DIR/config/genesis.json | jq -r '.genesis_time'`)
echo "$GENESIS_TIME"

NODE_PID=0
start_sync() {
  echo -n "Starting sync... "
  sudo ufw allow to any port 26656
  sudo ufw allow out to any port 26656
  $NODE_BINARY start --home $DATA_DIR > $DATA_DIR/node.log 2>&1 &
  NODE_PID=$!
  sleep 1
  echo "OK (pid: $NODE_PID)"
}
stop_sync() {
  kill -SIGINT $NODE_PID
}
snapshot() {
  snapshot_name=`date -d "$1" +%Y-%m-%d`
  sudo zfs snapshot $ZFS_FS@$snapshot_name
}

start_sync

NEXT_DATE=$(date -d "$GENESIS_TIME+$REPORT_FREQUENCY")
echo "NEXT SNAPSHOT AT $NEXT_DATE"

while true; do
  r=`curl -s $RPC_URL/status`
  if [[ $r == "" ]]; then continue; fi
  sync_info=$(echo -n $r | jq .result.sync_info)
  height=$(echo -n $sync_info | jq -r .latest_block_height)

  # if we're up to the final block of the chain,
  # take one final snapshot
  if (( $height >= $FINAL_BLOCK )); then
    stop_sync
    echo "FINAL SNAPSHOT $height at $time"
    snapshot "$time"
    exit 0
  fi

  # determine the time of this block
  time=$(date -d `echo -n $sync_info | jq -r .latest_block_time`)

  # check if the block time after our next target snapshot time
  timestamp=`date -d "$time" +%s`
  target=`date -d "$NEXT_DATE" +%s`
  if (( $timestamp >= $target )); then
    # we need to make a new snapshot at this height
    stop_sync
    snapshot "$time"
    echo "SNAPSHOT HEIGHT: $height at $time"

    # calculate next snapshot time based on report frequency
    NEXT_DATE=$(date -d "$NEXT_DATE+$REPORT_FREQUENCY")
    echo "NEXT SNAPSHOT AT $NEXT_DATE"
    echo

    # keep syncing
    start_sync
  else
    echo -ne "* $height at $time\r"
    sleep 0.05
  fi
done

# just in case, make sure gaiad is stopped
stop_sync
