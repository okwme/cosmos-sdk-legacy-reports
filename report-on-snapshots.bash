#!/usr/bin/env bash

set -e
trap cleanup EXIT

WORKING_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

DENOM=${DENOM:-uatom}

# the binaries to use
NODE_BINARY=${NODE_BINARY:-gaiad}
CLI_BINARY=${CLI_BINARY:-gaiacli}
PYTHON_BINARY=${PYTHON_BINARY:-python3}

# DATA_DIR is the --home option when running gaiad
if [ -z $DATA_DIR ]; then
  echo "No DATA_DIR specified. This script must be run with a --home option which resides in a ZFS pool/filesystem."
  exit 1
fi

# determine ZFS filesystem in use
ZFS_FS=$(zfs list | grep `eval echo $DATA_DIR` | awk '{print $1;}')
ZFS_POOL=$(zpool list -Hp | awk '{print $1;}' | while read pool; do if [[ "$ZFS_FS" =~ ^$pool ]]; then echo $pool; break; fi; done)
# ensure any existing tmp clone is destroyed
(sudo zfs destroy $ZFS_POOL/tmp > /dev/null 2>&1 || exit 0)

# lock down p2p port so no further syncing can take place
echo -n "Locking down firewall... "
sudo ufw deny to any port 26656 > /dev/null 2>&1
sudo ufw deny out to any port 26656 > /dev/null 2>&1
sleep 3
echo "DONE"

NODE_PID=0
LCD_PID=0
start_node() {
  home=${1:-/$ZFS_POOL/tmp}
  echo -n "Starting node... "
  $NODE_BINARY start --home "$home" > $home/node.log 2>&1 &
  NODE_PID=$!
  sleep 1
  echo "OK (pid: $NODE_PID)"
}
start_lcd() {
  home=${1:-/$ZFS_POOL/tmp}
  echo -n "Starting LCD... "
  $CLI_BINARY rest-server --laddr tcp://0.0.0.0:1317 --home "$home" --trust-node=true > $home/lcd.log 2>&1 &
  LCD_PID=$!
  sleep 1
  echo "OK (pid: $LCD_PID)"
}
stop_node() {
  kill -SIGINT $NODE_PID
}
stop_lcd() {
  kill -SIGINT $LCD_PID
}

clone_snapshot() {
  echo "Switching to snapshot: $snapshot"
  sudo zfs clone $1 $ZFS_POOL/tmp
}
discard_clone() {
  sudo zfs destroy $ZFS_POOL/tmp
}

cleanup() {
  stop_lcd
  stop_node
}

echo "Determining network..."
start_node "$DATA_DIR"
sleep 2
NETWORK_NAME=$(curl -s localhost:26657/status | jq -r '.result.node_info.network')
if [[ -z $NETWORK_NAME || $NETWORK_NAME == "" ]]; then
  echo "Unable to determine network."
  exit 1
fi
echo "Network: $NETWORK_NAME"
stop_node
echo "Database: $WORKING_DIR/${NETWORK_NAME}.db"
echo "Log: $WORKING_DIR/${NETWORK_NAME}-reports.log"

sleep 2

(rm "$WORKING_DIR/${NETWORK_NAME}.db" > /dev/null 2>&1 || exit 0)

zfs list -Hp -t snapshot -o name | grep $ZFS_FS | while read snapshot; do
  echo
  clone_snapshot "$snapshot"
  sleep 3

  start_node
  start_lcd
  sleep 2

  $PYTHON_BINARY -u $WORKING_DIR/calculate_earnings.py \
    --denom $DENOM \
    --db-path $WORKING_DIR/${NETWORK_NAME}.db \
    --log-path $WORKING_DIR/${NETWORK_NAME}-reports.log

  stop_lcd
  stop_node
  sleep 2

  discard_clone
  sleep 3
done


$PYTHON_BINARY -u $WORKING_DIR/output_csvs.py \
  --denom $DENOM \
  --db-path $WORKING_DIR/${NETWORK_NAME}.db \
  --output-dir $WORKING_DIR/csvs
