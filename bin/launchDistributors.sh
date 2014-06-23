#!/bin/bash
let distPort=9000
for i in `seq 1 22`;
do
    python -u $CTRL_AP_DIR/bin/distributorNode.py -P $distPort &>/tmp/dist.$i.out &
    pid=$!
    echo $pid >/tmp/dist.$i.pid
    let distPort=distPort+1
done
