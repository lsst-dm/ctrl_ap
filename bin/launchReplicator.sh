#!/bin/bash

# setup the main lsst stack in .bashrc before executing
# and set _CTRL_AP_DIR and _HTCONDOR_DIR to the correct directories.
. $LSSTSW/bin/setup.sh
setup ctrl_events
setup -r $_CTRL_AP_DIR
setup -r $_HTCONDOR_DIR
let repPort=8001
let distPort=$1
for i in `seq 1 $2`;
do
    python -u $CTRL_AP_DIR/bin/replicatorNode.py -D lsst-work.ncsa.illinois.edu -P $distPort -R $repPort &>/tmp/rep.$i.out &
    echo $distPort $repPort
    let repPort=repPort+1
    let distPort=distPort+1
done
