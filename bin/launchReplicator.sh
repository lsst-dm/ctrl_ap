#!/bin/bash
export LSSTSW=/lsst/home/srp/lsstsw_temp/lsstsw
. $LSSTSW/bin/setup.sh
setup ctrl_events
cd ap
setup -r ctrl_ap
setup -r htcondor
let repPort=8001
let distPort=$1
for i in `seq 1 $2`;
do
    python -u $CTRL_AP_DIR/bin/replicatorNode.py -D lsst-work.ncsa.illinois.edu -P $distPort -R $repPort &>/tmp/rep.$i.out &
    echo $distPort $repPort
    let repPort=repPort+1
    let distPort=distPort+1
done
