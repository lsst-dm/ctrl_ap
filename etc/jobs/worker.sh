#!/bin/sh
echo -n "script starts: ";date
echo "args are", $*
export HOME=/usr/local/home/srp
export SHELL=/bin/sh
export USER=srp
source /etc/bashrc

# LSST Personal software stack
WORKDIR=/nfs/workflow
export LSSTSW=$WORKDIR/lsstsw_rc3
echo -n "stack init started: ";date
. $LSSTSW/loadLSST.bash
echo -n "stack init finished: ";date
setup pex_config
setup ctrl_events
HERE=$PWD
cd $WORKDIR/srp/ap/ctrl_ap
setup -r .

echo -n "worker starts: ";date
workerJob.py $*
hostname
echo -n "worker completes: ";date
