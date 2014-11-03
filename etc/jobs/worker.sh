#!/bin/sh
echo -n "script starts: ";date
export HOME=/usr/local/home/srp
export SHELL=/bin/sh
export USER=srp
source /etc/bashrc

# LSST Personal software stack
WORKDIR=/nfs/workflow/srp
export LSSTSW=$WORKDIR/lsstsw
echo -n "stack init started: ";date
. $LSSTSW/loadLSST.sh
echo -n "stack init finished: ";date
setup pex_config
setup ctrl_events
HERE=$PWD
cd $WORKDIR/ap/ctrl_ap
setup -r .

echo -n "worker starts: ";date
workerJob.py $*
hostname
