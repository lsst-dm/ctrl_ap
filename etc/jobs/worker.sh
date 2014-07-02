#!/bin/sh
export HOME=/usr/local/home/srp
export SHELL=/bin/sh
export USER=srp
source /etc/bashrc

# LSST Personal software stack
WORKDIR=/nfs/workflow/srp
export LSSTSW=$WORKDIR/lsstsw
. $LSSTSW/loadLSST.sh
setup ctrl_events
HERE=$PWD
cd $WORKDIR/ap/ctrl_ap
setup -r .


workerJob.py $*
hostname
