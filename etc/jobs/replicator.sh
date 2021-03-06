#!/bin/sh
echo -n "script starts: ";date
export HOME=/usr/local/home/srp
export SHELL=/bin/sh
export USER=srp
source /etc/bashrc

# LSST Personal software stack
WORKDIR=/nfs/workflow
export LSSTSW=$WORKDIR/lsstsw_rc3
echo -n "init script starts: ";date
. $LSSTSW/loadLSST.bash
echo -n "init script ends: ";date
#setup pex_config
setup ctrl_events
HERE=$PWD
cd $WORKDIR/srp/ap/ctrl_ap
echo -n "last setup starts: ";date
setup -r .
echo -n "last setup ends: ";date
echo -n "replicatorJob starts: ";date
setup pex_config
cd $WORKDIR/srp/ap/ctrl_ap
replicatorJob.py $*
hostname
