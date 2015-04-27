#!/bin/sh
export HOME=/usr/local/home/srp
export SHELL=/bin/sh
export USER=srp
source /etc/bashrc

# LSST Personal software stack
WORKDIR=/nfs/workflow
export LSSTSW=$WORKDIR/lsstsw_r3
. $LSSTSW/loadLSST.bash
setup pex_config
setup ctrl_events
HERE=$PWD
cd $WORKDIR/ap/ctrl_ap
setup -r .

wavefrontSensorJob.py $*
hostname
