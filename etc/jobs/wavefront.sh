#!/bin/sh
export HOME=/usr/local/home/srp
export SHELL=/bin/sh
export USER=srp
source /etc/bashrc

# LSST Personal software stack
WORKDIR=/nfs/workflow
export LSSTSW=$WORKDIR/lsstsw_rc3
. $LSSTSW/loadLSST.bash
setup pex_config
setup ctrl_events
HERE=$PWD
cd $WORKDIR/srp/ap/ctrl_ap
setup -r .

wavefrontJob.py $*
hostname
