#!/bin/sh
export HOME=/lsst/home/srp
export SHELL=/bin/sh
export USER=srp
source /etc/bashrc

# LSST Personal software stack
export LSSTSW=/lsst/home/srp/lsstsw_temp/lsstsw
. $LSSTSW/bin/setup.sh
setup ctrl_events
HERE=$PWD
cd /lsst/home/srp/ap/ctrl_ap
setup -r .

export JOBS_PATH=/lsst/home/srp/ap/ctrl_ap/etc/jobs

python $JOBS_PATH/wavefrontJob.py $*
hostname
