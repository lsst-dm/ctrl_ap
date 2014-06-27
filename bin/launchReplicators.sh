#!/bin/bash
ssh lsst-rep1 $CTRL_AP_DIR/bin/launchReplicator.sh 9000 11 &
ssh lsst-rep2 $CTRL_AP_DIR/bin/launchReplicator.sh 9011 11 &
