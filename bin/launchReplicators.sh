#!/bin/bash
ssh lsst-run1 $CTRL_AP_DIR/bin/launchReplicator.sh 9000 11 &
ssh lsst-run2 $CTRL_AP_DIR/bin/launchReplicator.sh 9008 11 &
#ssh lsst15 $CTRL_AP_DIR/bin/launchReplicator.sh 9016 8 &
#ssh lsst5 $CTRL_AP_DIR/bin/launchReplicator.sh 9024 4 &
#ssh lsst6 $CTRL_AP_DIR/bin/launchReplicator.sh 9028 4 &
#ssh lsst9 $CTRL_AP_DIR/bin/launchReplicator.sh 9032 8 &
