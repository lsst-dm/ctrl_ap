#!/bin/bash
ssh lsst11 $CTRL_AP_DIR/bin/launchReplicator.sh 9000 8 &
ssh lsst14 $CTRL_AP_DIR/bin/launchReplicator.sh 9008 8 &
ssh lsst15 $CTRL_AP_DIR/bin/launchReplicator.sh 9016 8 &
ssh lsst5 $CTRL_AP_DIR/bin/launchReplicator.sh 9024 4 &
ssh lsst6 $CTRL_AP_DIR/bin/launchReplicator.sh 9028 4 &
ssh lsst9 $CTRL_AP_DIR/bin/launchReplicator.sh 9032 8 &
