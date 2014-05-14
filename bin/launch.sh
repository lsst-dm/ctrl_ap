#!/bin/bash
ssh lsst11 $PWD/launchReplicator.sh 9000 8 &
ssh lsst14 $PWD/launchReplicator.sh 9008 8 &
ssh lsst15 $PWD/launchReplicator.sh 9016 8 &
ssh lsst5 $PWD/launchReplicator.sh 9024 4 &
ssh lsst6 $PWD/launchReplicator.sh 9028 4 &
ssh lsst9 $PWD/launchReplicator.sh 9032 8 &
