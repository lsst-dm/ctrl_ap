#!/bin/bash
let distPort=$1
let repPort=$2
for i in `seq 1 8`;
do
    replicatorNode.py -D lsst-work.ncsa.illinois.edu -P $distPort -R $repPort
    echo $distPort $repPort
    let repPort=repPort+1
    let distPort=distPort+1
done
