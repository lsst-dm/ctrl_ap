#!/bin/bash
for i in `ls /tmp/dist.*.pid`
do
     kill -9 `cat $i`
     rm $i
done
