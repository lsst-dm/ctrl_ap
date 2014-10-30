#!/bin/bash
ssh lsst-work /bin/rm -f /tmp/tmp_* /tmp/rep.*.out /tmp/dist.*.out
ssh lsst-run1 /bin/rm -f /tmp/rep.*.out
ssh lsst-run2 /bin/rm -f /tmp/rep.*.out
ssh lsst-run3 /bin/rm -f /tmp/rep.*.out
ssh lsst-run4 /bin/rm -f /tmp/rep.*.out
ssh lsst-run5 /bin/rm -f /tmp/rep.*.out
ssh lsst-run1 /bin/rm -rf /tmp/lsst
ssh lsst-run2 /bin/rm -rf /tmp/lsst
ssh lsst-run3 /bin/rm -rf /tmp/lsst
ssh lsst-run4 /bin/rm -rf /tmp/lsst
ssh lsst-run5 /bin/rm -rf /tmp/lsst
ssh lsst-run6 /bin/rm -rf /tmp/lsst
ssh lsst-run7 /bin/rm -rf /tmp/lsst
ssh lsst-run8 /bin/rm -rf /tmp/lsst
ssh lsst-run9 /bin/rm -rf /tmp/lsst
ssh lsst-run10 /bin/rm -rf /tmp/lsst
ssh lsst-run11 /bin/rm -rf /tmp/lsst
ssh lsst-run12 /bin/rm -rf /tmp/lsst
ssh lsst-run13 /bin/rm -rf /tmp/lsst
ssh lsst-run14 /bin/rm -rf /tmp/lsst
ssh lsst-run15 /bin/rm -rf /tmp/lsst
ssh lsst-rep /bin/rm -f /tmp/tmp_* /tmp/rep.*.out

ssh lsst-rep1 /bin/rm -f /tmp/tmp_* /tmp/rep.*.out /tmp/R:* /tmp/wave*
ssh lsst-rep2 /bin/rm -f /tmp/tmp_* /tmp/rep.*.out /tmp/R:* /tmp/wave*

ssh lsst-dist /bin/rm -f /tmp/dist* /tmp/R:* /tmp/wave*
ssh lsst-dist /bin/rm -rf /tmp/lsst
