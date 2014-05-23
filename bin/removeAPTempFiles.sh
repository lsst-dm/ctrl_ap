#!/bin/bash
ssh lsst5 /bin/rm -f /tmp/tmp?_* /tmp/rep.*.out
ssh lsst6 /bin/rm -f /tmp/tmp_* /tmp/rep.*.out
ssh lsst9 /bin/rm -f /tmp/tmp_* /tmp/rep.*.out
ssh lsst11 /bin/rm -f /tmp/tmp_* /tmp/rep.*.out
ssh lsst14 /bin/rm -f /tmp/tmp_* /tmp/rep.*.out
ssh lsst15 /bin/rm -f /tmp/tmp_* /tmp/rep.*.out
ssh lsst-work /bin/rm -f /tmp/tmp_* /tmp/rep.*.out /tmp/dist.*.out
