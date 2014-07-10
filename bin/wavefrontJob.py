#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2014 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import time
import datetime
import os
import sys
import argparse
import lsst.ctrl.events as events
from lsst.daf.base import PropertySet
from lsst.ctrl.ap.job import Job
from lsst.pex.logging import Log

class WavefrontJob(Job):

    def __init__(self, expectedVisitID, expectedExpSeqID):
        # TODO:  these need to be placed in a configuration file
        # which is loaded, so they are not embedded in the code
        self.brokerName = "lsst8.ncsa.illinois.edu"
        self.eventTopic = "ocs_startReadout"
        self.expectedVisitID = expectedVisitID
        self.expectedExpSeqID = expectedExpSeqID
        logger = Log.getDefaultLog()
        self.logger = Log(logger, "WavefrontJob")

    def getCameraImage(self, imageID, visitID, exposureSequenceID):
        self.logger(Log.INFO, "getting data for four wave front sensors; visitID = %s, exposureSequenceID = %s" % (visitID, exposureSequenceID))

if __name__ == "__main__":
    basename = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=basename)
    parser.add_argument("-I", "--visitID", type=int, action="store", help="visit id", required=True)
    parser.add_argument("-x", "--exposureSequenceID", type=int, action="store", help="exposure sequence id", required=True)

    args = parser.parse_args()
    base = WavefrontJob(args.visitID, args.exposureSequenceID)
    base.handleEvents()

