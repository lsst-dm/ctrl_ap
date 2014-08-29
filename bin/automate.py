
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

import sys
import os.path
import argparse
import lsst.ctrl.events as events
from lsst.ctrl.ap import ocs
from lsst.ctrl.ap.status import Status

def AutomatedOCS(object):
    def __init__(self):
        self.st = Status()

    def sendStartIntegration(self, visitID, exposureSequenceID):
        self.st.publish(st.ocs, st.sendMsg, 
            {"cmd":"startIntegration", 
             "exposureSequenceID":args.exposureSequenceID,
             "visitID":args.visitID})
        self.ocs.sendStartIntegration(visitID, exposureSequenceID)

    def sendStartReadout(self, imageID, visitID, exposureSequenceID):
        self.st.publish(st.ocs, st.sendMsg, 
            {"cmd":args.cmd, 
             "exposureSequenceID":args.exposureSequenceID,
             "imageID":args.imageID, 
             "visitID":args.visitID})
        self.ocs.sendStartReadout(imageID, visitID, exposureSequenceID)

    def sendNextVisit(self, visitID, exposures, boresight, filterID):
        self.st.publish(st.ocs, st.sendMsg, 
                {"cmd":args.cmd, 
                 "exposures":args.exposures, 
                 "visitID":args.visitID, 
                 "filterID":args.filterID, 
                 "boresight":args.boresight})
        self.ocs.sendNextVisit(visitID, exposures, boresight, filterID)

    def begin(self):
        exposures = 2
        boresight = "18,18"
        filterID = "u"
        visitID = 18000
        imageID = 28000
        sleepInterval = 60
        visits = 2
        for visit in range(0, visits)
            self.sendNextVisit(visitID, exposures, boresight, filterID)
            time.sleep(sleepInterval)
            for expo in range(0, exposures):
                self.sendStartIntegration(visitID, expo)
                time.sleep(sleepInterval)
                self.sendStartReadout(imageID, visitID, expo)
                time.sleep(sleepInterval)
                imageID = imageID+1
            visitID = visitID+1

if __name__ == "__main__":
    auto = AutomatedOCS()

    auto.begin()
