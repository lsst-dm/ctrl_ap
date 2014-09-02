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
import time
import os.path
import argparse
import lsst.ctrl.events as events
from lsst.ctrl.ap import ocs
from lsst.ctrl.ap.status import Status

class AutomatedOCS(object):
    def __init__(self):
        self.ocs = ocs.OCS()

    def sendStartIntegration(self, verbose, visitID, exposureSequenceID):
        if verbose:
            print "startIntegration: visitID = ",visitID," exposureSequenceID = ", exposureSequenceID
        st = Status()
        st.publish(st.ocs, st.sendMsg, 
            {"cmd":"startIntegration", 
             "exposureSequenceID":exposureSequenceID,
             "visitID":visitID})
        self.ocs.sendStartIntegration(visitID, exposureSequenceID)

    def sendStartReadout(self, verbose, imageID, visitID, exposureSequenceID):
        if verbose:
            print "startReadout: imageID",imageID,"visitID = ",visitID," exposureSequenceID = ", exposureSequenceID
        st = Status()
        st.publish(st.ocs, st.sendMsg, 
            {"cmd":"startReadout", 
             "exposureSequenceID":exposureSequenceID,
             "imageID":imageID, 
             "visitID":visitID})
        self.ocs.sendStartReadout(imageID, visitID, exposureSequenceID)

    def sendNextVisit(self, verbose, visitID, exposures, boresight, filterID):
        if verbose:
            print "startReadout: visitID = ", visitID, " exposures = ", exposures, "boresight = ", boresight, "filterID = ",filterID
        st = Status()
        st.publish(st.ocs, st.sendMsg, 
                {"cmd":"nextVisit", 
                 "exposures":exposures, 
                 "visitID":visitID, 
                 "filterID":filterID, 
                 "boresight":boresight})
        self.ocs.sendNextVisit(visitID, exposures, boresight, filterID)

    def begin(self, verbose, exposures, boresight, filterID, visitID, imageID, sleepInterval, visits):

        if verbose:
            print "running with the following parameters:"
            print "exposures per visit = ", exposures
            print "boresight = ", boresight
            print "filterID = ", filterID
            print "beginning visitID = ", visitID
            print "beginning imageID = ", imageID
            print "sleep interval between commands = ", sleepInterval, "seconds"
            print "number of visits to perform = ",visits
            print

        _visitID = visitID
        _imageID = imageID
        for visit in range(0, visits):
            print "executing visit:",visit
            self.sendNextVisit(verbose, _visitID, exposures, boresight, filterID)
            time.sleep(sleepInterval)
            for expo in range(0, exposures):
                self.sendStartIntegration(verbose, _visitID, expo)
                time.sleep(sleepInterval)
                self.sendStartReadout(verbose, _imageID, _visitID, expo)
                time.sleep(sleepInterval)
                _imageID = _imageID+1
            _visitID = _visitID+1

if __name__ == "__main__":

    basename = os.path.basename(sys.argv[0])

    parser = argparse.ArgumentParser(prog=basename)


    parser.add_argument("-n", "--exposures", type=int, action="store", help="number of exposures", default=2)

    boresight="18,18"

    parser.add_argument("-F", "--filterID", type=str, action="store", help="image filter id", default="u")

    parser.add_argument("-I", "--visitID", type=int, action="store", help="visit id", default=1000)

    parser.add_argument("-i", "--imageID", type=int, action="store", help="image id", default=10000)

    parser.add_argument("-s", "--sleepInterval", type=int, action="store", help="interval to sleep between commands (in seconds)", default=60)

    parser.add_argument("-v", "--visits", type=int, action="store", help="number of visits to run", default=1)
    parser.add_argument("-V", "--verbose", action="store_true", help="turn on verbosity to see what's happening")


    args = parser.parse_args()

    auto = AutomatedOCS()
    auto.begin(args.verbose, args.exposures, boresight, args.filterID, args.visitID, args.imageID, args.sleepInterval, args.visits)
