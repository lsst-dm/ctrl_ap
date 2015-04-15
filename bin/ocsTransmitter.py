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

class OCSTransmitter(object):

    def __init__(self):
        """ construct the OCS object
        @param basename the base name of this command
        """
        self.ocs = ocs.OCS()

    def sendStartIntegration(self, visitID, exposureSequenceID):
        self.ocs.sendStartIntegration(visitID, exposureSequenceID)

    def sendStartReadout(self, imageID, visitID, exposureSequenceID):
        self.ocs.sendStartReadout(imageID, visitID, exposureSequenceID)

    def sendNextVisit(self, visitID, exposures, boresight, filterID):
        self.ocs.sendNextVisit(visitID, exposures, boresight, filterID)

if __name__ == "__main__":
    ocsT = OCSTransmitter()

    basename = os.path.basename(sys.argv[0])

    parser = argparse.ArgumentParser(prog=basename)
    
    subparsers = parser.add_subparsers(dest="cmd", help="send command from simulated OCS")
    parser_a = subparsers.add_parser("startIntegration")
    parser_a.add_argument("-I", "--visitID", type=str, action="store", help="visit id", required=True)
    parser_a.add_argument("-x", "--exposureSequenceID", type=int, action="store", help="exposure sequence id", required=True)

    parser_b = subparsers.add_parser("startReadout")
    parser_b.add_argument("-i", "--imageID", type=int, action="store", help="image id", required=True)
    parser_b.add_argument("-I", "--visitID", type=int, action="store", help="visit id", required=True)
    parser_b.add_argument("-x", "--exposureSequenceID", type=int, action="store", help="exposure sequence id", required=True)

    parser_c = subparsers.add_parser("nextVisit")
    parser_c.add_argument("-I", "--visitID", type=str, action="store", help="visit id", required=True)
    parser_c.add_argument("-n", "--exposures", type=int, action="store", help="number of exposures", required=True)
    parser_c.add_argument("-b", "--boresight", type=str, action="store", help="boresight pointing", required=True)
    parser_c.add_argument("-F", "--filterID", type=str, action="store", help="filter id", required=True)

    args = parser.parse_args()

    st = Status()
    if args.cmd == "startIntegration":
        st.publish(st.ocs, st.sendMsg, {"cmd":args.cmd, "exposureSequenceID":args.exposureSequenceID, "visitID":args.visitID})
        ocsT.sendStartIntegration(args.visitID, args.exposureSequenceID)
    elif args.cmd == "startReadout":
        st.publish(st.ocs, st.sendMsg, {"cmd":args.cmd, "exposureSequenceID":args.exposureSequenceID, "imageID":args.imageID, "visitID":args.visitID})
        ocsT.sendStartReadout(args.imageID, args.visitID, args.exposureSequenceID)
    elif args.cmd == "nextVisit":
        st.publish(st.ocs, st.sendMsg, {"cmd":args.cmd, "exposures":args.exposures, "visitID":args.visitID, "filterID":args.filterID, "boresight":args.boresight})
        ocsT.sendNextVisit(args.visitID, args.exposures, args.boresight, args.filterID)
