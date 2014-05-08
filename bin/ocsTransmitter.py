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

class OCSTransmitter(object):

    def __init__(self):
        """ construct the OCS object
        @param basename the base name of this command
        """
        self.ocs = ocs.OCS()

    def parseArgs(self, basename):
        """
        Parse command line arguments
        @param basename the base name of this command
        @return: the parser options and arguments
        """

        parser = argparse.ArgumentParser(prog=basename)
        #parser.add_argument("-s", "--startIntegration", action="store_true", help="startIntegration", required=False)
        
        subparsers = parser.add_subparsers(dest="cmd", help="send command from simulated OCS")
        parser_a = subparsers.add_parser("startIntegration")
        parser_a.add_argument("-s", "--sequenceTag", type=str, action="store", help="sequence tag", required=True)
        parser_a.add_argument("-x", "--integrationIndex", type=int, action="store", help="integration index", required=True)

        parser_b = subparsers.add_parser("startReadout")
        parser_b.add_argument("-i", "--imageID", type=int, action="store", help="image id", required=True)
        parser_b.add_argument("-t", "--sequenceTag", type=int, action="store", help="sequence Tag", required=True)
        parser_b.add_argument("-x", "--exposureSequenceID", type=int, action="store", help="exposure sequence id", required=True)


        return  parser.parse_args()

    def sendStartIntegration(self, sequenceTag, integrationIndex):
        self.ocs.sendStartIntegration(sequenceTag, integrationIndex)

    def sendStartReadout(self, imageID, sequenceTag, exposureSequenceID):
        self.ocs.sendStartReadout(imageID, sequenceTag, exposureSequenceID)


if __name__ == "__main__":
    ocsT = OCSTransmitter()

    basename = os.path.basename(sys.argv[0])

    args = ocsT.parseArgs(basename)

    if args.cmd == "startIntegration":
        ocsT.sendStartIntegration(args.sequenceTag, args.integrationIndex)
    elif args.cmd == "startReadout":
        ocsT.sendStartReadout(args.imageID, args.sequenceTag, args.exposureSequenceID)
