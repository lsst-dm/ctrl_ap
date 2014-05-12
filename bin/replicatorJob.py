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
import socket
import lsst.ctrl.events as events
from lsst.daf.base import PropertySet
from lsst.ctrl.ap.job import Job

class ReplicatorJob(Job):

    def connectToDistributor(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.distConnection = s.connect(self.distributor, self.distributorPort)
        if self.distConnection:
            return True
        return False

    def sendDistributorInfo(self, imageID, sequenceTag, raft):
        s = "%s,%s,%s" % (imageID, sequenceTag, raft)
        self.distConnection.send(s)
        # TODO Check return status


    def execute(self, imageID, sequenceTag, exposureSequenceID):
        print "sending %s info for image id = %s, sequenceTag = %s, exposureSequenceID = %s" % (self.distributor, imageID, sequenceTag, exposureSequenceID)
        if self.connectToDistributor():
            self.sendDistributorInfo(imageID, sequenceTag, exposureSequenceID)
        else:
            # handle not being able to connect to the distributor
            pass

if __name__ == "__main__":
    basename = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=basename)
    parser.add_argument("-d", "--distributor", type=str, action="store", help="distributor node to connect to", required=True)
    parser.add_argument("-r", "--raft", type=int, action="store", help="raft number", required=True)
    parser.add_argument("-t", "--sequenceTag", type=int, action="store", help="sequence Tag", required=True)
    parser.add_argument("-x", "--exposureSequenceID", type=int, action="store", help="exposure sequence id", required=True)

    args = parser.parse_args()
    base = ReplicatorJob(args.distributor, args.raft, args.sequenceTag, args.exposureSequenceID)
    base.handleEvents()
