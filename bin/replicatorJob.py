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

    def __init__(self, rPort, raft, expectedSequenceTag, expectedExpSeqID):
        super(ReplicatorJob, self).__init__(raft, expectedSequenceTag, expectedExpSeqID)
        self.replicatorPort = rPort
        self.rSock = None
        

    def createAndConnect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print "connect to replicator @ %s:%d" % ("localhost", self.replicatorPort)
        try:
            sock.connect(hostPort)
        except socket.gaierror, err:
            print "address problem?  %s " % err
            return None
        except socket.error, err:
            print "Connection problem: %s" % err
            return None
        return sock

    def sendInfo(self, imageID, sequenceTag, raft):
        s = "%s,%s,%s" % (imageID, sequenceTag, raft)
        print "sending = %s" % s
        # send this info to the distributor
        self.dSock.send(s)
        # TODO Check return status

    def execute(self, imageID, sequenceTag, exposureSequenceID):
        print "sending %s info for image id = %s, sequenceTag = %s, exposureSequenceID = %s" % (self.distributor, imageID, sequenceTag, exposureSequenceID)
        if self.connectToReplicator():
            print "sending info to distributor"
            self.sendInfo(imageID, sequenceTag, exposureSequenceID)
        else:
            print "not sending"
            # handle not being able to connect to the distributor
            pass

if __name__ == "__main__":
    basename = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=basename)
    parser.add_argument("-R", "--replicatorPort", type=int, action="store", help="replicator port to connect to", required=True)
    parser.add_argument("-r", "--raft", type=int, action="store", help="raft number", required=True)
    parser.add_argument("-t", "--sequenceTag", type=int, action="store", help="sequence Tag", required=True)
    parser.add_argument("-x", "--exposureSequenceID", type=int, action="store", help="exposure sequence id", required=True)

    args = parser.parse_args()
    base = ReplicatorJob(args.replicatorPort, args.raft, args.sequenceTag, args.exposureSequenceID)
    base.handleEvents()
