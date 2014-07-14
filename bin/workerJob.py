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
import json
import socket
import lsst.ctrl.events as events
from lsst.daf.base import PropertySet
from lsst.ctrl.ap.jsonSocket import JSONSocket
from lsst.pex.logging import Log
from tempfile import NamedTemporaryFile

class WorkerJob(object):

    def __init__(self, host, port, visitID, exposures, boresight, filterID, raft, ccd):
        print "worker startd"
        self.host = host
        self.port = port
        self.visitID = visitID
        self.exposures = exposures
        self.boresight = boresight
        self.filterID = filterID
        self.raft = raft
        self.ccd = ccd

    def requestDistributor(self, exposureSequenceID):
        sock = self.makeConnection(self.host, self.port)

        jsock = JSONSocket(sock)

        # send a message to the Archive DMCS, requesting the host and port
        # of the correct distributor
        vals = {"msgtype":"worker job", "request":"distributor", "visitID":self.visitID, "raft":self.raft, "ccd":self.ccd, "exposureSequenceID":exposureSequenceID}

        print "vals = ",vals
        jsock.sendJSON(vals)

        # wait for a response from the Archive DMCS, which is the host and port
        resp = jsock.recvJSON()
        print "worker response received; resp =", resp
        
        return resp["inetaddr"],resp["port"]
        
    def makeConnection(self, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((host, port))
        except socket.gaierror, err:
            print "address problem?"
            return False
        except socket.error, err:
            print "connection problem: %s" % err
            return False
        print "connection successful!"
        return sock

    def retrieveDistributorImage(self, host, port, exposure):
        print "retriving image from %s:%d" % (host, port)
        sock = self.makeConnection(host, port)
        jsock = JSONSocket(sock)
        vals = {"msgtype":"worker job", "request":"file", "visitID":self.visitID, "raft":self.raft, "exposureSequenceID":exposure}
        print "sending request"
        jsock.sendJSON(vals)
        print "sent request; attempting to receive file"
        newName = "%s_%s_%s_%s" % (self.visitID, exposure, self.raft, self.ccd)
        newName = os.path.join("/tmp",newName)
        name = jsock.recvFile(receiveTo=newName)
        print "file received = %s" % name
        return name, "telemetry"
        
    def execute(self):
        print "execution"
        # TODO: do these next two lines twice
        for exposure in range (0, self.exposures):
            print "getting exposure %d" % exposure
            distHost, distPort = self.requestDistributor(exposure)
            print "distHost = %s:%s, exposure = %d" % (distHost, int(distPort), exposure)
            image, telemetry = self.retrieveDistributorImage(distHost, int(distPort), exposure)

        print "Perform alert production:"
        print "generating DIASources"
        print "updating  DIAObjects"
        print "issuing alerts"
        print "alert production ends"
        sys.exit(0)

if __name__ == "__main__":
    basename = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=basename)
    parser.add_argument("-I", "--visitID", type=str, action="store", help="visit id", required=True)
    parser.add_argument("-n", "--exposures", type=int, action="store", help="number of exposures", required=True)
    parser.add_argument("-b", "--boresight", type=str, action="store", help="boresight pointing", required=True)
    parser.add_argument("-F", "--filterID", type=str, action="store", help="filter id", required=True)
    parser.add_argument("-r", "--raft", type=str, action="store", help="raft id", required=True)
    parser.add_argument("-c", "--ccd", type=str, action="store", help="ccd #", required=True)
    parser.add_argument("-H", "--host", type=str, action="store", help="archive DMCS host", required=True)
    parser.add_argument("-P", "--port", type=int, action="store", help="archive DMCS port", required=True)
    
    args = parser.parse_args()
    job = WorkerJob(args.host, args.port, args.visitID, args.exposures, args.boresight, args.filterID, args.raft, args.ccd)
    job.execute()
