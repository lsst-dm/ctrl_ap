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

    def __init__(self, visitID, exposures, boresight, filterID, raft, ccd):
        print "worker startd"
        self.visitID = visitID
        self.exposures = exposures
        self.boresight = boresight
        self.filterID = filterID
        self.raft = raft
        self.ccd = ccd

    def requestDistributor(self, host, port):
        sock = self.connectToArchiveDMCS(host, port)

        jsock = JSONSocket(sock)

        # XX Placeholder for exposureSequenceID

        vals = {"visitID":self.visitID, "raft":self.raft, "ccd":self.ccd, "exposureSequenceID":101}

        print "vals = ",vals
        jsock.sendJSON(vals)

        resp = jsock.recvJSON()
        
        return resp
        
    def connectToArchiveDMCS(self, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((host, port))
        except socket.gaierror, err:
            print "address problem?"
            return False
        except socket.error, err:
            print "connection problem: %s" % err
            return False
        return sock

    def connectToDistributor(self):
        return True

    def sendInfoToDistributor(self):
        return True

    def retrieveDistributorImage(self, host, port):
        

    def execute(self, host, port):
        # TODO: do these next two lines twice
        distHost, distPort = self.requestDistributor(host, port)
        image, telemetry = self.retrieveDistributorImage(distHost, distPort)

        print "Perform alert production:"
        print "generating DIASources"
        print "updating  DIAObjects"
        print "issuing alerts"
        print "alert production ends"
        sys.exit(0)

if __name__ == "__main__":
    basename = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=basename)
    parser.add_argument("-I", "--visitID", type=int, action="store", help="visit id", required=True)
    parser.add_argument("-n", "--exposures", type=int, action="store", help="number of exposures", required=True)
    parser.add_argument("-b", "--boresight", type=str, action="store", help="boresight pointing", required=True)
    parser.add_argument("-F", "--filterID", type=str, action="store", help="filter id", required=True)
    parser.add_argument("-r", "--raft", type=str, action="store", help="raft id", required=True)
    parser.add_argument("-c", "--ccd", type=str, action="store", help="ccd #", required=True)
    parser.add_argument("-H", "--host", type=str, action="store", help="archive DMCS host", required=True)
    parser.add_argument("-P", "--port", type=int, action="store", help="archive DMCS port", required=True)
    
    args = parser.parse_args()
    job = WorkerJob(args.visitID, args.exposures, args.boresight, args.filterID, args.raft, args.ccd)
    job.execute(args.host, args.port)
