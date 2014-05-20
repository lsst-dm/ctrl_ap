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
from lsst.ctrl.ap.job import Job
from lsst.pex.logging import Log
from tempfile import NamedTemporaryFile

class ReplicatorJob(Job):

    def __init__(self, rPort, raft, expectedSequenceTag, expectedExpSeqID):
        super(ReplicatorJob, self).__init__(raft, expectedSequenceTag, expectedExpSeqID)
        jobnum = os.getenv("_CONDOR_SLOT","slot0")
        self.replicatorPort = rPort+int(jobnum[4:])
        
        self.rSock = None

        logger = Log.getDefaultLog()
        self.logger = Log(logger, "replicatorJob")
        

    def connectToReplicator(self):
        self.rSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.logger.log(Log.INFO, "connect to replicator @ %s:%d" % ("localhost", self.replicatorPort))
        try:
            self.rSock.connect(("localhost", self.replicatorPort))
        except socket.gaierror, err:
            self.logger.log(Log.INFO, "address problem?  %s " % err)
            return False
        except socket.error, err:
            self.logger.log(Log.INFO, "Connection problem: %s" % err)
            return False
        return True

    def sendInfo(self, imageID, sequenceTag, raft):
        vals = {"imageID" : int(imageID), "sequenceTag": int(sequenceTag), "raft" : int(raft)}
        s = json.dumps(vals)
        self.logger.log(Log.INFO, "sending = %s" % s)
        # send this info to the distributor

        self.rSock.send(s)
        # TODO Check return status

    def execute(self, imageID, sequenceTag, exposureSequenceID):
        self.logger.log(Log.INFO, "info for image id = %s, sequenceTag = %s, exposureSequenceID = %s" % (imageID, sequenceTag, exposureSequenceID))
        if self.connectToReplicator():
            self.logger.log(Log.INFO, "sending info to replicator")
            self.sendInfo(imageID, sequenceTag, exposureSequenceID)
        else:
            self.logger.log(Log.INFO, "not sending")
            # handle not being able to connect to the distributor
            pass

        #here = os.getcwd()
        #os.chdir("/tmp")
        #print "was here = %s" % here
        #print "now here = %s" % os.getcwd()
        # write out a temporary, but big, file
        f = NamedTemporaryFile(delete=False, dir="/tmp")
        f.write(os.urandom(1024*1024))
        f.close()
        self.logger.log(Log.INFO, "file created is named %s" % f.name)
        #os.chdir(here)

        # send the replicator node the name of the file
        vals = {"filename" : f.name}
        info = json.dumps(vals)
        self.rSock.send(info)

if __name__ == "__main__":
    basename = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=basename)
    parser.add_argument("-R", "--replicatorPort", type=int, action="store", help="base replicator port (plus slot #) to connect to", required=True)
    parser.add_argument("-r", "--raft", type=int, action="store", help="raft number", required=True)
    parser.add_argument("-t", "--sequenceTag", type=int, action="store", help="sequence Tag", required=True)
    parser.add_argument("-x", "--exposureSequenceID", type=int, action="store", help="exposure sequence id", required=True)

    args = parser.parse_args()
    base = ReplicatorJob(args.replicatorPort, args.raft, args.sequenceTag, args.exposureSequenceID)
    base.handleEvents()
