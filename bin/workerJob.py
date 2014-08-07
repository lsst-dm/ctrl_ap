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
from lsst.ctrl.ap.status import Status
from lsst.pex.logging import Log
from tempfile import NamedTemporaryFile

class WorkerJob(object):

    def __init__(self, host, port, visitID, exposures, boresight, filterID, raft, ccd):
        self.host = host
        self.port = port
        self.visitID = visitID
        self.exposures = exposures
        self.boresight = boresight
        self.filterID = filterID
        self.raft = raft
        self.ccd = ccd

        logger = Log.getDefaultLog()
        self.logger = Log(logger, "workerJob")
        self.logger.log(Log.INFO, "worker started")

        # We don't know  which slot or host we're
        # running in on condor before it's assigned.
        # This is a hack to use the slot number and
        # the host number to get that info.
        slotsPerHost = 13
        jobnum = os.getenv("_CONDOR_SLOT","slot0");
        thishost = socket.gethostname();
        hostnum = int(thishost[len("lsst-run"):][:-len(".ncsa.illinois.edu")])
        self.workerID = (hostnum-1)*slotsPerHost+int(jobnum[4:])

        st = Status()
        data = {"workerID":self.workerID, "data":{"visitID":visitID,"raft":raft,"sensor":ccd}}
        st.publish(st.workerJob, st.start, data)

    def requestDistributor(self, exposureSequenceID):
        st = Status()
        server = {st.server:{st.host:self.host,st.port:self.port}}
        st.publish(st.workerJob, st.connect, server)
        sock = self.makeConnection(self.host, self.port)

        jsock = JSONSocket(sock)

        self.logger.log(Log.INFO, "worker sending request to Archive for distributor info")
        # send a message to the Archive DMCS, requesting the host and port
        # of the correct distributor
        vals = {"msgtype":"worker job", "request":"distributor", "visitID":self.visitID, "raft":self.raft, "ccd":self.ccd, "exposureSequenceID":exposureSequenceID}

        jsock.sendJSON(vals)

        data = {st.data:{"visitID":self.visitID, "raft":self.raft, "ccd":self.ccd, "exposureSequenceID":exposureSequenceID}}
        st.publish(st.workerJob, st.pub, data)
        # wait for a response from the Archive DMCS, which is the host and port
        resp = jsock.recvJSON()
        self.logger.log(Log.INFO, "worker from response received")
        
        st.publish(st.workerJob, st.infoReceived, data)
        return resp["inetaddr"],resp["port"]
        
    def makeConnection(self, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((host, port))
        except socket.gaierror, err:
            self.logger.log(Log.INFO, "address problem?")
            return False
        except socket.error, err:
            self.logger.log(Log.INFO, "connection problem: %s" % err)
            return False
        return sock

    def retrieveDistributorImage(self, host, port, exposure):
        self.logger.log(Log.INFO, "retriving image from distributor")
        st = Status()
        connection = {st.server:{st.host:host, st.port:port}}
        st.publish(st.workerJob, st.connect, connection)
        sock = self.makeConnection(host, port)
        jsock = JSONSocket(sock)
        vals = {"msgtype":"worker job", "request":"file", "visitID":self.visitID, "raft":self.raft, "exposureSequenceID":exposure, "sensor":self.ccd}
        jsock.sendJSON(vals)
        data = {"visitID":self.visitID, "raft":self.raft, "exposureSequenceID":exposure, "sensor":self.ccd}
        st.publish(st.workerJob, st.retrieve, data)
        newName = "lsst/%s/%s/%s_%s" % (self.visitID, exposure, self.raft, self.ccd)
        newName = os.path.join("/tmp",newName)
        self.safemakedirs(os.path.dirname(newName))
        #st.publish(st.workerJob, st.requestFile, newName)
        name = jsock.recvFile(receiveTo=newName)
        data["file"] = name;
        st.publish(st.workerJob, st.fileReceived, data)
        self.logger.log(Log.INFO, "file received = %s" % name)
        return name, "telemetry"

    # when this goes to python 3.2, we can use exist_ok, but
    # until then, we ignore the fact that someone got there before us.
    def safemakedirs(path):
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else: raise
        
    def execute(self):
        st = Status()
        for exposure in range (0, self.exposures):
            distHost, distPort = self.requestDistributor(exposure)
            image, telemetry = self.retrieveDistributorImage(distHost, int(distPort), exposure)
            data = {"workerID":self.workerID, "data":{"exposureSequenceID":exposure, "visitID":self.visitID,"raft":self.raft,"sensor":self.ccd}}
            time.sleep(2);
            st.publish(st.workerJob, st.perform, data)
            time.sleep(5);
            st.publish(st.workerJob, st.completed, data)

        st.publish(st.workerJob, st.generate, "DIASources")
        st.publish(st.workerJob, st.update, "DIAObjects")
        st.publish(st.workerJob, st.issue, "alerts")
        st.publish(st.workerJob, st.finish, st.success)
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
