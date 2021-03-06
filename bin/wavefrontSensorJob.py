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
import os
import sys
import argparse
import socket
import errno
from lsst.ctrl.ap.jsonSocket import JSONSocket
from lsst.ctrl.ap.status import Status
from lsst.ctrl.ap.fileTransfer import FileTransfer
from lsst.ctrl.ap.socketFileTransfer import SocketFileTransfer
import lsst.log as log

class WavefrontSensorJob(object):

    def __init__(self, host, port, visitID, exposures, boresight, filterID, raft, ccd):
        self.host = host
        self.port = port
        self.visitID = visitID
        self.exposures = exposures
        self.boresight = boresight
        self.filterID = filterID
        self.raft = raft
        self.ccd = ccd

        log.configure()
        log.info("wavefront sensor worker started")


        # We don't know  which slot or host we're
        # running in on condor before it's assigned.
        # This is a hack to use the slot number and
        # the host number to get that info.
        slotsPerHost = 13
        jobnum = os.getenv("_CONDOR_SLOT","slot0");
        thishost = socket.gethostname()
        hostnum = int(thishost[len("lsst-run"):][:-len(".ncsa.illinois.edu")])
        self.workerID = (hostnum-1)*slotsPerHost+int(jobnum[4:])+1

        st = Status()
        data = {"workerID":self.workerID, "data":{"visitID":visitID,"raft":raft,"sensor":ccd}}
        st.publish(st.wavefrontSensorJob, st.start, data)

    def requestDistributor(self, exposureSequenceID):
        sock = self.makeConnection(self.host, self.port)

        jsock = JSONSocket(sock)

        log.info("worker sending request to Archive for distributor info")
        # send a message to the Archive DMCS, requesting the host and port
        # of the correct distributor
        vals = {"msgtype":"worker job", "request":"distributor", "visitID":self.visitID, "raft":self.raft, "ccd":self.ccd, "exposureSequenceID":exposureSequenceID}
        print 'sending ',vals

        jsock.sendJSON(vals)

        st = Status()
        data = {st.data:{"visitID":self.visitID, "raft":self.raft, "ccd":self.ccd, "exposureSequenceID":exposureSequenceID}}
        st.publish(st.wavefrontSensorJob, st.pub, data)

        # wait for a response from the Archive DMCS, which is the host and port
        resp = jsock.recvJSON()
        log.info("worker from response received")

        st.publish(st.wavefrontSensorJob, st.infoReceived, data)
        return resp["inetaddr"],resp["port"]
        
    def makeConnection(self, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((host, port))
        except socket.gaierror, err:
            log.info("address problem?")
            return None
        except socket.error, err:
            log.info("connection problem: %s" % err)
            return None
        return sock

    def retrieveDistributorImage(self, host, port, exposure):
        log.info("retriving image from distributor")
        st = Status()
        connection = {st.server:{st.host:host, st.port:port}}
        st.publish(st.wavefrontSensorJob, st.connect, connection)
        print "host = %s, port = %d" % (host, port)
        sock = self.makeConnection(host, port)
        jsock = JSONSocket(sock)
        fileTransfer = FileTransfer(SocketFileTransfer(socket))
        vals = {"msgtype":"worker job", "request":"file", "visitID":self.visitID, "raft":self.raft, "exposureSequenceID":exposure, "sensor":self.ccd}
        jsock.sendJSON(vals)
        data = {"visitID":self.visitID, "raft":self.raft, "exposureSequenceID":exposure, "sensor":self.ccd}
        st.publish(st.wavefrontSensorJob, st.retrieve, data)
        print "wavefrontSensorJob vals = ",vals
        newName = "lsst/%s/%s/%s_%s" % (self.visitID, exposure, self.raft, self.ccd)
        newName = os.path.join("/tmp",newName)
        self.safemakedirs(os.path.dirname(newName))
        name = fileTransfer.receive(newName)
        data["file"] = name
        st.publish(st.wavefrontSensorJob, st.fileReceived, data)
        log.info("file received = %s" % name)
        return name, "telemetry"

    # when this goes to python 3.2, we can use exist_ok, but
    # until then, we ignore the fact that someone got there before us.
    def safemakedirs(self, path):
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

        st.publish(st.wavefrontSensorJob, st.generate, "DIASources")
        st.publish(st.wavefrontSensorJob, st.update, "DIAObjects")
        st.publish(st.wavefrontSensorJob, st.issue, "alerts")
        st.publish(st.wavefrontSensorJob, st.finish, st.success)
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
    job = WavefrontSensorJob(args.host, args.port, args.visitID, args.exposures, args.boresight, args.filterID, args.raft, args.ccd)
    job.execute()
