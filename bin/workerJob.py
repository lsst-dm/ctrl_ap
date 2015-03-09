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
import errno
import lsst.ctrl.events as events
from lsst.daf.base import PropertySet
from lsst.ctrl.ap.jsonSocket import JSONSocket
from lsst.ctrl.ap.status import Status
from lsst.pex.logging import Log
from tempfile import NamedTemporaryFile
from lsst.ctrl.ap.dmcsHostConfig import ArchiveConfig
from lsst.ctrl.ap.terminator import Terminator

class WorkerJob(object):

    def __init__(self, archiveConfig, timeout, visitID, exposures, boresight, filterID, raft, ccd):
        self.timeout = timeout
        self.archiveConfig = archiveConfig
        self.host = archiveConfig.main.host
        self.port = archiveConfig.main.port
        self.visitID = visitID
        self.exposures = exposures
        self.boresight = boresight
        self.filterID = filterID
        self.raft = raft
        self.ccd = ccd
        self.connectionAttemptInterval = 2 # seconds

        logger = Log.getDefaultLog()
        self.logger = Log(logger, "workerJob")
        self.logger.log(Log.INFO, "visitID = %s" % str(visitID))
        self.logger.log(Log.INFO, "raft = %s" % str(raft))
        self.logger.log(Log.INFO, "ccd = %s" % str(ccd))
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
        try:
            st = Status()
            server = {st.server:{st.host:self.host,st.port:self.port}, st.failover:{st.host:self.archiveConfig.failover.host, st.port:self.archiveConfig.failover.port}}
            st.publish(st.workerJob, st.connect, server)
            # terminate this process if we haven't been able to connect within
            # self.timeout seconds
            term = Terminator(self.logger, "archive connection",  self.timeout)
            term.start()
            sock = self.makeArchiveConnection(self.archiveConfig)
            term.cancel()
    
            jsock = JSONSocket(sock)
    
            self.logger.log(Log.INFO, "worker sending request to Archive for distributor info")
            # send a message to the Archive DMCS, requesting the host and port
            # of the correct distributor
            vals = {"msgtype":"worker job", "request":"distributor", "visitID":self.visitID, "raft":self.raft, "ccd":self.ccd, "exposureSequenceID":exposureSequenceID}
    
            jsock.sendJSON(vals)
    
            data = {st.data:{"visitID":self.visitID, "raft":self.raft, "ccd":self.ccd, "exposureSequenceID":exposureSequenceID}}
            st.publish(st.workerJob, st.pub, data)
            self.logger.log(Log.INFO, "waiting for response from ArchiveDMCS")

            # terminate this process if we haven't received a response within 
            # self.timeout seconds
            term = Terminator(self.logger, "archive response", self.timeout)
            term.start()
            # wait for a response from the Archive DMCS, which is the host and port
            resp = jsock.recvJSON()
            term.cancel()
            self.logger.log(Log.INFO, "response received from ArchiveDMCS")
            
            st.publish(st.workerJob, st.infoReceived, data)
            return resp["inetaddr"],resp["port"]
        except Exception as exp:
            self.logger.log(Log.INFO, "exception from distributor")
            print exp
            return None,None

    def makeArchiveConnection(self, archiveConfig):
        while True:
            sock = self.makeConnection(archiveConfig.main.host, archiveConfig.main.port)
            if sock is not None:
                return sock
            sock = self.makeConnection(archiveConfig.failover.host, archiveConfig.failover.port)
            if sock is not None:
                return sock
            time.sleep(self.connectionAttemptInterval)
        
        
        
    def makeConnection(self, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((host, port))
        except socket.gaierror, err:
            self.logger.log(Log.INFO, "address problem?")
            sock = None
        except socket.error, err:
            self.logger.log(Log.INFO, "connection problem: host %s" % host)
            self.logger.log(Log.INFO, "connection problem: port %d" % port)
            self.logger.log(Log.INFO, "connection problem: %s" % err)
            sock = None
        return sock

    def retrieveDistributorImage(self, host, port, exposure):
        self.logger.log(Log.INFO, "retrieving image from distributor")
        st = Status()
        while True:
            try:
                connection = {st.server:{st.host:host, st.port:port}}
                st.publish(st.workerJob, st.connect, connection)
                self.logger.log(Log.INFO, "making connection")
                sock = self.makeConnection(host, port)
                self.logger.log(Log.INFO, "connection made")
                jsock = JSONSocket(sock)
                vals = {"msgtype":"worker job", "request":"file", "visitID":self.visitID, "raft":self.raft, "exposureSequenceID":exposure, "sensor":self.ccd}
                jsock.sendJSON(vals)
                data = {"visitID":self.visitID, "raft":self.raft, "exposureSequenceID":exposure, "sensor":self.ccd}
                st.publish(st.workerJob, st.retrieve, data)
                newName = "lsst/%s/%s/%s_%s" % (self.visitID, exposure, self.raft, self.ccd)
                newName = os.path.join("/tmp",newName)
                self.safemakedirs(os.path.dirname(newName))
                #st.publish(st.workerJob, st.requestFile, newName)
                self.logger.log(Log.INFO, "trying receive file sequence for %s "% newName)
                msg = jsock.recvJSON()
                if msg == None:
                    self.logger.log(Log.INFO, "distributor did not have file")
                    return None, None
                if msg["status"] == st.fileNotFound:
                    self.logger.log(Log.INFO, "distributor did not have file")
                    return None, None
                
                #for x in msg:
                #    self.logger.log(Log.INFO, "%s - %s " % (x,msg[x]))
                self.logger.log(Log.INFO, "now receiving file %s"  % newName)
                name = jsock.recvFile(newName)
                self.logger.log(Log.INFO, "file received %s" % newName)
                data["file"] = name;
                st.publish(st.workerJob, st.fileReceived, data)
                self.logger.log(Log.INFO, "file received = %s" % name)
                return name, "telemetry"
            except Exception, err:
                self.logger.log(Log.INFO, "exception %s " % err.message)
                self.logger.log(Log.INFO, "trying to get file, recv was empty")
                self.logger.log(Log.INFO, "trying again in %d seconds" % self.connectionAttemptInterval)
                jsock.close()
                time.sleep(self.connectionAttemptInterval)

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
        # start self termination thread

        st = Status()
        for exposure in range (0, self.exposures):
            distHost = None
            distPort = None
            while distHost is None:
                distHost, distPort = self.requestDistributor(exposure)
            # terminate this process if we haven't received Image
            # self.timeout seconds
            term = Terminator(self.logger, "execute", self.timeout)
            term.start()
            image, telemetry = self.retrieveDistributorImage(distHost, int(distPort), exposure)
            # we received something; cancel termination of this process
            term.cancel()
            data = {"workerID":self.workerID, "data":{"exposureSequenceID":exposure, "visitID":self.visitID,"raft":self.raft,"sensor":self.ccd}}
            time.sleep(1);
            st.publish(st.workerJob, st.perform, data)
            time.sleep(5);
            st.publish(st.workerJob, st.completed, data)

        st.publish(st.workerJob, st.generate, "DIASources")
        st.publish(st.workerJob, st.update, "DIAObjects")
        st.publish(st.workerJob, st.issue, "alerts")
        st.publish(st.workerJob, st.finish, st.success)
        sys.exit(0)

if __name__ == "__main__":
    apCtrlPath = os.getenv("CTRL_AP_DIR")
    archiveConfig = ArchiveConfig()
    subDirPath = os.path.join(apCtrlPath, "etc", "config", "archive.py")
    archiveConfig.load(subDirPath)

    basename = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=basename)
    parser.add_argument("-I", "--visitID", type=str, action="store", help="visit id", required=True)
    parser.add_argument("-n", "--exposures", type=int, action="store", help="number of exposures", required=True)
    parser.add_argument("-b", "--boresight", type=str, action="store", help="boresight pointing", required=True)
    parser.add_argument("-F", "--filterID", type=str, action="store", help="filter id", required=True)
    parser.add_argument("-r", "--raft", type=str, action="store", help="raft id", required=True)
    parser.add_argument("-c", "--ccd", type=str, action="store", help="ccd #", required=True)
    parser.add_argument("-t", "--timeout", type=int, action="store", help="ccd #", default=120, required=False)
    
    args = parser.parse_args()

    job = WorkerJob(archiveConfig, args.timeout, args.visitID, args.exposures, args.boresight, args.filterID, args.raft, args.ccd)
    job.execute()
