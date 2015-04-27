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
from lsst.ctrl.ap.jsonSocket import JSONSocket
from lsst.ctrl.ap.status import Status
from lsst.ctrl.ap.terminator import Terminator
import lsst.log as log

class ReplicatorJob(object):

    def __init__(self, timeout, rPort, raft, expectedVisitID, expectedExpSeqID):
        jobnum = os.getenv("_CONDOR_SLOT","slot0")
        self.replicatorPort = rPort+int(jobnum[4:])
        
        self.filesize = 1024

        self.timeout = timeout
        self.rSock = None
        # TODO:  these need to be placed in a configuration file
        # which is loaded, so they are not embedded in the code
        self.brokerName = "lsst8.ncsa.illinois.edu"
        self.eventTopic = "ocs_startReadout"
        self.raft = raft
        self.expectedVisitID = expectedVisitID
        self.expectedExpSeqID = expectedExpSeqID

        log.configure()
        st = Status()
        vals = {"replicatorHost":socket.gethostname(), "replicatorPort":self.replicatorPort, "startupArgs":{"visitID":expectedVisitID, "exposureSequenceID":expectedExpSeqID, "raft":self.raft}}
        st.publish(st.replicatorJob, st.start, vals)
        log.info("visitID = %s"%str(expectedVisitID))
        log.info("expectedSequenceID = %s"%str(expectedExpSeqID))
        

    def connectToReplicator(self):
        rSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host = socket.gethostname()
        st = Status()
        serv = {st.server:{st.host:host, st.port:self.replicatorPort}}
        st.publish(st.replicatorJob, st.connect, serv)
        log.info("connect to replicator @ %s:%d" % (host, self.replicatorPort))
        try:
            rSock.connect((host, self.replicatorPort))
        except socket.gaierror, err:
            log.info("address problem?  %s " % err)
            return False
        except socket.error, err:
            log.info("Connection problem: %s" % err)
            log.info("I'm on host: %s" % host)
            return False
        self.rSock = JSONSocket(rSock)
        return True

    def sendInfoToReplicator(self):
        term = Terminator("info to replicator", self.timeout)
        term.start()
        
        while self.connectToReplicator() == False:
            log.info("retrying")
            time.sleep(1)
            # handle not being able to connect to the distributor
            pass
        term.cancel()
        

        log.info("sending info to replicator node")
        vals = {"msgtype":"replicator job", "request":"info post", "visitID" : int(self.expectedVisitID), "exposureSequenceID": int(self.expectedExpSeqID), "raft" : self.raft}
        # send this info to the distributor, via the replicator
        self.rSock.sendJSON(vals)
        st = Status()
        data = {st.data:{"visitID" : int(self.expectedVisitID), "exposureSequenceID": int(self.expectedExpSeqID), "raft" : self.raft}}
        st.publish(st.replicatorJob, st.inform, data)

    def execute(self, imageID, visitID, exposureSequenceID):
        imageSize = 3200000000
        raftSize = imageSize/21
        ccdSize = raftSize/9
        log.info("info for image id = %s, visitID = %s, exposureSequenceID = %s" % (imageID, visitID, exposureSequenceID))

        # artificial wait to simulate some processing going on.
        #time.sleep(2)
        data = {"data":{"visitID":visitID,"exposureSequenceID":exposureSequenceID,"raft":self.raft}}
        st = Status()
        st.publish(st.replicatorJob, st.read, data)

        # write a random binary file to disk
        tmp = "%s_%s_%s_%s" % (self.raft, imageID, visitID, exposureSequenceID)
        tmp = os.path.join("/tmp",tmp)
        f = open(tmp, "wb")
        size = self.filesize*9 # nine self.filesize images into one file
        f.write(os.urandom(size))
        f.close()
        log.info("file created is named %s" % f.name)

        st.publish(st.replicatorJob, st.fileReceived, {"fileinfo":{"filename":f.name, "size":size}})

        # send the replicator node the name of the file
        vals = {"msgtype":"replicator job", "request":"upload", "filename" : f.name, "visitID": visitID, "exposureSequenceID":exposureSequenceID, "raft":self.raft}
        self.rSock.sendJSON(vals)
        st.publish(st.replicatorJob, st.upload, f.name)

    def begin(self):
        self.sendInfoToReplicator()
        eventSystem = events.EventSystem.getDefaultEventSystem()
        eventSystem.createReceiver(self.brokerName, self.eventTopic)
        st = Status()
        # loop until you get the right thing, process and then die.
        term = Terminator("execute", self.timeout)
        term.start()
        while True:
            ts = time.time()
            log.info(datetime.datetime.fromtimestamp(ts).strftime('listening for events: %Y-%m-%d %H:%M:%S'))
            st.publish(st.replicatorJob, "waiting", {"eventTopic":self.eventTopic})
            ocsEvent = eventSystem.receiveEvent(self.eventTopic)
            ps = ocsEvent.getPropertySet()
            imageID = ps.get("imageID")
            # TODO:  for now, assume visit id, and exp. seq. id is also sent
            visitID = ps.get("visitID")
            exposureSequenceID = ps.get("exposureSequenceID")
            log.info("image id = %s" % imageID)
            log.info("sequence tag = %s" % visitID)
            log.info("exposure sequence id = %s" % exposureSequenceID)
            data = {"visitID":visitID, "exposureSequenceID":exposureSequenceID, "imageID":imageID}
            st.publish(st.replicatorJob, st.receivedMsg, {st.startReadout:data})
            # NOTE:  While should be done through a selector on the broker
            # so we only get the visitID and exp seq ID we are looking
            # for, DM Messages are not the ultimate way we'll be receiving
            # this info. we'll be using the DDS OCS messages, so this is good
            # for now.
            if visitID == self.expectedVisitID and exposureSequenceID == self.expectedExpSeqID:
                term.cancel()
                log.info("got expected info.  Getting image")
                log.info(datetime.datetime.fromtimestamp(ts).strftime('start execute: %Y-%m-%d %H:%M:%S'))
                self.execute(imageID, visitID, exposureSequenceID)
                log.info(datetime.datetime.fromtimestamp(ts).strftime('done execute: %Y-%m-%d %H:%M:%S'))
                st = Status()
                st.publish(st.replicatorJob, st.finish, st.success)
                log.info(datetime.datetime.fromtimestamp(ts).strftime('finished: %Y-%m-%d %H:%M:%S'))
                sys.exit(0)
            else:
                log.info("did not get expected info!")

if __name__ == "__main__":
    basename = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=basename)
    parser.add_argument("-R", "--replicatorPort", type=int, action="store", help="base replicator port (plus slot #) to connect to", required=True)
    parser.add_argument("-r", "--raft", type=str, action="store", help="raft", required=True)
    parser.add_argument("-I", "--visitID", type=int, action="store", help="visitID", required=True)
    parser.add_argument("-x", "--exposureSequenceID", type=int, action="store", help="exposure sequence id", required=True)
    parser.add_argument("-t", "--timeout", type=int, action="store", help="ccd #", default=120, required=False)

    args = parser.parse_args()
    base = ReplicatorJob(args.timeout, args.replicatorPort, args.raft, args.visitID, args.exposureSequenceID)
    base.begin()
