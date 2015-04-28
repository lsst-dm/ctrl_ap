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
import lsst.log as log
from lsst.ctrl.ap.terminator import Terminator

class WavefrontJob(object):

    def __init__(self, timeout, rPort, expectedVisitID, expectedExpSeqID):
        self.timeout = timeout
        jobnum = os.getenv("_CONDOR_SLOT","slot0")
        self.replicatorPort = rPort+int(jobnum[4:])
        
        self.rSock = None
        # TODO:  these need to be placed in a configuration file
        # which is loaded, so they are not embedded in the code
        self.brokerName = "lsst8.ncsa.illinois.edu"
        self.eventTopic = "ocs_startReadout"
        self.expectedVisitID = expectedVisitID
        self.expectedExpSeqID = expectedExpSeqID

        log.configure()
        # We don't know  which slot or host we're
        # running in on condor before it's assigned.
        # This is a hack to use the slot number and
        # the host number to get that info.
        slotsPerHost = 13
        jobnum = os.getenv("_CONDOR_SLOT","slot0");
        thishost = socket.gethostname();
        hostnum = int(thishost[len("lsst-run"):][:-len(".ncsa.illinois.edu")])
        workerID = (hostnum-1)*slotsPerHost+int(jobnum[4:])+1

        vals = {"workerID":workerID, "replicatorHost":socket.gethostname(), "replicatorPort":self.replicatorPort, "startupArgs":{"visitID":expectedVisitID, "exposureSequenceID":expectedExpSeqID, "raft":"wave"}}
        st = Status()
        st.publish(st.wavefrontJob, st.start, vals)
        

    def connectToReplicator(self):
        rSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host = socket.gethostname()
        st = Status()
        serv = {st.server:{st.host:host, st.port:self.replicatorPort}}
        st.publish(st.wavefrontJob, st.connect, serv)
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

    def sendInfoToReplicator(self, raft):
        term = Terminator("to replicator", self.timeout)
        term.start()
        if self.connectToReplicator() == False:
            log.info("not sending")
            # handle not being able to connect to the distributor
            pass
        term.cancel()

        log.info("sending info to replicator node")
        vals = {"msgtype":"wavefront job", "request":"info post", "visitID" : int(self.expectedVisitID), "exposureSequenceID": int(self.expectedExpSeqID), "raft" : raft}
        # send this info to the distributor, via the replicator
        self.rSock.sendJSON(vals)
        st = Status()
        data = {st.data:{"visitID" : int(self.expectedVisitID), "exposureSequenceID": int(self.expectedExpSeqID), "raft" : raft}}
        st.publish(st.wavefrontJob, st.pub, data)

#    def execute(self, imageID, visitID, exposureSequenceID, raft):
#        log.info("info for image id = %s, visitID = %s, exposureSequenceID = %s" % (imageID, visitID, exposureSequenceID))
#
#        # artificial wait to simulate some processing going on.
#        time.sleep(2)
#        data = {"data":{"visitID":visitID,"exposureSequenceID":exposureSequenceID,"raft":raft}}
#        st = Status()
#        st.publish(st.wavefrontJob, st.read, data)
#
#        # write a random binary file to disk
#        tmp = "%s_%s_%s_%s" % (raft, imageID, visitID, exposureSequenceID)
#        tmp = os.path.join("/tmp",tmp)
#        f = open(tmp, "wb")
#        f.write(os.urandom(1024*200*4))
#        f.close()
#        log.info("file created is named %s" % f.name)
#
#        # send the replicator node the name of the file
#        vals = {"msgtype":"wavefront job", "request":"upload", "filename" : f.name, "visitID":visitID, "exposureSequenceID":exposureSequenceID, "raft":raft}
#        self.rSock.sendJSON(vals)
#        st.publish(st.wavefrontJob, st.upload, f.name)



    def execute(self):
        raft = "wave"
        self.sendInfoToReplicator(raft)
        eventSystem = events.EventSystem.getDefaultEventSystem()
        eventSystem.createReceiver(self.brokerName, self.eventTopic)
        # loop until you get the right thing, process and then die.
        term = Terminator("start", self.timeout)
        term.start()
        while True:
            ts = time.time()
            log.info(datetime.datetime.fromtimestamp(ts).strftime('listening for events: %Y-%m-%d %H:%M:%S'))
            ocsEvent = eventSystem.receiveEvent(self.eventTopic)
            ps = ocsEvent.getPropertySet()
            imageID = ps.get("imageID")
            visitID = ps.get("visitID")
            exposureSequenceID = ps.get("exposureSequenceID")
            log.info("image id = %s" % imageID)
            log.info("sequence tag = %s" % visitID)
            log.info("exposure sequence id = %s" % exposureSequenceID)
            # NOTE:  While should be done through a selector on the broker
            # so we only get the visitID and exp seq ID we are looking
            # for, DM Messages are not the ultimate way we'll be receiving
            # this info. we'll be using the DDS OCS messages, so this is good
            # for now.
            if visitID == self.expectedVisitID and exposureSequenceID == self.expectedExpSeqID:
                term.cancel()
                log.info("got expected info.  Getting image")
                self.execute(imageID, visitID, exposureSequenceID, raft)
        while True:
            ts = time.time()
            log.info(datetime.datetime.fromtimestamp(ts).strftime('listening for events: %Y-%m-%d %H:%M:%S'))
            ocsEvent = eventSystem.receiveEvent(self.eventTopic)
            ps = ocsEvent.getPropertySet()
            imageID = ps.get("imageID")
            visitID = ps.get("visitID")
            exposureSequenceID = ps.get("exposureSequenceID")
            log.info("image id = %s" % imageID)
            log.info("sequence tag = %s" % visitID)
            log.info("exposure sequence id = %s" % exposureSequenceID)
            # NOTE:  While should be done through a selector on the broker
            # so we only get the visitID and exp seq ID we are looking
            # for, DM Messages are not the ultimate way we'll be receiving
            # this info. we'll be using the DDS OCS messages, so this is good
            # for now.
            if visitID == self.expectedVisitID and exposureSequenceID == self.expectedExpSeqID:
                term.cancel()
                log.info("got expected info.  Getting image")
                self.execute(imageID, visitID, exposureSequenceID, raft)
                st = Status() 
                st.publish(st.wavefrontJob, st.finish, st.success)
                sys.exit(0)

if __name__ == "__main__":
    basename = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=basename)
    parser.add_argument("-R", "--replicatorPort", type=int, action="store", help="base replicator port (plus slot #) to connect to", required=True)
    parser.add_argument("-I", "--visitID", type=int, action="store", help="visitID", required=True)
    parser.add_argument("-x", "--exposureSequenceID", type=int, action="store", help="exposure sequence id", required=True)
    parser.add_argument("-t", "--timeout", type=int, action="store", help="ccd #", default=120, required=False)

    args = parser.parse_args()
    base = WavefrontJob(args.timeout, args.replicatorPort, args.visitID, args.exposureSequenceID)
    base.execute()
