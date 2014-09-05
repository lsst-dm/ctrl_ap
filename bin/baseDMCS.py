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


import os
import select
import socket
import sys
import threading
import time
import lsst.ctrl.events as events
from lsst.ctrl.ap import envString
from lsst.daf.base import PropertySet
from lsst.ctrl.ap import jobManager
from lsst.ctrl.ap.status import Status
from lsst.pex.logging import Log
from lsst.ctrl.ap.heartbeat import Heartbeat
from lsst.ctrl.ap.dmcsHostConfig import BaseConfig
from lsst.ctrl.ap.jsonSocket import JSONSocket

class BaseDMCS(object):

    def __init__(self, rHostList):
        # TODO:  these need to be placed in a configuration file
        # which is loaded, so they are not embedded in the code
        self.brokerName = "lsst8.ncsa.illinois.edu"
        self.eventTopic = "ocs_event"
        self.rHostList = rHostList
        logger = Log.getDefaultLog()
        self.logger = Log(logger, "BaseDMCS")
        self.baseConfig = self.loadConfig()
        self.isConnected = False
        self.event = None

        self.MAIN = "main"
        self.FAILOVER = "failover"
        self.UNKNOWN = "unknown"
        self.identity = self.UNKNOWN
        self.establishInitialIdentity()

    def loadConfig(self):
        apCtrlPath = envString.resolve("$CTRL_AP_DIR")
        baseConfig = BaseConfig()
        subDirPath = os.path.join(apCtrlPath, "etc", "config", "base.py")
        baseConfig.load(subDirPath)
        return baseConfig

    def establishInitialIdentity(self):
        thisHost = socket.gethostname()
        if thisHost == self.baseConfig.main.host:
            self.identity = self.MAIN
        elif thisHost == self.baseConfig.failover.host:
            self.identity = self.FAILOVER
        else:
            print "couldn't determine host type from config"
            print "I think I'm: ",socket.gethostname()
            print "main host is configured as: ",self.baseConfig.main.host
            print "failover host is configured as: ",self.baseConfig.failover.host
            sys.exit(1)

    def connectToFailover(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((self.baseConfig.failover.host, self.baseConfig.failover.heartbeatPort))
        except socket.gaierror, err:
            print "gaierror ", err
            s = None
        except socket.error, err:
            print "socket.error ", err
            s = None
        return s

    def establishConnection(self):
        if self.identity == self.MAIN:
            s = None
            while s is None:
                s = self.connectToFailover()
                time.sleep(1)
            self.isConnected = True
            self.jsock = JSONSocket(s)

            msg = {"msgtype":"inquire", "question":"whoareyou"}
            self.jsock.sendJSON(msg)

            # response is {"msgtype":"response", "answer":"failover|main"}
            # where "answer" is either "failover" or "main"
            msg = self.jsock.recvJSON()

            print "other side is",msg["answer"]

            if msg["answer"] == self.MAIN:
                self.identity = self.FAILOVER

        elif self.identity == self.FAILOVER:
            serverSock = socket.socket()
            serverSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            serverSock.bind((self.baseConfig.failover.host, self.baseConfig.failover.heartbeatPort))
            serverSock.listen(5)
            readable = False
            while not readable:
                readList = [ serverSock ]
                timeout = 5
                readable, writeable, errored = select.select(readList, [], [], 5)
                if not readable:
                    self.identity = self.MAIN
                    print "main didn't contact; switching to MAIN"
                    continue
            (clientSock, (ipAddr, clientPort)) = serverSock.accept()
            print "connection accepted!"
            self.isConnected = True
            self.jsock = JSONSocket(clientSock)

            msg = self.jsock.recvJSON()
            # TODO: check message
            if msg["msgtype"] != "inquire":
                print "unknown message = ",msg
                return
            if msg["question"] != "whoareyou":
                print "unknown message = ",msg
                return
            resp = {"msgtype":"response", "answer":self.identity}
            msg = self.jsock.sendJSON(resp)

    def handleEvents(self):
        eventSystem = events.EventSystem().getDefaultEventSystem()
        eventSystem.createReceiver(self.brokerName, self.eventTopic)
        st = Status()
        st.publish(st.baseDMCS, st.start)

        self.establishConnection()

        if self.identity == self.MAIN:
            baseHeartEvent = threading.Event()
            hb = BaseHeartbeat(self.jsock, baseHeartEvent)
            hb.start()
        elif self.identity == self.FAILOVER:
            self.event = threading.Event()
            hbr = BaseHeartbeatHandler(self.jsock, self.event)
            hbr.start()


        while True:
            self.logger.log(Log.INFO, "listening on %s " % self.eventTopic)
            st.publish(st.baseDMCS, st.listen, {"topic":self.eventTopic})
            ocsEvent = eventSystem.receiveEvent(self.eventTopic)

            # if the current identity is FAILOVER, don't do anything.
            if self.identify == self.FAILOVER:
                continue
            ps = ocsEvent.getPropertySet()
            ocsEventType = ps.get("ocs_event")
            self.logger.log(Log.INFO, ocsEventType)
            if ocsEventType == "startIntegration":
                jm = jobManager.JobManager()
                visitID = ps.get("visitID")
                exposureSequenceID = ps.get("exposureSequenceID")
                data = {"visitID":visitID, "exposureSequenceID":exposureSequenceID}
                st.publish(st.baseDMCS, st.receivedMsg, {ocsEventType:data})
                jm.submitAllReplicatorJobs(rHostList, visitID, exposureSequenceID)
            elif ocsEventType == "nextVisit":
                visitID = ps.get("visitID")
                exposures = ps.get("exposures")
                boresight = ps.get("boresight")
                filterID = ps.get("filterID")
                data = {"visitID":visitID, "exposures":exposures, "boresight":boresight, "filterID":filterID}
                st.publish(st.baseDMCS, st.receivedMsg, {ocsEventType:data})
                jm = jobManager.JobManager()
                jm.submitWorkerJobs(visitID, exposures, boresight, filterID)

class BaseHeartbeat(threading.Thread):
    def __init__(self, jsock, event):
        super(BaseHeartbeat, self).__init__()
        self.jsock = jsock
        self.event = event
        print "start BaseHeartbeat"
    
    def run(self):

        while not self.event.is_set():
            msg = {"msgtype":"heartbeat"}
            try :
                self.jsock.sendJSON(msg)
                time.sleep(1)
            except:
                print "BaseHeartbeat exception"
                self.event.set()

class BaseHeartbeatHandler(threading.Thread):
    def __init__(self, jsock, event):
        super(BaseHeartbeatHandler, self).__init__()
        self.jsock = jsock
        self.event = event
        print "start BaseHeartbeatHandler"

    def run(self):
        while not self.event.is_set():
            try:
            # TODO: this has to be done via select and a timeout
                msg = self.jsock.recvJSON()
                print msg
            except:
                print "BaseBeartbeatHandler exception"
                self.event.set()

if __name__ == "__main__":
    rHostList = []
    # replicator connection locations
    for x in range(1,12):
        rHostList.append(("lsst-rep1.ncsa.illinois.edu", 8000))
        rHostList.append(("lsst-rep2.ncsa.illinois.edu", 8000))
        
    base = BaseDMCS(rHostList)
    base.handleEvents()
