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
from lsst.daf.base import PropertySet
from lsst.ctrl.ap import jobManager
from lsst.ctrl.ap.status import Status
from lsst.pex.logging import Log
from lsst.ctrl.ap.heartbeat import Heartbeat
from lsst.ctrl.ap.heartbeat import HeartbeatHandler
from lsst.ctrl.ap.baseConfig import BaseConfig
from lsst.ctrl.ap.jsonSocket import JSONSocket

class BaseDMCS(object):

    def __init__(self):
        self.baseConfig = self.loadConfig()

        self.brokerName = self.baseConfig.broker.host
        self.eventTopic = self.baseConfig.broker.topic

        logger = Log.getDefaultLog()
        self.logger = Log(logger, "BaseDMCS")
        self.isConnected = False
        self.event = None

        self.MAIN = "main"
        self.FAILOVER = "failover"
        self.UNKNOWN = "unknown"
        self.identity = self.UNKNOWN
        self.isActive = [ False ]

    def loadConfig(self):
        pack = os.getenv("CTRL_AP_DIR")
        configPath = os.path.join(pack, "etc", "config", "base.py")
        baseConfig = BaseConfig()
        baseConfig.load(configPath)
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

    def handleEvents(self):
        self.establishInitialIdentity()

        eventSystem = events.EventSystem().getDefaultEventSystem()
        eventSystem.createReceiver(self.brokerName, self.eventTopic)
        st = Status()
        st.publish(st.baseDMCS, st.start)

        if self.identity == self.MAIN:
            self.isActive[0] = True
        elif self.identity == self.FAILOVER:
            self.isActive[0] = False

        hm = HeartbeatMonitor(self.baseConfig, self.identity, self.isActive)
        hm.start()

        while True:
            self.logger.log(Log.INFO, "listening on %s " % self.eventTopic)
            st.publish(st.baseDMCS, st.listen, {"topic":self.eventTopic})
            ocsEvent = eventSystem.receiveEvent(self.eventTopic)

            # if the current identity is FAILOVER, don't do anything.
            ps = ocsEvent.getPropertySet()
            ocsEventType = ps.get("ocs_event")
            self.logger.log(Log.INFO, ocsEventType)
            jm = jobManager.JobManager(self.baseConfig)
            if ocsEventType == "startIntegration":
                visitID = ps.get("visitID")
                exposureSequenceID = ps.get("exposureSequenceID")
                data = {"visitID":visitID, "exposureSequenceID":exposureSequenceID}
                st.publish(st.baseDMCS, st.receivedMsg, {ocsEventType:data})
                if self.isActive[0] == False:
                    continue
                jm.submitAllReplicatorJobs(visitID, exposureSequenceID)
            elif ocsEventType == "nextVisit":
                visitID = ps.get("visitID")
                exposures = ps.get("exposures")
                boresight = ps.get("boresight")
                filterID = ps.get("filterID")
                data = {"visitID":visitID, "exposures":exposures, "boresight":boresight, "filterID":filterID}
                st.publish(st.baseDMCS, st.receivedMsg, {ocsEventType:data})
                if self.isActive[0] == False:
                    continue
                jm.submitWorkerJobs(visitID, exposures, boresight, filterID)
            else:
                jm = None

class HeartbeatMonitor(threading.Thread):
    def __init__(self, baseConfig, identity, isActive):
        super(HeartbeatMonitor, self).__init__()
        self.baseConfig = baseConfig
        self.identity = identity
        self.isActive = isActive
        self.MAIN = "main"
        self.FAILOVER = "failover"
        self.UNKNOWN = "unknown"
        self.timeout = 1

    def run(self):
        while True:
            self.establishConnection()
            thr = self.startHeartbeat()
            thr.join()

    def startHeartbeat(self):
        if self.isActive[0] == True:
            hb = Heartbeat(self.jsock, 1)
            hb.start()
            return hb
        else:
            hbr = HeartbeatHandler(self.jsock)
            hbr.start()
            return hbr


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

            msg = {"msgtype":"inquire", "question":"areyouactive"}
            self.jsock.sendJSON(msg)

            # response is {"msgtype":"response", "answer":"failover|main"}
            # where "answer" is either "failover" or "main"
            msg = self.jsock.recvJSON()

            print "other side is",msg["answer"]

            if msg["answer"] == True:
                self.isActive[0] = False
            else:
                self.isActive[0] = True

        elif self.identity == self.FAILOVER:
            serverSock = socket.socket()
            serverSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            serverSock.bind((self.baseConfig.failover.host, self.baseConfig.failover.heartbeatPort))
            serverSock.listen(5)
            readable = False
            while not readable:
                readList = [ serverSock ]
                readable, writeable, errored = select.select(readList, [], [], self.timeout)
                if not readable and (self.isActive[0] == False):
                    self.isActive[0] = True
                    print "main didn't contact; switching to isActive True"
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
            if msg["question"] != "areyouactive":
                print "unknown message = ",msg
                return
            resp = {"msgtype":"response", "answer":self.isActive[0]}
            print "sending:",resp
            msg = self.jsock.sendJSON(resp)


if __name__ == "__main__":
    base = BaseDMCS()
    base.handleEvents()
