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

    def loadConfig(self):
        apCtrlPath = envString.resolve("$CTRL_AP_DIR")
        baseConfig = BaseConfig()
        subDirPath = os.path.join(apCtrlPath, "etc", "config", "base.py")
        baseConfig.load(subDirPath)
        return baseConfig

    def isMain(self):
        thisHost = socket.gethostname()
        if thisHost == self.baseConfig.main.host:
            print "Configured as the main base DMCS"
            return True
        return False

    def isFailover(self):
        thisHost = socket.gethostname()
        if thisHost == self.baseConfig.failover.host:
            print "Configured as the failover base DMCS"
            return True
        return False

    def connectToFailover(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((self.baseConfig.failover.host, self.baseConfig.failover.heartbeatPort))
        except socket.gaierror, err:
            print err
            s = None
        except socket.error, err:
            print err
            s = None
        return s


    def handleEvents(self):
        eventSystem = events.EventSystem().getDefaultEventSystem()
        eventSystem.createReceiver(self.brokerName, self.eventTopic)
        st = Status()
        st.publish(st.baseDMCS, st.start)

        if self.isMain():
            s = None
            while s is None:
                s = self.connectToFailover()
                time.sleep(1)
            self.isConnected = True
            jsock = JSONSocket(s)
            hb = Heartbeat(jsock)
            hb.start()
        elif self.isFailover():
            serverSock = socket.socket()
            serverSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            serverSock.bind((self.baseConfig.failover.host, self.baseConfig.failover.heartbeatPort))
            serverSock.listen(5)
            while True:
                (clientSock, (ipAddr, clientPort)) = serverSock.accept()
                self.isConnected = True
                jsock = JSONSocket(clientSock)

                self.event = threading.Event()
                hbr = HeartbeatHandler(jsock, self.event)
                hbr.start()
        else:
            print "couldn't determine host type from config"
            print "I think I'm: ",socket.gethostname()
            print "main host is configured as: ",self.baseConfig.main.host
            print "failover host is configured as: ",self.baseConfig.failover.host
            sys.exit(1)

        while True:
            self.logger.log(Log.INFO, "listening on %s " % self.eventTopic)
            st.publish(st.baseDMCS, st.listen, {"topic":self.eventTopic})
            ocsEvent = eventSystem.receiveEvent(self.eventTopic)
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

class HeartbeatHandler(threading.Thread):
    def __init__(self, jsock, event):
        super(HeartbeatHandler, self).__init__()
        self.jsock = jsock
        self.event = event

    def run(self):
        while not self.event.is_set():
            try:
            # TODO: this has to be done via select and a timeout
                msg = self.jsock.recvJSON()
                print msg
            except:
                print "heartbeat exception"
                self.event.set()

if __name__ == "__main__":
    rHostList = []
    # replicator connection locations
    for x in range(1,12):
        rHostList.append(("lsst-rep1.ncsa.illinois.edu", 8000))
        rHostList.append(("lsst-rep2.ncsa.illinois.edu", 8000))
        
    base = BaseDMCS(rHostList)
    base.handleEvents()
