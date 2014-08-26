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
import sys
import time
import argparse
import socket
import threading
import lsst.ctrl.events as events
from lsst.ctrl.ap.node import Node
from lsst.ctrl.ap.distributorHandler import DistributorHandler
from lsst.ctrl.ap.jsonSocket import JSONSocket
from lsst.ctrl.ap.key import Key
from lsst.ctrl.ap.status import Status
from lsst.pex.logging import Log
from lsst.daf.base import PropertySet

class DistributorEventHandler(threading.Thread):
    def __init__(self, logger, dataTable, condition):
        threading.Thread.__init__(self)
        self.logger = logger
        self.dataTable = dataTable
        self.condition = condition
        self.brokerName = "lsst8.ncsa.illinois.edu"
        self.eventTopic = "archive_event"

    def run(self):
        eventSystem = events.EventSystem().getDefaultEventSystem()
        eventSystem.createReceiver(self.brokerName, self.eventTopic)
        eventSystem.createTransmitter(self.brokerName, "distributor_event")
        while True:
            self.logger.log(Log.INFO, "listening on %s " % self.eventTopic)
            archiveEvent = eventSystem.receiveEvent(self.eventTopic)
            ps = archiveEvent.getPropertySet()

            # todo: switch in event "request"
            request = ps.get("request")

            
            # send events for contents dataTable
            self.condition.acquire()
            print "dataTable = ",self.dataTable
            for key in self.dataTable:
                root = PropertySet()
                distInfo = Key.split(key)
                root.add("distributor_event", "info")
                root.add("visitID", distInfo[0])
                root.add("exposureSequenceID", distInfo[1])
                root.add("raft", distInfo[2])
                root.add("sensor", distInfo[3])
                info = self.dataTable[key]
                root.add("networkAddress", info.getAddr())
                root.add("networkPort", info.getPort())
                event = events.Event("distributor", root)
                eventSystem.publishEvent("distributor_event", event)
            self.condition.release()

class Heartbeat(threading.Thread):
    def __init__(self, jsock):
        threading.Thread.__init__(self)
        self.jsock = jsock

    def run(self):
        while True:
                msg = {"msgtype":"heartbeat"}
                self.jsock.sendJSON(msg)
                time.sleep(1)

class DistributorNode(Node):

    def __init__(self, port):
        super(DistributorNode, self).__init__()
        self.inboundPort = port
        st = Status()
        self.createIncomingSocket(self.inboundPort)
        server = {st.server:{st.host:socket.gethostname(),st.port:self.inboundPort}}
        st.publish(st.distributorNode, st.start, server)
        logger = Log.getDefaultLog()
        self.logger = Log(logger,"DistributorNode")

    def activate(self):
        dataTable = {}
        condition = threading.Condition()


        # start handler for incoming requests from the archive
        eventHandler = DistributorEventHandler(self.logger, dataTable, condition)
        eventHandler.start()

        st = Status()
        while True:
            self.logger.log(Log.INFO, "Waiting on connection")
            (client, (ipAddr, clientPort)) = self.inSock.accept()
            (remote,addrlist,ipaddrlist) = socket.gethostbyaddr(ipAddr)
            self.logger.log(Log.INFO, "connection accepted: %s:%d" % (remote, clientPort))
            serverInfo = {st.host:socket.gethostname(), st.port:self.inboundPort}
            clientInfo = {st.host:remote,st.port:clientPort}
            connection = {"connection":{st.server:serverInfo, st.client:clientInfo}}
            st.publish(st.distributorNode, st.accept, connection)
            jclient = JSONSocket(client)
            heartbeat = Heartbeat(jclient)
            heartbeat.start()
            dh = DistributorHandler(jclient, dataTable, condition)
            dh.start()

if __name__ == "__main__":
    basename = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=basename)
    parser.add_argument("-P", "--port", type=int, action="store", help="distributor port to connect to", required=True)

    args = parser.parse_args()
    dist = DistributorNode(args.port)
    dist.activate()

