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
from lsst.ctrl.ap.node import Node
from lsst.ctrl.ap.distributorHandler import DistributorHandler
from lsst.ctrl.ap.jsonSocket import JSONSocket
from lsst.pex.logging import Log

class DistributorEventHandler(threading.Thread):
    def __init__(self, dataTable, condition):
        threading.Thread.__init__(self)
        self.dataTable = dataTable
        self.condition = condition
        self.brokerName = "lsst8.ncsa.illinois.edu"
        self.eventTopic = "archive_event"

    def run(self):
        eventSystem = events.EventSystem().getDefaultEventSystem()
        eventSystem.createReceiver(self.brokerName, self.eventTopic)
        while True:
            self.logger.log(Log.INFO, "listening on %s " % self.eventTopic)
            archiveEvent = eventSystem.receiveEvent(self.eventTopic)
            ps = archiveEvent.getPropertySet()
            archiveEventType = ps.get("archive_event")

            eventSystem.createTransmitter(self.brokerName, "distributor_event")
            root = PropertySet()
            # todo: switch in event "request"
            
            # send events for contents dataTable
            self.condition.aquire()
            for key in dataTable:
                elem = split(":")
                root.add("exposureSequenceID", elem[0])
                root.add("visitID", elem[1])
                root.add("raft", elem[2])
                net = dataTable[key]
                root.add("networkAddress", net[0])
                root.add("networkPort", net[1])
                event = events.Event("distributor", "root")
                eventSystem.publishEvent("distributor_event", event)
            self.condition.release()
                

class DistributorNode(Node):

    def __init__(self, port):
        super(DistributorNode, self).__init__()
        self.createIncomingSocket(socket.gethostname(), port)
        logger = Log.getDefaultLog()
        self.logger = Log(logger,"DistributorNode")

    def activate(self):
        dataTable = {}
        condition = threading.Condition()

        # start handler for incoming requests from the archive
        eventHandler = DistributorEventHandler(dataTable, condition)
        eventHandler.start()

        while True:
            self.logger.log(Log.INFO, "Waiting on connection")
            (client, (ipAddr, clientPort)) = self.inSock.accept()
            self.logger.log(Log.INFO, "connection accepted: %s:%d" % (ipAddr, clientPort))
            jclient = JSONSocket(client)
            dh = DistributorHandler(jclient, dataTable, condition)
            dh.start()

if __name__ == "__main__":
    basename = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=basename)
    parser.add_argument("-P", "--port", type=int, action="store", help="distributor port to connect to", required=True)

    args = parser.parse_args()
    dist = DistributorNode(args.port)
    dist.activate()

