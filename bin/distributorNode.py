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
import argparse
import socket
import threading
import traceback
import lsst.ctrl.events as events
from lsst.ctrl.ap.node import Node
from lsst.ctrl.ap.jobMessageDispatcher import JobMessageDispatcher
from lsst.ctrl.ap.jsonSocket import JSONSocket
from lsst.ctrl.ap.status import Status
import lsst.log as log
from lsst.ctrl.ap.logConfigurator import LogConfigurator
from lsst.daf.base import PropertySet
from lsst.ctrl.ap.conditionNotifier import ConditionNotifier

class DistributorDataReporter(threading.Thread):
    def __init__(self, broker, topic, dataTable, condition):
        threading.Thread.__init__(self)
        self.dataTable = dataTable
        self.condition = condition
        self.brokerName = broker
        self.eventTopic = topic

    def run(self):
        eventSystem = events.EventSystem.getDefaultEventSystem()
        while True:
            log.info("listening on %s " % self.eventTopic)
            log.info("handler: thread count = %d" % threading.activeCount())
            archiveEvent = eventSystem.receiveEvent(self.eventTopic)
            
            ps = archiveEvent.getPropertySet()
            print"event received: ",ps.toString()

            # send events for contents of dataTable
            self.condition.acquire()
            print "dataTable = ",self.dataTable
            for key in self.dataTable:
                root = PropertySet()
                distInfo = list(key)
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

class DistributorNode(Node):

    def __init__(self, port):
        super(DistributorNode, self).__init__()
        self.brokerName = "lsst8.ncsa.illinois.edu"
        self.eventTopic = "archive_event"

        self.inboundPort = port

        configurator = LogConfigurator()
        configurator.loadProperties()

        st = Status()
        self.createIncomingSocket(self.inboundPort)
        server = {st.server:{st.host:socket.gethostname(),st.port:self.inboundPort}}
        st.publish(st.distributorNode, st.start, server)

        eventSystem = events.EventSystem.getDefaultEventSystem()
        eventSystem.createReceiver(self.brokerName, self.eventTopic)
        eventSystem.createTransmitter(self.brokerName, "distributor_event")
        
        root = PropertySet()
        root.add("distributor_event", "started")
        root.add("networkAddress",socket.gethostbyname(socket.gethostname()))
        root.add("networkPort", self.inboundPort)
        event = events.Event("distributor", root)
        eventSystem.publishEvent("distributor_event", event)

    def activate(self):
        # tcp keep alive activates after 1 second
        idle = 1
        # keep alive ping every 5 seconds
        interval = 2
        # close connection after 3 failed pings
        fails = 3
        dataTable = {}
        condition = threading.Condition()


        # start handler for incoming requests from the archive
        # to replenish its information
        reporter = DistributorDataReporter(self.brokerName, self.eventTopic, dataTable, condition)
        reporter.start()

        cn = ConditionNotifier(condition)
        cn.start()

        st = Status()
        while True:
            log.info("Waiting on connection")
            log.info("activate: thread count = %d" % threading.activeCount())
            (client, (ipAddr, clientPort)) = self.inSock.accept()
            client.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, idle)
            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval)
            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, fails)
            log.info("accepted: ipAddr = %s",str(ipAddr))
            log.info("accepted: clientPort = %s",str(clientPort))
            (remote,addrlist,ipaddrlist) = socket.gethostbyaddr(ipAddr)
            log.info("connection accepted: %s:%d" % (remote, clientPort))
            serverInfo = {st.host:socket.gethostname(), st.port:self.inboundPort}
            clientInfo = {st.host:remote,st.port:clientPort}
            connection = {"connection":{st.server:serverInfo, st.client:clientInfo}}
            st.publish(st.distributorNode, st.accept, connection)
            jclient = JSONSocket(client)

            messageDispatcher = JobMessageDispatcher(jclient, dataTable, condition)
            messageDispatcher.start()
            log.info("active thread count: %d" % threading.activeCount())

if __name__ == "__main__":
    basename = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=basename)
    parser.add_argument("-P", "--port", type=int, action="store", help="distributor port to connect to", required=True)

    args = parser.parse_args()
    dist = DistributorNode(args.port)
    dist.activate()

