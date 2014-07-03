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


import lsst.ctrl.events as events
from lsst.daf.base import PropertySet
from lsst.ctrl.ap import jobManager
from lsst.pex.logging import Log
import threading
import socket

class SocketHandler(threading.Thread):
    def __init__(self, dataTable):
        print "init SocketHandler()"
        threading.Thread.__init__(self)
        self.dataTable = dataTable

    def run(self):
        print "starting SocketHandler()"
        serverSock = socket.socket()
        host = socket.gethostname()
        port = 9595
        print "starting on %s:%d" % (host, port)
        serverSock.bind((host,port))
        serverSock.listen(5)
        while True:
            client, port = serverSock.accept()
            # do something interesting here
    

class EventHandler(threading.Thread):

    def __init__(self, logger, brokerName, eventTopic, dataTable):
        print "init EventHandler()"
        threading.Thread.__init__(self)
        self.logger = logger
        self.dataTable = dataTable
        self.brokerName = brokerName
        self.eventTopic = eventTopic

    def run(self):
        print "starting EventHandler()"
        eventSystem = events.EventSystem().getDefaultEventSystem()
        eventSystem.createReceiver(self.brokerName, self.eventTopic)
        while True:
            self.logger.log(Log.INFO, "listening on %s " % self.eventTopic)
            ocsEvent = eventSystem.receiveEvent(self.eventTopic)
            ps = ocsEvent.getPropertySet()
            ocsEventType = ps.get("distributor_event")
            self.logger.log(Log.INFO, ocsEventType)
            exposureSequenceID = ps.get("exposureSequenceID")
            visitID = ps.get("visitID")
            raft = ps.get("raft")
            inetaddr = ps.get("networkAddress")
            port = ps.get("networkPort")
            print "exposureSequenceID = %s, visitID = %s, raft = %s, networkAddress = %s, networkPort = %s" % (exposureSequenceID, visitID, raft, inetaddr, str(port))

class ArchiveDMCS(object):

    def __init__(self):
        # TODO:  these need to be placed in a configuration file
        # which is loaded, so they are not embedded in the code
        self.brokerName = "lsst8.ncsa.illinois.edu"
        self.eventTopic = "distributor_event"
        logger = Log.getDefaultLog()
        self.logger = Log(logger, "ArchiveDMCS")


if __name__ == "__main__":
    archive = ArchiveDMCS()

    dataTable = {}

    socks = SocketHandler(dataTable)
    socks.setDaemon(True)
    socks.start()

    eve = EventHandler(archive.logger, archive.brokerName, archive.eventTopic, dataTable)
    eve.setDaemon(True)
    eve.start()

    socks.join()
    eve.join()
