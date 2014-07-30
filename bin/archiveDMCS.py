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
from lsst.ctrl.ap.jsonSocket import JSONSocket
from lsst.ctrl.ap.key import Key
from lsst.ctrl.ap.status import Status
import threading
import socket
from time import sleep

class DistributorLookupHandler(threading.Thread):
    def __init__(self, dataTable, condition, sock):
        threading.Thread.__init__(self)
        self.dataTable = dataTable
        self.condition = condition
        self.sock = sock

    def run(self):
        jsock = JSONSocket(self.sock)
            
        request = jsock.recvJSON()
            
        inetaddr = None
        port = None
        data = self.lookupData(request)
        if data is not None:
            inetaddr = data[0]
            port = data[1]

        vals = {"inetaddr":inetaddr, "port":port}
        jsock.sendJSON(vals)
        self.sock.close()

    def lookupData(self, request):
        exposureSequenceID = request["exposureSequenceID"]
        visitID = request["visitID"]
        raft = request["raft"]
        ccd = request["ccd"]
        #key = "%s:%s:%s" % (exposureSequenceID,visitID,raft)
        key = Key.create(visitID, exposureSequenceID, raft, ccd)
        # 
        #print "about to acquire condition"
        st = Status()
        request = {st.data:{"visitID":visitID, "exposureSequenceID":exposureSequenceID, "raft":raft, "sensor":ccd}}
        st.publish(st.archiveDMCS, st.lookup, request)
        self.condition.acquire()
        while True:
            if key in self.dataTable:
                data = self.dataTable[key]
                break
            # wait until the self.dataTable is updated, so we can
            # check again
            self.condition.wait()
        self.condition.release()
        st.publish(st.archiveDMCS, st.retrieved, request)
        return data

class ArchiveConnectionHandler(threading.Thread):
    def __init__(self, dataTable, condition):
        threading.Thread.__init__(self)
        self.dataTable = dataTable
        self.condition = condition

    def run(self):
        serverSock = socket.socket()
        serverSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        host = socket.gethostname()
        port = 9595
        serverSock.bind((host,port))
        serverSock.listen(5)
        st = Status()
        while True:
            (clientSock, (ipAddr, clientPort)) = serverSock.accept()
            # spawn a thread to handle this connection
            clientInfo = {st.client:{st.host:ipAddr,st.port:clientPort}}
            st.publish(st.archiveDMCS, st.accept, clientInfo)
            dist = DistributorLookupHandler(self.dataTable, self.condition, clientSock)
            dist.start()
            # TODO: should do cleanup here

class EventHandler(threading.Thread):

    def __init__(self, logger, brokerName, eventTopic, dataTable, condition):
        threading.Thread.__init__(self)
        self.logger = logger
        self.brokerName = brokerName
        self.eventTopic = eventTopic
        self.dataTable = dataTable
        self.condition = condition

    def requestDistributors(self):
        topic = "archive_event"

        eventSystem = events.EventSystem().getDefaultEventSystem()
        eventSystem.createTransmitter(self.brokerName, topic)

        root = PropertySet()
        root.add("request","distributorInfo")

        event = events.Event("archive", root)
        eventSystem.publishEvent(topic, event)
        

    def run(self):
        eventSystem = events.EventSystem().getDefaultEventSystem()
        eventSystem.createReceiver(self.brokerName, self.eventTopic)
        self.requestDistributors()
        st = Status()
        while True:
            self.logger.log(Log.INFO, "listening on %s " % self.eventTopic)
            st.publish(st.archiveDMCS, st.listen, {"topic":self.eventTopic})
            ocsEvent = eventSystem.receiveEvent(self.eventTopic)
            ps = ocsEvent.getPropertySet()
            print "ps = ",ps.toString()
            ocsEventType = ps.get("distributor_event")
            exposureSequenceID = ps.get("exposureSequenceID")
            visitID = ps.get("visitID")
            raft = ps.get("raft")
            sensor = ps.get("sensor")
            inetaddr = ps.get("networkAddress")
            port = ps.get("networkPort")
            key = Key.create(visitID, exposureSequenceID, raft, sensor)
            self.logger.log(Log.INFO, "%s %s:%s" % (key, inetaddr,port))
            data = {"endpoint":{st.host:inetaddr,st.port:port},
                    st.data:{"visitID":visitID, "exposureSequenceID":exposureSequenceID, "raft":raft, "sensor":sensor}}

            st.publish(st.archiveDMCS, st.receivedMsg, data)

            self.condition.acquire()
            self.dataTable[key] = (inetaddr, port)
            self.condition.notifyAll()
            self.condition.release()

class ArchiveDMCS(object):
    def __init__(self):
        # TODO:  these need to be placed in a configuration file
        # which is loaded, so they are not embedded in the code
        self.brokerName = "lsst8.ncsa.illinois.edu"
        self.eventTopic = "distributor_event"
        logger = Log.getDefaultLog()
        self.logger = Log(logger, "ArchiveDMCS")
        st = Status()
        st.publish(st.archiveDMCS, st.start)


if __name__ == "__main__":
    archive = ArchiveDMCS()

    condition = threading.Condition()
    dataTable = {}

    socks = ArchiveConnectionHandler(dataTable, condition)
    socks.setDaemon(True)
    socks.start()

    eve = EventHandler(archive.logger, archive.brokerName, archive.eventTopic, dataTable, condition)
    eve.setDaemon(True)
    eve.start()

    socks.join()
    eve.join()
