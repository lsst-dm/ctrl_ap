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
from lsst.ctrl.ap.jsonSocket import JSONSocket
from lsst.ctrl.ap.key import Key
from lsst.ctrl.ap.status import Status
import threading
import socket
import sys
import lsst.log as log
from lsst.ctrl.ap.logConfigurator import LogConfigurator

class LookupMessageDispatcher(threading.Thread):
    def __init__(self, name, dataTable, condition, sock):
        threading.Thread.__init__(self, name="distributor:%s" % name)
        self.dataTable = dataTable
        self.condition = condition
        self.sock = sock

    def run(self):
        jsock = JSONSocket(self.sock)
            
        # incoming lookup request from worker
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
        key = Key.create(visitID, exposureSequenceID, raft, ccd)
        
        st = Status()
        request = {st.data:{"visitID":visitID, "exposureSequenceID":exposureSequenceID, "raft":raft, "sensor":ccd}}
        st.publish(st.archiveDMCS, st.lookup, request)
        self.condition.acquire()
        while True:
            if key in self.dataTable:
                data = self.dataTable[key]
                break
            log.warn("couldn't find key = %s",key)
            # wait until the self.dataTable is updated, so we can
            # check again
            self.condition.wait()
        self.condition.release()
        st.publish(st.archiveDMCS, st.retrieved, request)
        return data

class ArchiveConnectionHandler(threading.Thread):
    def __init__(self, dataTable, condition):
        threading.Thread.__init__(self, name="archive")
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
        connectCount = 0
        while True:
            (clientSock, (ipAddr, clientPort)) = serverSock.accept()
            # spawn a thread to handle this connection
            clientInfo = {st.client:{st.host:ipAddr,st.port:clientPort}}
            st.publish(st.archiveDMCS, st.accept, clientInfo)
            name = "%s:%s" % (str(ipAddr), str(clientPort))
            lmd = LookupMessageDispatcher(name, self.dataTable, self.condition, clientSock)
            lmd.start()
            connectCount += 1
            # TODO: should do cleanup here
            log.debug("connection count = %d; threadCount = %d", connectCount,threading.activeCount())
            threads = threading.enumerate()
            for x in threads:
                log.debug(x.name)

class EventHandler(threading.Thread):

    def __init__(self, brokerName, eventTopic, dataTable, condition):
        threading.Thread.__init__(self, name="event")
        self.brokerName = brokerName
        self.eventTopic = eventTopic
        self.dataTable = dataTable
        self.condition = condition

    def requestDistributors(self):
        topic = "archive_event"

        eventSystem = events.EventSystem.getDefaultEventSystem()
        eventSystem.createTransmitter(self.brokerName, topic)

        root = PropertySet()
        root.add("request","distributorInfo")

        event = events.Event("archive", root)
        eventSystem.publishEvent(topic, event)

    # return exposure sequence id, visit id, raft and sensor specified in 
    # property set
    def getImageInfo(self, ps):
        exposureSequenceID = ps.get("exposureSequenceID")
        visitID = ps.get("visitID")
        raft = ps.get("raft")
        sensor = ps.get("sensor")
        return exposureSequenceID, visitID, raft, sensor
        
    # create the key used for entries in the data table
    def createKey(self, ps):
        exposureSequenceID, visitID, raft, sensor = self.getImageInfo(ps)
        key = Key.create(visitID, exposureSequenceID, raft, sensor)
        return key

    # create a dictionary containing information about a data table entry
    # specified in a property set
    def createData(self, ps):
        exposureSequenceID, visitID, raft, sensor = self.getImageInfo(ps)
        inetaddr = ps.get("networkAddress")
        port = ps.get("networkPort")
        st = Status()
        data = {"endpoint":{st.host:inetaddr,st.port:port},
                st.data:{"visitID":visitID, "exposureSequenceID":exposureSequenceID, "raft":raft, "sensor":sensor}}
        return data

    # insert entry into data table, given info specified in property set
    def insert(self, ps):
        st = Status()
        key = self.createKey(ps)
        data = self.createData(ps)

        st.publish(st.archiveDMCS, st.receivedMsg, data)

        inetaddr = ps.get("networkAddress")
        port = ps.get("networkPort")

        log.debug("attempting to add %s", ps.toString())
        self.condition.acquire()
        self.dataTable[key] = (inetaddr, port)
        self.condition.notifyAll()
        self.condition.release()
        log.debug("added")

    # remove entries from data table, given ip addr and port  specified
    # in property set
    def remove(self, ps):
        addr = ps.get("networkAddress")
        port = ps.get("networkPort")
        hostport = (addr, port)

        log.debug("attempting to remove %s", ps.toString())
        self.condition.acquire()
        removeThese = []
        for ent in self.dataTable:
            if dataTable[ent] == hostport:
                log.debug("removing %s",ent)
                removeThese.append(ent)
        if len(removeThese) == 0:
            log.warn("Didn't remove anything")
            pass
        else:
            for x in removeThese:
                self.dataTable.pop(x)
        self.condition.notifyAll()
        self.condition.release()

    def run(self):
        eventSystem = events.EventSystem.getDefaultEventSystem()
        eventSystem.createReceiver(self.brokerName, self.eventTopic)
        self.requestDistributors()
        st = Status()
        log.info("listening on %s " % self.eventTopic)
        while True:
            st.publish(st.archiveDMCS, st.listen, {"topic":self.eventTopic})
            ocsEvent = eventSystem.receiveEvent(self.eventTopic)
            ps = ocsEvent.getPropertySet()
        
            ocsEventType = ps.get("distributor_event")
            if ocsEventType == "info":
                self.insert(ps)
            elif ocsEventType == "started":
                self.remove(ps)
            else:
                log.warn("ocsEventType unknown: %s",ocsEventType)
                sys.exit(0)

class ArchiveDMCS(object):
    def __init__(self):
        # TODO:  these need to be placed in a configuration file
        # which is loaded, so they are not embedded in the code
        self.brokerName = "lsst8.ncsa.illinois.edu"
        self.eventTopic = "distributor_event"

        configurator = LogConfigurator()
        configurator.loadProperties()

        st = Status()
        st.publish(st.archiveDMCS, st.start)

if __name__ == "__main__":
    archive = ArchiveDMCS()

    condition = threading.Condition()
    dataTable = {}

    socks = ArchiveConnectionHandler(dataTable, condition)
    socks.setDaemon(True)
    socks.start()

    eve = EventHandler(archive.brokerName, archive.eventTopic, dataTable, condition)
    eve.setDaemon(True)
    eve.start()

    socks.join()
    eve.join()
