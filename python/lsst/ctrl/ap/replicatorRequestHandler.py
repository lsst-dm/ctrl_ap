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
import errno
import lsst.log as log
from lsst.daf.base import PropertySet
import lsst.ctrl.events as events
from lsst.ctrl.ap.key import Key
from lsst.ctrl.ap.distributor import Distributor
from lsst.ctrl.ap.status import Status

class ReplicatorRequestHandler(object):
    def __init__(self, jsock, dataTable, condition):
        log.debug("rrh: object created")
        self.jsock = jsock
        self.dataTable = dataTable
        self.condition = condition
        # TODO: get these from a config
        self.broker = "lsst8.ncsa.uiuc.edu"
        self.topic = "distributor_event"
        self.distributorTransmitter = events.EventTransmitter(self.broker, self.topic)

    def serviceRequestOLD(self, msg):
        log.debug("rrh: serviceRequest: msg = %s ", msg)
        msgType = msg["msgtype"]
        if msgType == "replicator job":
            self.handleReplicatorJob(msg)
        elif msgType == "wavefront job":
            self.handleWavefrontJob(msg)
            return
        else:
            log.warn("rrh: unknown msg: %s",msg)

    def serviceRequest(self, msg):
        log.debug("rrh: serviceRequest: msg = %s ", msg)
        msgType = msg["msgtype"]
        if msgType == "replicator job":
            self.handleJob(msg, self.splitReplicatorFile)
        elif msgType == "wavefront job":
            self.handleJob(msg, self.splitWavefrontFile)
        else:
            log.warn("rrh: unknown msg: %s",msg)

    def handleJob(self, msg, method):
        log.info("handling request from replicator job")
        st = Status()
        st.publish(st.distributorNode, st.infoReceived, msg)

        request = msg["request"]
        if request == "info post":
            self.sendToArchiveDMCS(msg) # XXX
            log.info('received from replicator %s', msg)
        elif request == "upload":
            name = msg["filename"]
            self.jsock.recvFile(name)
            visitID = msg["visitID"]
            exposureSequenceID = msg["exposureSequenceID"]
            raft = msg["raft"]
            st.publish(st.distributorNode, st.fileReceived, {"file":name, "visitID":visitID, "exposureSequenceID":exposureSequenceID, "raft":raft})
            method(self.jsock, visitID, exposureSequenceID, raft, name)
        else:
            log.warn("unknown request; msg = %s",msg)

    def handleReplicatorJob(self, msg):
        log.info("handling request from replicator job")
        st = Status()
        st.publish(st.distributorNode, st.infoReceived, msg)

        request = msg["request"]
        if request == "info post":
            self.sendToArchiveDMCS(msg) # XXX
            log.info('received from replicator %s', msg)
        elif request == "upload":
            name = msg["filename"]
            self.jsock.recvFile(name)
            visitID = msg["visitID"]
            exposureSequenceID = msg["exposureSequenceID"]
            raft = msg["raft"]
            st.publish(st.distributorNode, st.fileReceived, {"file":name, "visitID":visitID, "exposureSequenceID":exposureSequenceID, "raft":raft})
            #log.info('file received: %s' % name)
            self.splitReplicatorFile(self.jsock, visitID, exposureSequenceID, raft, name)
        else:
            log.warn("unknown request; msg = %s",msg)

    def handleWavefrontJob(self, msg):
        log.info("handling request from wavefront job")
        st = Status()
        st.publish(st.distributorNode,st.infoReceived, msg)
        # this used to send info to the archive DMCS.  Keeping this
        # in place (but commented out) to handle additional work
        # that will be done by the distributor for this type of file. That
        # work hasn't been implemented yet, so this is a placeholder.
        #for raft in ["R:0,0", "R:0,4", "R:4,0", "R:4,4"]:
        #    d["raft"] = raft
        #    self.sendToArchiveDMCS(d)
        request = msg["request"]
        if request == "info post":
            self.sendToArchiveDMCS(msg) # XXX
            log.info('received from replicator %s', msg)
        elif request == "upload":
            name = msg["filename"]
            self.jsock.recvFile(name)
            visitID = msg["visitID"]
            exposureSequenceID = msg["exposureSequenceID"]
            st.publish(st.distributorNode, st.fileReceived, {"file":name, "visitID":visitID, "exposureSequenceID":exposureSequenceID})
            #log.info('file received: %s' % name)
            self.splitWavefrontFile(self.jsock, visitID, exposureSequenceID, name)
        else:
            log.warn("unknown request; msg = %s", msg)

    def sendToArchiveDMCS(self, msg):
        props = PropertySet()
        log.debug("sending the following to archive dmcs...")
        for x in msg:
            print "x = ", x
            log.debug("key = %s", str(x))
            log.debug("val = %s",  str(msg[x]))
            val = msg[x]
            if type(val) == int:
                props.set(str(x), int(msg[x]))
            else:
                props.set(str(x), str(msg[x]))
        log.debug("...done")
        props.set("distributor_event", "info")
        hostinfo = self.jsock.getsockname()
        props.set("networkAddress", hostinfo[0])
        props.set("networkPort", hostinfo[1])

        # store this info locally, in case the archive asks for it again, later

        visitID = props.get("visitID")
        exposureSequenceID = props.get("exposureSequenceID")
        raft = props.get("raft")
        sensors = ["S:0,0","S:1,0","S:2,0", "S:0,1","S:1,1","S:2,1", "S:0,2","S:1,2","S:2,2"]
        st = Status()
        for sensor in sensors:
            props.set("sensor", sensor)
            key = Key.create(visitID, exposureSequenceID, raft, sensor)
            self.storeDistributor(key, hostinfo[0], hostinfo[1])

            data = {}
            for x in props.paramNames():
                data[x] = props.get(x)
            st.publish(st.distributorNode, st.sendMsg, data)

            event = events.Event("distributor", props)
            self.distributorTransmitter.publishEvent(event)

    def storeFileInfo(self, key, inetaddr, port, name):
        log.debug("storeFileInfo: key = %s, name = %s ", str(key), str(name))
        distInfo = None
        notifyArchive = False
        self.condition.acquire()
        # Ordinarily, the worker would already know which distributor has
        # its information, and will be waiting for the file to arrive.
        #
        # It's possible that the distributor receives information
        # about the incoming file, but doesn't have a previous entry
        # for that file.  In this case, the worker is stuck at the archiveDMCS
        # waiting for that information.  If that's the case, then we need
        # to send information to the archiveDMCS with the info for the file,
        # so we indicate that in the return value in this method.
        # 
        if key in self.dataTable:
            distInfo = self.dataTable[key]
            distInfo.setName(name)
            self.dataTable[key] = distInfo
        else:
            notifyArchive = True
            distInfo = Distributor(inetaddr, port)
            distInfo.setName(name)
            self.dataTable[key] = distInfo
        self.condition.notifyAll()
        self.condition.release()
        return notifyArchive

    def storeDistributor(self, key, inetaddr, port):
        log.debug("storeDistributor: key = %s, inet = %s:%s ", key, inetaddr, port)
        self.condition.acquire()
        self.dataTable[key] = Distributor(inetaddr, port)
        self.condition.notifyAll()
        self.condition.release()

    def splitReplicatorFile(self, jsock, visitID, exposureSequenceID, raft, filename):
        sensors = ["S:0,0","S:1,0","S:2,0", "S:0,1","S:1,1","S:2,1", "S:0,2","S:1,2","S:2,2"]
        x = 0
        for s in sensors:
            sensors[x] = raft+" "+s
            x += 1
        self.splitFile(jsock, visitID, exposureSequenceID, filename, sensors)

    def splitWavefrontFile(self, jsock, visitID, exposureSequenceID, filename):
        sensors = ["R:0,0 S:2,2", "R:0,4 S:2,0", "R:4,0 S:0,2", "R:4,4 S:0,0"]
        self.splitFile(jsock, visitID, exposureSequenceID, filename, sensors)

    def splitFile(self, jsock, visitID, exposureSequenceID, filename, sensors):
        statinfo = os.stat(filename)
        filesize = statinfo.st_size
        buflen = filesize/len(sensors)
        hostinfo = self.jsock.getsockname()
        inetaddr =  hostinfo[0]
        port = hostinfo[1]
        with open(filename, 'rb') as src:
            for sensorInfo in sensors:
                raft = sensorInfo.split(" ")[0]
                sensor = sensorInfo.split(" ")[1]
                target = "/tmp/lsst/%s/%s/%s_%s" % (visitID, exposureSequenceID, raft, sensor)
                targetDir = os.path.dirname(target)
                # try and create the directory tree.  there's a race condition
                # where directories could be created during the makedirs
                # call (by another process on the same machine), but that's
                # ok, so if they do exist, we can ignore the exception that
                # would be thrown.
                try:
                    os.makedirs(targetDir)
                except OSError as exc:
                    if exc.errno == errno.EEXIST and os.path.isdir(targetDir):
                        pass
                    else:
                        raise
                f = open(target,'wb')
                # TODO:  This needs to seek to the right place.
                f.write(src.read(buflen))
                f.close()
                key = Key.create(visitID, exposureSequenceID, raft, sensor)
                notifyArchive = self.storeFileInfo(key,inetaddr, port, target)
                # If there wasn't a previous entry for this file, we need
                # to notify the archive of it's existence so the worker job
                # can find it.
                if notifyArchive:
                    vals = {
                        "request":"info post",
                        "msgtype": "replicator job",
                        "exposureSequenceID": exposureSequenceID,
                        "raft": str(raft),
                        "sensor": str(sensor),
                        "visitID": str(visitID)
                    }
                    self.sendToArchiveDMCS(vals)
