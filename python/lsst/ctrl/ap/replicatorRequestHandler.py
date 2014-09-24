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
import json
import socket
import threading
import errno
from lsst.pex.logging import Log
from lsst.daf.base import PropertySet
import lsst.ctrl.events as events
from lsst.ctrl.ap.key import Key
from lsst.ctrl.ap.distributorInfo import DistributorInfo
from lsst.ctrl.ap.status import Status

class ReplicatorRequestHandler(object):
    def __init__(self, jsock, dataTable, condition):
        print "rrh: object created"
        self.logger = Log.getDefaultLog()
        self.jsock = jsock
        self.dataTable = dataTable
        self.condition = condition

    def serviceRequest(self, msg):
        print "rrh: serviceRequest: msg = ",msg
        msgType = msg["msgtype"]
        if msgType == "replicator job":
            self.handleReplicatorJob(msg)
        elif msgType == "wavefront job":
            self.handleWavefrontJob(msg)
            return
        else:
            print "rrh: unknown msg: ",msg

    def handleReplicatorJob(self, msg):
        self.logger.log(Log.INFO, "handling request from replicator job")
        st = Status()
        st.publish(st.distributorNode, st.infoReceived, msg)

        request = msg["request"]
        if request == "info post":
            self.sendToArchiveDMCS(msg) # XXX
            self.logger.log(Log.INFO, 'received from replicator %s' % msg)
        elif request == "upload":
            name = msg["filename"]
            self.jsock.recvFile(name)
            visitID = msg["visitID"]
            exposureSequenceID = msg["exposureSequenceID"]
            raft = msg["raft"]
            st.publish(st.distributorNode, st.fileReceived, {"file":name, "visitID":visitID, "exposureSequenceID":exposureSequenceID, "raft":raft})
            #self.logger.log(Log.INFO, 'file received: %s' % name)
            self.splitReplicatorFile(visitID, exposureSequenceID, raft, name)
        else:
            print "unknown request; msg = ",msg

    def handleWavefrontJob(self, msg):
        d = msg.copy()
        synRaft = msg["raft"]
        st = Status()
        st.publish(st.distributorNode,st.infoReceived, d)
        # this used to send info to the archive DMCS.  Keeping this
        # in place (but commented out) to handle additional work
        # that will be done by the distributor for this type of file. That
        # work hasn't been implemented yet, so this is a placeholder.
        #for raft in ["R:0,0", "R:0,4", "R:4,0", "R:4,4"]:
        #    d["raft"] = raft
        #    self.sendToArchiveDMCS(d)
        name = self.jsock.recvFile2()
        visitID = msg["visitID"]
        exposureSequenceID = msg["exposureSequenceID"]
        st.publish(st.distributorNode, st.fileReceived, {"file":name, "visitID":visitID, "exposureSequenceID":exposureSequenceID, "raft":synRaft})
        #self.logger.log(Log.INFO, 'wavefront file received: %s' % name)
        #self.splitWavefrontFile(visitID, exposureSequenceID, name)

    def sendToArchiveDMCS(self, msg):
        props = PropertySet()
        print "sendToArchiveDMCS: props = ",props.toString()
        for x in msg:
            print x, msg[x]
            val = msg[x]
            if type(val) == int:
                props.set(str(x), int(msg[x]))
            else:
                props.set(str(x), str(msg[x]))
        props.set("distributor_event", "archive info")
        hostinfo = self.jsock.getsockname()
        props.set("networkAddress", hostinfo[0])
        props.set("networkPort", hostinfo[1])

        # TODO: get these from a config
        self.broker = "lsst8.ncsa.uiuc.edu"
        self.topic = "distributor_event"
        self.distributorTransmitter = events.EventTransmitter(self.broker, self.topic)
        # store this info locally, in case the archive asks for it again, later

        visitID = props.get("visitID")
        exposureSequenceID = props.get("exposureSequenceID")
        raft = props.get("raft")
        sensors = ["S:0,0","S:1,0","S:2,0", "S:0,1","S:1,1","S:2,1", "S:0,2","S:1,2","S:2,2"]
        st = Status()
        for sensor in sensors:
            props.set("sensor", sensor)
            key = Key.create(visitID, exposureSequenceID, raft, sensor)
            self.storeDistributorInfo(key, hostinfo[0], hostinfo[1])

            
            data = {}
            for x in props.paramNames():
                data[x] = props.get(x)
            st.publish(st.distributorNode, st.sendMsg, data)

            event = events.Event("distributor", props)
            self.distributorTransmitter.publishEvent(event)

    def storeFileInfo(self, key, name):
        print "storeFileInfo: key = %s, name = %s " % (key, name)
        self.condition.acquire()
        distInfo = self.dataTable[key]
        distInfo.setName(name)
        self.dataTable[key] = distInfo
        self.condition.notifyAll()
        self.condition.release()

    def storeDistributorInfo(self, key, inetaddr, port):
        print "storeDistributorInfo: key = %s, inet = %s:%s " % (key, inetaddr, port)
        self.condition.acquire()
        self.dataTable[key] = DistributorInfo(inetaddr, port)
        self.condition.notifyAll()
        self.condition.release()

    def splitReplicatorFile(self, visitID, exposureSequenceID, raft, filename):
        sensors = ["S:0,0","S:1,0","S:2,0", "S:0,1","S:1,1","S:2,1", "S:0,2","S:1,2","S:2,2"]
        x = 0
        for s in sensors:
            sensors[x] = raft+" "+s
            x += 1
        self.splitFile(visitID, exposureSequenceID, filename, sensors)

    def splitWavefrontFile(self, visitID, exposureSequenceID, filename):
        sensors = ["R:0,0 S:2,2", "R:0,4 S:2,0", "R:4,0 S:0,2", "R:4,4 S:0,0"]
        self.splitFile(visitID, exposureSequenceID, filename, sensors)

    def splitFile(self, visitID, exposureSequenceID, filename, sensors):
        statinfo = os.stat(filename)
        filesize = statinfo.st_size
        buflen = filesize/len(sensors)
        with open(filename, 'rb') as src:
            for sensorInfo in sensors:
                raft = sensorInfo.split(" ")[0]
                sensor = sensorInfo.split(" ")[1]
                target = "/tmp/lsst/%s/%s/%s_%s" % (visitID, exposureSequenceID, raft, sensor)
                targetDir = os.path.dirname(target)
                try:
                    os.makedirs(targetDir)
                except OSError as exc:
                    if exc.errno == errno.EEXIST and os.path.isdir(targetDir):
                        pass
                    else:
                        raise
                f = open(target,'wb')
                f.write(src.read(buflen))
                f.close()
                key = Key.create(visitID, exposureSequenceID, raft, sensor)
                self.storeFileInfo(key, target)
