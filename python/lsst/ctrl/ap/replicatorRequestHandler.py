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
from lsst.pex.logging import Log
from lsst.daf.base import PropertySet
import lsst.ctrl.events as events
from lsst.ctrl.ap.key import Key

class ReplicatorRequestHandler(object):
    def __init__(self, logger, jsock, msg, dataTable, condition):
        self.logger = logger
        self.jsock = jsock
        self.msg = msg
        self.dataTable = dataTable
        self.condition = condition

    def handleRequest(self):
        msgtype = self.msg["msgtype"]
        if msgtype == "replicator job":
            self.handleReplicatorJob()
        elif msgtype == "wavefront job":
            self.handleWavefrontJob()
        else:
            print "unknown msgtype: %s" % msgtype

    def handleReplicatorJob(self):
        self.sendToArchiveDMCS(self.msg) # XXX
        self.logger.log(Log.INFO, 'received from replicator %s' % self.msg)
        name = self.jsock.recvFile()
        self.logger.log(Log.INFO, 'file received: %s' % name)
        visitID = self.msg["visitID"]
        exposureSequenceID = self.msg["exposureSequenceID"]
        raft = self.msg["raft"]
        self.splitReplicatorFile(visitID, exposureSequenceID, raft, name)

    def handleWavefrontJob(self):
        d = self.msg.copy()
        for raft in ["R:0,0", "R:0,4", "R:4,0", "R:4,4"]:
            d["raft"] = raft
            self.sendToArchiveDMCS(d)
        name = self.jsock.recvFile()
        self.logger.log(Log.INFO, 'wavefront file received: %s' % name)
        visitID = self.msg["visitID"]
        exposureSequenceID = self.msg["exposureSequenceID"]
        self.splitWavefrontFile(visitID, exposureSequenceID, name)

    def sendToArchiveDMCS(self, msg):
        props = PropertySet()
        print "sendToArchiveDMCS: props = ",props
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
        eventSystem = events.EventSystem.getDefaultEventSystem()
        self.archiveTransmitter = events.EventTransmitter(self.broker, self.topic)
        event = events.Event("distributor", props)
        self.archiveTransmitter.publishEvent(event)

    def putFile(self, key, name):
        print "putFile: key = %s, name = %s " % (key, name)
        self.condition.acquire()
        self.dataTable[key] = name
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
        sensors = ["R:0,0 S:2,2", "R:0,4 S:0,2", "R:4,0 S:2,0", "R:4,4 S:0,0"]
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
                if not os.path.exists(os.path.dirname(target)):
                    os.makedirs(os.path.dirname(target))
                f = open(target,'wb')
                f.write(src.read(buflen))
                f.close()
                key = Key.create(visitID, exposureSequenceID, raft, sensor)
                self.putFile(key, target)
