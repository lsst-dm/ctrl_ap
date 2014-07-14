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
import lsst.ctrl.events as events
from lsst.pex.logging import Log
from lsst.daf.base import PropertySet

class DistributorHandler(threading.Thread):
    def __init__(self, sock, dataTable, condition):
        super(DistributorHandler, self).__init__()
        self.sock = sock
        self.dataTable = dataTable
        self.condition = condition
        logger = Log.getDefaultLog()
        self.logger = Log(logger, "distributorHandler")

        
    def sendToArchiveDMCS(self, vals):
        props = PropertySet()
        print "sendToArchiveDMCS: props = ",props
        for x in vals:
            print x, vals[x]
            val = vals[x]
            if type(val) == int:
                props.set(str(x), int(vals[x]))
            else:
                props.set(str(x), str(vals[x]))
        props.set("distributor_event", "archive info")
        hostinfo = self.sock.getsockname()
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


    def getFile(self, key):
        print "getFile: key = %s" % key
        name = ""
        self.condition.acquire()
        while True:
            if key in self.dataTable:
                name = self.dataTable[key]
                print "getFile name = ",name
                break
            self.condition.wait()
        self.condition.release()
        return name

    # todo: this needs to block when the key doesn't exist.
    def transmitFile(self, vals):
        print "transmitFile: vals = ",vals
        key = self.createKey(vals)
        print "transmitFile: key = ",key
        name = self.getFile(key)
        print "transmitFile: name = ",name,"to ",self.sock.getsockname()
        self.sock.sendFile(name)
        print "transmitFile: done"

    def createKey(self, vals):
        exposureSequenceID = vals["exposureSequenceID"]
        visitID = vals["visitID"]
        raft = vals["raft"]
        key = "%s:%s:%s" % (exposureSequenceID, visitID, raft)
        return key

    def run(self):
        # TODO:  this should probably renew a lease to the archive, so the
        # archive knows this is still alive, rather than the archive always
        # assuming it.

        # receive the visit id, exposure sequence number and raft id, from
        # the replicator.
        vals = self.sock.recvJSON()
        if vals ==  None:
            self.logger.log(Log.INFO, 'received nothing')
            return 
        msgtype = vals["msgtype"]
        print "message type = ",msgtype
        if msgtype == "replicator job":
            # send the message we just received, along with some additional
            # information, to the Archive DMCS.
            self.sendToArchiveDMCS(vals) 
        
            key = self.createKey(vals)
            self.logger.log(Log.INFO, 'received from replicator %s' % vals)
            name = self.sock.recvFile()
            self.logger.log(Log.INFO, 'file received: %s' % name)
            self.putFile(key, name)
            self.exit()
        elif msgtype == "worker job":
            request = vals["request"]
            if request == "file":
                self.transmitFile(vals)
                self.exit()

