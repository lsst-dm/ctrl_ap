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
import json
import socket
import lsst.ctrl.events as events
from lsst.daf.base import PropertySet

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Status(object):
    __metaclass__ = Singleton

    # broker location
    broker = "lsst8.ncsa.illinois.edu"
    # ap status topic
    topic = "ap_status"

    # JSON message element types
    component = "component"
    status = "status"
    message = "message"
    id = "component id"
    port ="client port"
    data = "data"

    # stanard ap components
    archiveDMCS = "archive dmcs"
    baseDMCS = "base dmcs"
    distributorNode = "distributor"
    replicatorNode = "replicator node"
    replicatorJob = "replicator job"
    workerJob = "worker job"

    # standard status types
    start = "start"
    connect = "connecting to"
    requestFile = "request file"
    sendFile = "send file"
    fileReceived = "file received"
    receivedMsg = "received message"
    submit = "submit"
    perform = "perform"
    generate = "generate"
    update = "update"
    issue = "issue"
    finish = "finish"
    create = "create"
    lookup = "lookup"
    retrieve = "retrieve"
    accept = "accept from"
    connectionWait = "waiting on connection"
    idle = "idle"

    success = "success"



    def __init__(self):
        #self.broker = broker
        #self.topic = topic
        self.eventSystem = events.EventSystem.getDefaultEventSystem()
        self.eventSystem.createTransmitter(self.broker, self.topic)
        self.process = "%s/%d" % (socket.gethostname(), os.getpid())

    def publish(self, component, status, msg, port=None):
        m = {self.component:component, self.status:status, self.message:msg, self.id:self.process}
        if port is not None:
            m[self.port] = port
        s = json.dumps(m)

        root = PropertySet()
        root.add(self.data,s)

        event = events.Event("status_runid",root)
        self.eventSystem.publishEvent(self.topic,event)



if __name__ == "__main__":
        s = Status()
        s.publish("status.py", "ok", "Hello, World!")
