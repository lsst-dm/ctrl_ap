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
import json
import socket
import lsst.ctrl.events as events
from lsst.daf.base import PropertySet
import lsst.log as log

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
    id = "componentID"
    port ="clientPort"
    data = "data"

    # standard ap components
    archiveDMCS = "archive dmcs"
    baseDMCS = "base dmcs"
    distributorNode = "distributor"
    replicatorNode = "replicator node"
    wavefrontJob = "wavefront job"
    wavefrontSensorJob = "wavefront sensor job"
    replicatorJob = "replicator job"
    replicatorJobs = "replicator jobs"
    workerJob = "worker job"
    workerJobs = "worker jobs"
    ocs = "ocs"

    # part deux:
    server = "server"
    client = "client"
    host = "host"
    port = "port"

    # standard status types
    start = "start"
    connect = "connect"
    requestFile = "request file"
    sendFile = "send file"
    fileReceived = "file received"
    receivedMsg = "received message"
    sendMsg = "send message"
    submit = "submit"
    perform = "perform"
    completed = "completed"
    generate = "generate"
    update = "update"
    pub = "publish"
    upload = "upload"
    read = "read"
    finish = "finish"
    create = "create"
    lookup = "lookup"
    retrieve = "retrieve"
    accept = "accept from"
    connectionWait = "waiting on connection"
    idle = "idle"
    listen = "listen"
    inform = "inform"
    infoReceived ="information received"
    retrieved = "retrieved"
    issue = "issue"
    failover = "failover"
    startReadout = "startReadout"

    success = "success"
    error = "error"
    reason = "reason"
    removeEntry = "remove entry"

    # error types
    fileNotFound = "file not found"


    def __init__(self):
        self.eventSystem = events.EventSystem.getDefaultEventSystem()
        self.eventSystem.createTransmitter(self.broker, self.topic)
        self.process = "%s/%d"% (socket.gethostname(), os.getpid())

    def publishMessage(self, component, status, msg=None):
        m = {self.component:component, self.status:status, self.id:self.process}
        if msg is not None:
            m[self.message] = msg
        s = json.dumps(m)

        root = PropertySet()
        root.add(self.data,s)

        event = events.Event("status_runid",root)
        self.eventSystem.publishEvent(self.topic,event)

    def publishDict(self, component, status, d):
        m = {self.component:component, self.status:status, self.id:self.process}
        for key, value in d.iteritems():
            m[key] = value
        s = json.dumps(m)

        root = PropertySet()
        root.add(self.data,s)

        event = events.Event("status_runid",root)
        self.eventSystem.publishEvent(self.topic,event)

    def publish(self, component, status, data=None):
        if data is None:
            self.publishMessage(component, status)
        elif type(data) == str:
            self.publishMessage(component, status, data)
        elif type(data) == dict:
            self.publishDict(component, status, data)
        else:
            log.warn("publish: unknown type")


if __name__ == "__main__":
        s = Status()
        s.publish("status.py", "start", "Hello, World!")
        client = {"host":"hostname","port":22}
        s.publish("status.py", "connection", client)
        topic = {"topic":"whatthe"}
        s.publish("status.py", "listening", topic)

        server = {"host":"hostname2","port":23332}
        connection = {"connection":{"client":client, "server":server}}
        s.publish("status.py","connect", connection)
