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

    def __init__(self):
        self.broker = "lsst8.ncsa.illinois.edu"
        self.topic = "ap_status"
        self.eventSystem = events.EventSystem.getDefaultEventSystem()
        self.eventSystem.createTransmitter(self.broker, self.topic)

    def publish(self, component, status, msg):
        m = {"component":component, "status":status, "msg":msg}
        s = json.dumps(m)

        root = PropertySet()
        root.add("data",s)

        event = events.Event("runid",root)
        self.eventSystem.publishEvent(self.topic,event)


if __name__ == "__main__":
        s = Status()
        s.publish("status.py", "ok", "Hello, World!")
