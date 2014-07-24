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
from lsst.ctrl.ap.key import Key

class WorkerRequestHandler(object):
    def __init__(self, logger, jsock, msg, dataTable, condition):
        self.logger = logger
        self.jsock = jsock
        self.msg = msg
        self.dataTable = dataTable
        self.condition = condition

    def getFileInfo(self, key):
        print "getFileInfo: key = %s" % key
        name = ""
        self.condition.acquire()
        while True:
            if key in self.dataTable:
                info = self.dataTable[key]
                print "getFileInfo: key = ",key 
                print "getFileInfo: info = ", info
                name = info.getName()
                if name is not None: 
                    print "getFileInfo name = ",name
                    break
            self.condition.wait()
        self.condition.release()
        return name

    def transmitFile(self, key):
        print "transmitFile: key = ",key
        name = self.getFileInfo(key)
        print "transmitFile: name = ",name,"to ",self.jsock.getsockname()
        self.jsock.sendFile(name)
        print "transmitFile: done"

    def handleRequest(self):
        request = self.msg["request"]
        if request == "file":
            visitID = self.msg["visitID"]
            exposureSequenceID = self.msg["exposureSequenceID"]
            raft = self.msg["raft"]
            sensor = self.msg["sensor"]
            key = Key.create(visitID, exposureSequenceID, raft, sensor)
            self.transmitFile(key)
            return
