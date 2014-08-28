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
from lsst.ctrl.ap.key import Key
from lsst.ctrl.ap.status import Status

class WorkerRequestHandler(object):
    def __init__(self, jsock, dataTable, condition):
        print "wrh:  object created"
        self.logger = Log.getDefaultLog()
        self.jsock = jsock
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

    def transmitFile(self, key, data):
        print "transmitFile: key = ",key
        name = self.getFileInfo(key)
        print "transmitFile: name = ",name,"to ",self.jsock.getsockname()
        st = Status();
        msg = data.copy()
        msg["filename"] = name
        self.jsock.sendFile(msg)
        st.publish(st.distributorNode, st.sendFile, {"filename":name})
        print "transmitFile: done"

    def serviceRequest(self, msg):
        request = msg["request"]
        if request == "file":
            visitID = msg["visitID"]
            exposureSequenceID = msg["exposureSequenceID"]
            raft = msg["raft"]
            sensor = msg["sensor"]
            st = Status()
            data = { "visitID":visitID, "exposureSequenceID":exposureSequenceID, "raft":raft, "sensor":sensor}
            st.publish(st.distributorNode, st.requestFile, data)
            key = Key.create(visitID, exposureSequenceID, raft, sensor)
            self.transmitFile(key, data)
            return
