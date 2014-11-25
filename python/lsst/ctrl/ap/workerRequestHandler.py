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

from sys import getsizeof, stderr
from itertools import chain
from collections import deque
try:
    from reprlib import repr
except ImportError:
    pass

class WorkerRequestHandler(object):
    def __init__(self, jsock, dataTable, condition):
        print "wrh:  object created"
        self.logger = Log.getDefaultLog()
        self.jsock = jsock
        self.dataTable = dataTable
        self.condition = condition


    def getFileInfo(self, key):
        print "getFileInfo: key = %s" % key
        name = None
        self.condition.acquire()
        while True:
            size = getsizeof(self.dataTable)
            size += sum(map(getsizeof,self.dataTable.itervalues())) + sum(map(getsizeof,self.dataTable.iterkeys()));

            print "datatable len = %d size of datatable = %d" % (len(self.dataTable), size)
            if key in self.dataTable:
                info = self.dataTable[key]
                print "getFileInfo: key = ",key
                #print "getFileInfo: info = ", info
                name = info.getName()
                if name is not None: 
                    print "getFileInfo name = ",name
                    break
                else:
                    print "name was none??"
            else: 
                # worker contacted distributor and the key didn't exist,
                # when it should have.  That means the distributor
                # had previously given this info to the archive DMCS, but
                # the distributor no longer has it (due to reboot, or 
                # expiration.
                print "XXX - key didn't exist"
                self.condition.release()
                return None
            self.condition.wait()
        self.condition.release()
        return name

    def transmitFile(self, key, data):
        print "transmitFile: key = ",key
        name = self.getFileInfo(key)
        st = Status();
        print "transmitFile requested"
        if name is None:
            # respond to the worker with a message that the file isn't here
            # TODO: tell the archive DMCS to remove that entry
            msg = {st.status:st.error, st.reason:st.fileNotFound}
            self.jsock.sendJSON(msg)
            print "transmitFile NOT sent"
            return
        print "transmitFile: name = %s to %s "% (name, self.jsock.getsockname())
        msg = data.copy()
        msg["status"] = st.sendFile
        msg["filename"] = name
        print "transmitFile: msg = ",msg
        # TODO: tell worker we do have it, and then send file
        self.jsock.sendFile(msg)
        st.publish(st.distributorNode, st.sendFile, {"filename":name})
        print "transmitFile: done"

    def serviceRequest(self, msg):
        print "serviceRequest: ",msg
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
