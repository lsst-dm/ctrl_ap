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
from lsst.ctrl.ap.replicatorRequestHandler import ReplicatorRequestHandler
from lsst.ctrl.ap.workerRequestHandler import WorkerRequestHandler

class DistributorHandler(threading.Thread):
    def __init__(self, jsock, dataTable, condition):
        super(DistributorHandler, self).__init__()
        self.jsock = jsock
        self.dataTable = dataTable
        self.condition = condition
        self.socketCondition = threading.Condition()
        logger = Log.getDefaultLog()
        self.logger = Log(logger, "distributorHandler")

        
    def run(self):
        msg = self.jsock.recvJSON()
        msgType = msg["msgtype"]
        print "dh: msg =",msg
        if msgType == "replicator job" or msgType == "wavefront job":
            handler = ReplicatorRequestHandler(self.jsock, self.dataTable, self.condition)
            handler.serviceRequest(msg)
            print "dh: serviced msg =",msg
            while True:
                msg = self.jsock.recvJSON()
                print "dh: servicing msg =",msg
                handler.serviceRequest(msg)
        elif msgType == "worker job":
            handler = WorkerRequestHandler(self.jsock, self.dataTable,  self.condition)
            handler.serviceRequest(msg)
