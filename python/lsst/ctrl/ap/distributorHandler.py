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

import threading
from lsst.ctrl.ap.replicatorRequestHandler import ReplicatorRequestHandler
from lsst.ctrl.ap.workerRequestHandler import WorkerRequestHandler
from lsst.ctrl.ap.heartbeat import Heartbeat
import lsst.log as log

class DistributorHandler(threading.Thread):
    def __init__(self, jsock, dataTable, condition):
        super(DistributorHandler, self).__init__()
        self.jsock = jsock
        self.dataTable = dataTable
        self.condition = condition

    def run(self):
        msg = self.jsock.recvJSON()
        log.debug("dh: 1: msg = %s", msg)
        msgType = msg["msgtype"]
        if msgType == "replicator job" or msgType == "wavefront job":
            handler = ReplicatorRequestHandler(self.jsock, self.dataTable, self.condition)
            handler.serviceRequest(msg)
            heartbeat = Heartbeat(self.jsock, 1)
            heartbeat.start()
            while True:
                msg = self.jsock.recvJSON()
                log.debug("dh: 2: msg = %s", msg)
                handler.serviceRequest(msg)
        elif msgType == "worker job":
            handler = WorkerRequestHandler(self.jsock, self.dataTable,  self.condition)
            handler.serviceRequest(msg)
