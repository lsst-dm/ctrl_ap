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
    def __init__(self, sock):
        super(DistributorHandler, self).__init__()
        self.sock = sock
        logger = Log.getDefaultLog()
        self.logger = Log(logger, "distributorHandler")

        # TODO: get these from a config
        self.broker = "lsst8.ncsa.uiuc.edu"
        self.topic = "distributor_event"
        eventSystem = events.EventSystem.getDefaultEventSystem()
        self.archiveTransmitter = events.EventTransmitter(self.broker, self.topic)
        
    def sendToArchiveDMCS(self, vals):
        props = PropertySet()
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

        event = events.Event("distributor", props)
        self.archiveTransmitter.publishEvent(event)

    def run(self):
        while True:
            s = self.sock.recvJSON()
            if s == "":
                self.logger.log(Log.INFO, 'received nothing')
                return 
            self.sendToArchiveDMCS(s) 
            self.logger.log(Log.INFO, '1 received from replicator %s' % s)
            name = self.sock.recvFile()
            self.logger.log(Log.INFO, 'file received: %s' % name)
