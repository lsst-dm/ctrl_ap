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
import time
import lsst.log as log
class Heartbeat(threading.Thread):
    def __init__(self, jsock, delay):
        super(Heartbeat, self).__init__()
        self.jsock = jsock
        self.delay = delay
        log.debug("start Heartbeat")

    def run(self):

        excepted = False

        while not excepted:
            msg = {"msgtype":"heartbeat"}
            try :
                self.jsock.sendJSON(msg)
                time.sleep(self.delay)
            except Exception as exp:
                log.warn("Heartbeat exception")
                excepted = True

class HeartbeatHandler(threading.Thread):
    def __init__(self, jsock):
        super(HeartbeatHandler, self).__init__()
        self.jsock = jsock
        log.debug("start HeartbeatHandler")

    def run(self):
        excepted = False
        while not excepted:
            try:
            # TODO: this has to be done via select and a timeout
                msg = self.jsock.recvJSON()
                log.debug(msg)
            except:
                log.warn("HeartbeatHandler exception")
                excepted = True
