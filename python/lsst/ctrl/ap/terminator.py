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

import datetime
import os
import signal
import sys
import time
import socket
import thread
import threading
from lsst.pex.logging import Log

# terminate the process after a set period, unless we're told not to.
# this thread will run to completion, and in the end determine whether
# or not the process should exit.
class Terminator(object):
    def __init__(self, logger, id, timeout):
        self.logger = logger
        self.id = id
        self.timeout = timeout

        self.timer = None
        self.logger.log(Log.INFO, "terminator: %s: started" % self.id)

    def start(self):
        self.timer = threading.Timer(self.timeout, self.die)
        self.timer.daemon = True
        self.timer.start()

    def cancel(self):
        self.timer.cancel()
        self.logger.log(Log.INFO, "terminator: %s: cancelled" % self.id)

    def die(self):
        self.logger.log(Log.INFO, "terminator: %s: finished, and terminating" % self.id)
        ts = time.time()
        self.logger.log(Log.INFO, datetime.datetime.fromtimestamp(ts).strftime('termination at: %Y-%m-%d %H:%M:%S'))
        os.kill(os.getpid(), signal.SIGKILL)
