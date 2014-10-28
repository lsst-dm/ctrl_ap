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
import signal
import sys
import time
import socket
import threading
from lsst.pex.logging import Log

# terminate the process after a set period, unless we're told not to.
class Terminator(threading.Thread):
    def __init__(self, logger, timeout):
        super(Terminator, self).__init__()
        self.logger = logger
        self.timeout = timeout
        self.condition = threading.Condition()
        self.terminate = True

    def run(self):
        time.sleep(self.timeout)
        self.condition.acquire()
        if self.terminate == True:
            self.logger.log(Log.INFO, "terminator: finished, and terminating")
            os.kill(os.getpid(), signal.SIGKILL)
        self.condition.release()
        # we just fall through and finish this thread
        self.logger.log(Log.INFO, "terminator: finished, but not terminating")


    def cancel(self):
        self.condition.acquire()
        self.terminate = False
        self.condition.release()

if __name__ == "__main__":
    print "starting first thread"
    term = Terminator(15)
    term.start()
    time.sleep(5)
    term.cancel()
    term.join()
    print "first thread done"

    print "starting second thread"
    term = Terminator(15)
    term.start()
    time.sleep(5)
    term.join()


    print "You shouldn't see this message"
