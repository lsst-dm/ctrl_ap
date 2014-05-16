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


####
#### this is a prototype and will be refactored
####

import os
import sys
import time
import argparse
import socket
from lsst.pex.logging import Log

class Monitor(object):

    def __init__(self, logger, host, port):
        # TODO:  these need to be placed in a configuration file
        # which is loaded, so they are not embedded in the code
        self.host = host
        self.port = port
        self.sock = None
        self.logger = logger

    def connect(self):
        # attempt a connection.

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.connect((self.host, self.port))
        except socket.gaierror, err:
            self.logger.Log(Log.INFO, "address problem?  %s " % err
            sys.exit(1)
        except socket.error, err:
            self.logger.Log(Log.INFO, "Connection problem: %s" % err
            self.sock = None
            return False
        return True

    def checkStatus(self):
        # check status of the socket with a ping/pong message
        self.send("ping")
        s = self.recv(4)
        self.logger.Log(s)
        return True

        

if __name__ == "__main__":
    logger = Log.getDefaultLog()
    logger = Log(logger, "Monitor")
    basename = os.path.basename(sys.argv[0])

    parser = argparse.ArgumentParser(prog=basename)
        
    parser.add_argument("-H", "--host", type=str, action="store", help="host to connect to", required=True)
    parser.add_argument("-P", "--port", type=int, action="store", help="port to connect to", required=True)

    args = parser.parse_args()

    # add argparse
    monitor = Monitor(logger, args.host, args.port)

    isConnected = monitor.connect()
    if isConnected:
        logger.Log(Log.INFO, "reg.registerFullyOperation()")
    else:
        logger.Log(Log.INFO, "reg.registerLocalOnly()")

    while True:
        time.sleep(30)
        connected = monitor.checkStatus()
        if connected:
            logger.Log(Log.INFO, "reg.registerFullyOperation()")
        else:
            logger.Log(Log.INFO, "reg.registerLocalOnly()")
