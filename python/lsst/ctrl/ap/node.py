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

import argparse
import datetime
import os
import sys
import socket
import time
import lsst.ctrl.events as events
from lsst.daf.base import PropertySet

class Node(object):

    def __init__(self):
        self.inSock = None
        self.outSock = None


    def createSocket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        return sock

    def createIncomingSocket(self, host, port):
        self.inSock = self.createSocket()
        self.inSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        print "creating incoming socket %s:%d" % (host, port)
        self.inSock.bind((host, port))
        self.inSock.listen(5)
        print "done creating socket"

    def connectToNode(self, host, port):
        self.outSock = self.createSocket()
        print "connecting to node %s:%d" % (host, port)
        try:
            self.outSock.connect((host, port))
        except socket.gaierror, err:
            print "address problem?  %s " % err
            sys.exit(1)
        except socket.error, err:
            print "Connection problem: %s" % err
            self.outSock = None
            return False
        return True

    def process(self):
        pass 
